const express = require('express');
const router = express.Router();
const { answerQuestion, answerQuestionForFile } = require('../utils/ragService');
const qdrantService = require('../utils/qdrantService');
const { authenticateToken } = require('../middlewares/auth');
const File = require('../models/File');
const queueService = require('../utils/queueService');

/**
 * @route   POST /api/rag/ask
 * @desc    Ask a question using RAG
 * @access  Protected
 */
async function handleAskSync(req, res) {
    const startTime = Date.now();
    const requestId = `req_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

    try {
        const { question, topK, minScore } = req.body;

        if (!question || typeof question !== 'string' || question.trim().length === 0) {
            console.warn(`[${requestId}] ✗ Invalid question after ${Date.now() - startTime}ms`);
            return res.status(400).json({
                success: false,
                message: 'Question is required'
            });
        }

        console.log(`[${requestId}] RAG Query from user ${req.user.userId}: "${question.substring(0, 50)}..."`);

        // Fetch user's files for context
        const dbStart = Date.now();
        const userFiles = await File.find({ userId: req.user.userId }).select('fileName');
        const dbTime = Date.now() - dbStart;
        const fileNames = userFiles.map(f => f.fileName);
        console.log(`[${requestId}] DB query took ${dbTime}ms, found ${userFiles.length} files`);

        const ragStart = Date.now();
        const result = await answerQuestion(question, req.user.userId, {
            topK: topK || 5,
            minScore: minScore || 0.3,
            fileContext: fileNames
        });
        const ragTime = Date.now() - ragStart;
        const totalTime = Date.now() - startTime;

        console.log(`[${requestId}] ✓ Query succeeded in ${totalTime}ms (DB: ${dbTime}ms, RAG: ${ragTime}ms)`);

        return res.status(200).json({
            success: true,
            data: result,
            metadata: {
                requestId,
                timing: {
                    total: totalTime,
                    database: dbTime,
                    rag: ragTime
                }
            }
        });

    } catch (error) {
        const totalTime = Date.now() - startTime;
        console.error(`[${requestId}] ✗ Query failed after ${totalTime}ms:`, {
            error: error.message,
            stack: error.stack,
            question: req.body.question?.substring(0, 50),
            userId: req.user?.userId,
            errorType: error.constructor.name
        });

        return res.status(500).json({
            success: false,
            message: 'Failed to process question',
            error: error.message,
            requestId,
            timing: { total: totalTime }
        });
    }
}

/**
 * @route   POST /api/rag/ask
 * @desc    Ask a question using RAG (async via queue)
 * @access  Protected
 */
router.post('/ask', authenticateToken, async (req, res) => {
    try {
        const { question, topK, minScore } = req.body;

        if (!question || typeof question !== 'string' || question.trim().length === 0) {
            return res.status(400).json({
                success: false,
                message: 'Question is required'
            });
        }

        const jobId = await queueService.enqueueRagJob({
            userId: req.user.userId,
            question,
            topK: topK || 5,
            minScore: minScore || 0.3,
            priority: 5
        });

        if (!jobId) {
            console.warn('Redis unavailable - falling back to synchronous RAG');
            return handleAskSync(req, res);
        }

        return res.status(202).json({
            success: true,
            message: 'RAG query accepted for processing',
            jobId,
            statusUrl: `/api/rag/status/${jobId}`
        });
    } catch (error) {
        return res.status(500).json({
            success: false,
            message: 'Failed to enqueue RAG query',
            error: error.message
        });
    }
});

/**
 * @route   POST /api/rag/ask-sync
 * @desc    Ask a question using RAG (sync)
 * @access  Protected
 */
router.post('/ask-sync', authenticateToken, handleAskSync);

/**
 * @route   POST /api/rag/ask-file/:fileId
 * @desc    Ask a question about a specific file
 * @access  Protected
 */
router.post('/ask-file/:fileId', authenticateToken, async (req, res) => {
    try {
        const { question, topK, minScore } = req.body;
        const fileId = req.params.fileId;

        if (!question || typeof question !== 'string' || question.trim().length === 0) {
            return res.status(400).json({
                success: false,
                message: 'Question is required'
            });
        }

        // Verify file exists and user owns it
        const file = await File.findOne({
            _id: fileId,
            userId: req.user.userId
        });

        if (!file) {
            return res.status(404).json({
                success: false,
                message: 'File not found or access denied'
            });
        }

        console.log(`RAG Query for file ${fileId}: "${question}"`);

        const jobId = await queueService.enqueueRagJob({
            userId: req.user.userId,
            question,
            topK: topK || 5,
            minScore: minScore || 0.3,
            fileId,
            priority: 5
        });

        if (!jobId) {
            const result = await answerQuestionForFile(question, fileId, {
                topK: topK || 5,
                minScore: minScore || 0.3,
            });

            return res.status(200).json({
                success: true,
                data: result
            });
        }

        return res.status(202).json({
            success: true,
            message: 'RAG file query accepted for processing',
            jobId,
            statusUrl: `/api/rag/status/${jobId}`
        });

    } catch (error) {
        console.error('RAG file query error:', error);
        res.status(500).json({
            success: false,
            message: 'Failed to process question',
            error: error.message
        });
    }
});

/**
 * @route   GET /api/rag/status/:jobId
 * @desc    Get RAG job status/result
 * @access  Protected
 */
router.get('/status/:jobId', authenticateToken, async (req, res) => {
    try {
        const jobId = req.params.jobId;
        const status = await queueService.getJobStatus(jobId);

        if (!status) {
            return res.status(404).json({
                success: false,
                message: 'Job not found'
            });
        }

        return res.status(200).json({
            success: true,
            data: status
        });
    } catch (error) {
        return res.status(500).json({
            success: false,
            message: 'Failed to fetch job status',
            error: error.message
        });
    }
});

/**
 * @route   GET /api/rag/stats
 * @desc    Get RAG system statistics
 * @access  Protected
 */
router.get('/stats', authenticateToken, async (req, res) => {
    try {
        const collectionInfo = await qdrantService.getCollectionInfo();

        // Get user's file count
        const userFileCount = await File.countDocuments({ userId: req.user.userId });

        res.status(200).json({
            success: true,
            data: {
                totalVectors: collectionInfo.pointsCount,
                userFiles: userFileCount,
                collectionName: collectionInfo.name,
                vectorSize: collectionInfo.config.vectorSize
            }
        });

    } catch (error) {
        console.error('Stats error:', error);
        res.status(500).json({
            success: false,
            message: 'Failed to fetch stats',
            error: error.message
        });
    }
});

module.exports = router;
