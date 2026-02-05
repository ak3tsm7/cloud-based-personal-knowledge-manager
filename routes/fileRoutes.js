const express = require('express');
const router = express.Router();
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const mongoose = require('mongoose');
const File = require('../models/File');
const { extractText } = require('../utils/textExtractor');
const { chunkFileText, getChunkingStats } = require('../utils/chunking');
const { generateChunkEmbeddings, generateAndStoreChunkEmbeddings, generateEmbedding } = require('../utils/embeddingService');
const qdrantService = require('../utils/qdrantService');
const queueService = require('../utils/queueService');
const { authenticateToken } = require('../middlewares/auth');

// Configure multer storage
const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        const uploadDir = 'uploads/';
        if (!fs.existsSync(uploadDir)) {
            fs.mkdirSync(uploadDir, { recursive: true });
        }
        cb(null, uploadDir);
    },
    filename: (req, file, cb) => {
        cb(null, Date.now() + '-' + file.originalname);
    }
});

const upload = multer({
    storage,
    fileFilter: (req, file, cb) => {
        const allowedTypes = /pdf|docx|xlsx|pptx|csv|txt|md|json|yaml|yml|jpeg|jpg|png|gif|mp4|avi|mov/;
        const extname = allowedTypes.test(file.originalname.toLowerCase());

        if (extname) {
            return cb(null, true);
        } else {
            cb(new Error('Only PDFs, documents, images, and videos are allowed!'));
        }
    },
    limits: { fileSize: 50 * 1024 * 1024 }
});

async function processUploadSync(req, res, existingFile = null) {
    const extractedText = await extractText(req.file.path);
    let chunks = [];
    let chunkStats = null;
    let embeddingStats = null;
    let qdrantIds = [];

    if (extractedText && extractedText.trim().length > 0) {
        chunks = chunkFileText(extractedText, {
            filename: req.file.originalname,
            mimeType: req.file.mimetype
        });
        chunkStats = getChunkingStats(chunks);
    }

    let fileDoc = existingFile;
    if (!fileDoc) {
        fileDoc = new File({
            userId: req.user.userId,
            fileName: req.file.originalname,
            fileType: req.file.mimetype,
            fileUrl: req.file.path,
            extractedText: extractedText || '',
            chunks: chunks,
            summary: '',
            embedding: [],
            qdrantIds: [],
            status: 'COMPLETED',
        });
    } else {
        fileDoc.fileName = req.file.originalname;
        fileDoc.fileType = req.file.mimetype;
        fileDoc.fileUrl = req.file.path;
        fileDoc.extractedText = extractedText || '';
        fileDoc.chunks = chunks;
        fileDoc.summary = '';
        fileDoc.embedding = [];
        fileDoc.qdrantIds = [];
        fileDoc.status = 'COMPLETED';
    }

    await fileDoc.save();

    if (chunks.length > 0) {
        try {
            const result = await generateAndStoreChunkEmbeddings(
                chunks,
                fileDoc._id.toString(),
                req.file.originalname,
                req.user.userId
            );
            chunks = result.chunks;
            qdrantIds = result.qdrantIds;
            embeddingStats = {
                total: result.stats.total,
                successful: result.stats.successful,
                failed: result.stats.failed,
                dimensions: qdrantService.VECTOR_SIZE,
                storedInQdrant: true
            };
            fileDoc.qdrantIds = qdrantIds;
            fileDoc.chunks = chunks;
            await fileDoc.save();
        } catch (embeddingError) {
            console.error('Error generating embeddings:', embeddingError.message);
            embeddingStats = { error: embeddingError.message };
        }
    }

    return res.status(201).json({
        message: 'File uploaded successfully',
        mode: 'sync',
        file: fileDoc,
        textExtracted: !!extractedText,
        chunkCount: chunks.length,
        chunkStats,
        embeddingStats
    });
}

// GET /api/upload - Show upload form (Protected)
router.get('/upload', authenticateToken, (req, res) => {
    res.render('upload', { user: req.user });
});

// POST /api/upload - Upload a file (Protected) - ASYNC with SYNC FALLBACK
router.post('/upload', authenticateToken, upload.single('file'), async (req, res) => {
    try {
        if (!req.file) {
            return res.status(400).json({ error: 'No file uploaded' });
        }

        console.log(`📤 Received file: ${req.file.originalname}`);

        // Check if Redis is available for async processing
        const redisAvailable = await queueService.isRedisAvailable();

        if (redisAvailable) {
            // ASYNC MODE: Queue for background processing
            const newFile = new File({
                userId: req.user.userId,
                fileName: req.file.originalname,
                fileType: req.file.mimetype,
                fileUrl: req.file.path,
                extractedText: '',
                chunks: [],
                summary: '',
                embedding: [],
                qdrantIds: [],
                status: 'QUEUED',
                totalChunks: 0,
                chunksProcessed: 0,
            });

            await newFile.save();
            console.log(`✓ File saved with ID: ${newFile._id}, status: QUEUED`);

            const jobId = await queueService.enqueueJob({
                fileId: newFile._id.toString(),
                filepath: req.file.path,
                userId: req.user.userId,
                mimetype: req.file.mimetype,
                filename: req.file.originalname,
                priority: 5,
            });

            if (!jobId) {
                console.warn('Redis enqueue failed - processing synchronously');
                return await processUploadSync(req, res, newFile);
            }

            newFile.jobId = jobId;
            await newFile.save();

            console.log(`✓ Job ${jobId} queued for file ${newFile._id}`);

            return res.status(202).json({
                message: 'File upload accepted for processing',
                mode: 'async',
                fileId: newFile._id,
                jobId: jobId,
                status: 'QUEUED',
                statusUrl: `/api/files/${newFile._id}/status`,
                file: {
                    id: newFile._id,
                    fileName: newFile.fileName,
                    fileType: newFile.fileType,
                    createdAt: newFile.createdAt,
                },
            });
        }

        // SYNC MODE: Redis not available, process immediately
        console.log('⚠️  Redis unavailable - processing synchronously');
        return await processUploadSync(req, res);

    } catch (error) {
        console.error('Upload error:', error);
        res.status(500).json({ error: 'Failed to upload file', details: error.message });
    }
});

// POST /api/upload/sync - Synchronous upload (for backwards compatibility)
router.post('/upload/sync', authenticateToken, upload.single('file'), async (req, res) => {
    try {
        if (!req.file) {
            return res.status(400).json({ error: 'No file uploaded' });
        }

        console.log('Extracting text from:', req.file.path);
        const extractedText = await extractText(req.file.path);

        if (extractedText) {
            console.log(`Successfully extracted ${extractedText.length} characters from ${req.file.originalname}`);
        } else {
            console.log('No text extracted or unsupported file type');
        }

        let chunks = [];
        let chunkStats = null;
        let embeddingStats = null;
        let qdrantIds = [];

        if (extractedText && extractedText.trim().length > 0) {
            console.log('Chunking extracted text...');
            chunks = chunkFileText(extractedText, {
                filename: req.file.originalname,
                mimeType: req.file.mimetype
            });
            chunkStats = getChunkingStats(chunks);
            console.log(`Created ${chunks.length} chunks (avg size: ${chunkStats.avgChunkSize} chars)`);
        }

        const newFile = new File({
            userId: req.user.userId,
            fileName: req.file.originalname,
            fileType: req.file.mimetype,
            fileUrl: req.file.path,
            extractedText: extractedText || '',
            chunks: chunks,
            summary: '',
            embedding: [],
            qdrantIds: [],
            status: 'COMPLETED',
        });

        await newFile.save();
        console.log(`File saved to MongoDB with ID: ${newFile._id} for user: ${req.user.userId}`);

        if (chunks.length > 0) {
            try {
                console.log('Generating embeddings for chunks and storing in Qdrant...');

                const result = await generateAndStoreChunkEmbeddings(
                    chunks,
                    newFile._id.toString(),
                    req.file.originalname,
                    req.user.userId
                );

                chunks = result.chunks;
                qdrantIds = result.qdrantIds;

                embeddingStats = {
                    total: result.stats.total,
                    successful: result.stats.successful,
                    failed: result.stats.failed,
                    dimensions: qdrantService.VECTOR_SIZE,
                    storedInQdrant: true
                };

                console.log(`Generated and stored ${result.stats.successful}/${chunks.length} embeddings in Qdrant`);

                newFile.qdrantIds = qdrantIds;
                newFile.chunks = chunks;
                await newFile.save();

            } catch (embeddingError) {
                console.error('Error generating embeddings:', embeddingError.message);
                embeddingStats = {
                    total: chunks.length,
                    successful: 0,
                    failed: chunks.length,
                    error: embeddingError.message,
                    storedInQdrant: false
                };
            }
        }

        res.status(201).json({
            message: 'File uploaded successfully',
            file: newFile,
            textExtracted: !!extractedText,
            textLength: extractedText ? extractedText.length : 0,
            chunksCreated: chunks.length > 0,
            chunkCount: chunks.length,
            chunkStats: chunkStats,
            embeddingStats: embeddingStats
        });
    } catch (error) {
        console.error('Upload error:', error);
        res.status(500).json({ error: 'Failed to upload file' });
    }
});

// GET /api/files/:fileId/status - Check file processing status (Protected)
router.get('/files/:fileId/status', authenticateToken, async (req, res) => {
    try {
        const file = await File.findOne({
            _id: req.params.fileId,
            userId: req.user.userId,
        });

        if (!file) {
            return res.status(404).json({
                success: false,
                error: 'File not found',
            });
        }

        // Get job status from Redis if still processing
        let jobStatus = null;
        if (file.jobId && (file.status === 'QUEUED' || file.status === 'PROCESSING')) {
            jobStatus = await queueService.getJobStatus(file.jobId);
        }

        // Calculate progress
        let progress = 0;
        if (file.status === 'COMPLETED') {
            progress = 100;
        } else if (file.status === 'FAILED') {
            progress = 0;
        } else if (jobStatus) {
            progress = jobStatus.progress || 0;
        }

        res.json({
            fileId: file._id,
            fileName: file.fileName,
            status: file.status,
            jobId: file.jobId,
            progress: progress,
            totalChunks: file.totalChunks,
            chunksProcessed: file.chunksProcessed,
            processingTime: file.processingTime,
            startedAt: file.startedAt,
            completedAt: file.completedAt,
            error: file.error,
            createdAt: file.createdAt,
            // Include job queue status if available
            queueStatus: jobStatus ? {
                lastHeartbeat: jobStatus.lastHeartbeat,
                queueProgress: jobStatus.progress,
            } : null,
        });

    } catch (error) {
        console.error('Status check error:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to get file status',
            details: error.message,
        });
    }
});

// GET /api/files - Get all files (Protected)
router.get('/files', authenticateToken, async (req, res) => {
    try {
        const files = await File.find({ userId: req.user.userId })
            .sort({ createdAt: -1 });

        // If request wants JSON, return JSON
        if (req.headers.accept && req.headers.accept.includes('application/json')) {
            return res.json({
                success: true,
                count: files.length,
                data: files
            });
        }

        // Otherwise, return HTML view
        res.render('files', { files, user: req.user });
    } catch (error) {
        console.error('Fetch error:', error);
        res.status(500).json({ error: 'Failed to fetch files' });
    }
});

// GET /api/files/view/:id - View file content (Protected)
router.get('/files/view/:id', authenticateToken, async (req, res) => {
    try {
        const file = await File.findOne({
            _id: req.params.id,
            userId: req.user.userId
        });

        if (!file) {
            return res.status(404).send('File not found or access denied');
        }

        const extractedText = file.extractedText || 'No text extracted for this file.';
        res.render('file-view', { file, extractedText, user: req.user });
    } catch (error) {
        console.error('Fetch error:', error);
        res.status(500).send('Failed to fetch file');
    }
});

// GET /api/files/:id - Get a specific file JSON (Protected)
router.get('/files/:id', authenticateToken, async (req, res) => {
    try {
        const file = await File.findOne({
            _id: req.params.id,
            userId: req.user.userId
        });

        if (!file) {
            return res.status(404).json({
                success: false,
                message: 'File not found'
            });
        }

        res.json({
            success: true,
            data: file
        });
    } catch (error) {
        console.error('Fetch error:', error);
        res.status(500).json({
            success: false,
            message: 'Failed to fetch file',
            error: error.message
        });
    }
});

// POST /api/files/:id/generate-embeddings (Protected)
router.post('/files/:id/generate-embeddings', authenticateToken, async (req, res) => {
    try {
        const file = await File.findOne({
            _id: req.params.id,
            userId: req.user.userId
        });

        if (!file) {
            return res.status(404).json({ error: 'File not found' });
        }

        if (!file.chunks || file.chunks.length === 0) {
            return res.status(400).json({
                error: 'File has no chunks. Please re-upload the file to generate chunks first.'
            });
        }

        console.log(`Generating embeddings for ${file.chunks.length} chunks in file: ${file.fileName}`);

        try {
            const result = await generateAndStoreChunkEmbeddings(
                file.chunks,
                file._id.toString(),
                file.fileName,
                req.user.userId
            );

            file.chunks = result.chunks;
            file.qdrantIds = result.qdrantIds;
            await file.save();

            const embeddingStats = {
                total: result.stats.total,
                successful: result.stats.successful,
                failed: result.stats.failed,
                dimensions: qdrantService.VECTOR_SIZE,
                storedInQdrant: true
            };

            console.log(`Successfully generated and stored ${result.stats.successful}/${result.stats.total} embeddings in Qdrant`);

            res.json({
                message: 'Embeddings generated and stored in Qdrant successfully',
                fileId: file._id,
                fileName: file.fileName,
                embeddingStats: embeddingStats
            });
        } catch (embeddingError) {
            console.error('Error generating embeddings:', embeddingError.message);
            res.status(500).json({
                error: 'Failed to generate embeddings',
                details: embeddingError.message
            });
        }
    } catch (error) {
        console.error('Error:', error);
        res.status(500).json({ error: 'Failed to process request' });
    }
});

// DELETE /api/files/:id (Protected)
router.delete('/files/:id', authenticateToken, async (req, res) => {
    try {
        const file = await File.findOne({
            _id: req.params.id,
            userId: req.user.userId
        });

        if (!file) {
            return res.status(404).json({
                success: false,
                message: 'File not found or access denied'
            });
        }

        console.log(`Deleting file: ${file.fileName} (ID: ${req.params.id})`);

        // Delete from Qdrant
        try {
            await qdrantService.deleteFileEmbeddings(req.params.id);
            console.log(`✓ Deleted Qdrant embeddings for file: ${req.params.id}`);
        } catch (qdrantError) {
            console.error('Error deleting from Qdrant:', qdrantError.message);
        }

        // Delete physical file
        if (file.fileUrl && fs.existsSync(file.fileUrl)) {
            try {
                fs.unlinkSync(file.fileUrl);
                console.log(`✓ Deleted physical file: ${file.fileUrl}`);
            } catch (fsError) {
                console.error('File system deletion error:', fsError);
            }
        }

        // Delete from MongoDB
        await File.findByIdAndDelete(req.params.id);
        console.log(`✓ Deleted database record for file: ${req.params.id}`);

        res.json({
            success: true,
            message: 'File and embeddings deleted successfully'
        });
    } catch (error) {
        console.error('Delete error:', error);
        res.status(500).json({ error: 'Failed to delete file' });
    }
});

// POST /api/search - Semantic search (Protected)
router.post('/search', authenticateToken, async (req, res) => {
    try {
        const { query, limit = 10 } = req.body;

        if (!query || typeof query !== 'string' || query.trim().length === 0) {
            return res.status(400).json({ error: 'Query is required' });
        }

        console.log(`Searching for: "${query}" (User: ${req.user.userId})`);

        const queryEmbedding = await generateEmbedding(query);

        if (!queryEmbedding) {
            return res.status(500).json({ error: 'Failed to generate query embedding' });
        }

        // Search with user filter
        const searchResults = await qdrantService.searchSimilarChunks(
            queryEmbedding,
            parseInt(limit),
            {
                must: [
                    { key: "userId", match: { value: req.user.userId } }
                ]
            }
        );

        const fileIds = [...new Set(searchResults.map(r => r.fileId))];
        const files = await File.find({
            _id: { $in: fileIds },
            userId: req.user.userId
        }).select('fileName fileType createdAt');

        const fileMap = {};
        files.forEach(f => {
            fileMap[f._id.toString()] = f;
        });

        const enrichedResults = searchResults.map(result => ({
            ...result,
            file: fileMap[result.fileId] ? {
                id: fileMap[result.fileId]._id,
                name: fileMap[result.fileId].fileName,
                type: fileMap[result.fileId].fileType,
                createdAt: fileMap[result.fileId].createdAt
            } : null
        }));

        res.json({
            query: query,
            resultsCount: enrichedResults.length,
            results: enrichedResults
        });
    } catch (error) {
        console.error('Search error:', error);
        res.status(500).json({ error: 'Failed to perform search', details: error.message });
    }
});

module.exports = router;


