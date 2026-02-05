/**
 * RAG Query Worker
 *
 * Background worker that processes RAG query jobs from Redis queue.
 *
 * Usage: node workers/ragWorker.js
 */

require('dotenv').config();
const mongoose = require('mongoose');
const queueService = require('../utils/queueService');
const { answerQuestion, answerQuestionForFile } = require('../utils/ragService');

// Worker configuration
const WORKER_ID = process.env.WORKER_ID || `rag-worker-${process.pid}`;
const WORKER_TYPE = process.env.WORKER_TYPE || 'rag';
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS) || 1000;
const HEARTBEAT_INTERVAL_MS = parseInt(process.env.HEARTBEAT_INTERVAL_MS) || 5000;

let isShuttingDown = false;
let currentJob = null;
let heartbeatInterval = null;

async function processJob(job) {
    const { question, topK, minScore, userId, fileId } = job.payload;

    await queueService.updateJobProgress(job.job_id, 10, 0);

    let result;
    if (fileId) {
        result = await answerQuestionForFile(question, fileId, {
            topK: topK || 5,
            minScore: minScore || 0.3
        });
    } else {
        result = await answerQuestion(question, userId, {
            topK: topK || 5,
            minScore: minScore || 0.3
        });
    }

    await queueService.updateJobProgress(job.job_id, 90, 0);
    return result;
}

function startHeartbeat(jobId) {
    stopHeartbeat();
    heartbeatInterval = setInterval(async () => {
        if (currentJob) {
            try {
                await queueService.sendHeartbeat(jobId, WORKER_ID);
            } catch (err) {
                console.error('Heartbeat failed:', err.message);
            }
        }
    }, HEARTBEAT_INTERVAL_MS);
}

function stopHeartbeat() {
    if (heartbeatInterval) {
        clearInterval(heartbeatInterval);
        heartbeatInterval = null;
    }
}

async function workerLoop() {
    console.log(`\nðŸ”„ ${WORKER_ID} polling for RAG jobs...`);

    while (!isShuttingDown) {
        try {
            const job = await queueService.claimJob(WORKER_TYPE, WORKER_ID);

            if (job) {
                currentJob = job;
                startHeartbeat(job.job_id);

                try {
                    const result = await processJob(job);
                    await queueService.completeJob(job.job_id, WORKER_ID, result);
                } catch (error) {
                    await queueService.failJob(job.job_id, WORKER_ID, error.message);
                }

                stopHeartbeat();
                currentJob = null;
            } else {
                await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL_MS));
            }
        } catch (error) {
            console.error('Worker loop error:', error.message);
            await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL_MS * 2));
        }
    }
}

async function shutdown(signal) {
    console.log(`\nâš ï¸  Received ${signal}, shutting down gracefully...`);
    isShuttingDown = true;
    stopHeartbeat();

    if (currentJob) {
        console.log('Waiting for current job to complete...');
        const timeout = setTimeout(() => {
            console.log('Timeout waiting for job, forcing shutdown');
            process.exit(1);
        }, 30000);

        while (currentJob) {
            await new Promise(resolve => setTimeout(resolve, 500));
        }
        clearTimeout(timeout);
    }

    try {
        await queueService.disconnect();
        await mongoose.disconnect();
        console.log('âœ“ Connections closed');
    } catch (err) {
        console.error('Error during cleanup:', err.message);
    }

    process.exit(0);
}

async function main() {
    console.log('â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•');
    console.log('  RAG Query Worker');
    console.log('â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•');
    console.log(`  Worker ID:   ${WORKER_ID}`);
    console.log(`  Worker Type: ${WORKER_TYPE}`);
    console.log(`  Poll Interval: ${POLL_INTERVAL_MS}ms`);
    console.log(`  Heartbeat Interval: ${HEARTBEAT_INTERVAL_MS}ms`);
    console.log('â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•\n');

    try {
        await mongoose.connect(process.env.MONGO_URI);
        console.log('âœ“ Connected to MongoDB');

        const redisOk = await queueService.checkConnection();
        if (!redisOk) {
            throw new Error('Cannot connect to Redis');
        }
        console.log('âœ“ Connected to Redis');

        process.on('SIGINT', () => shutdown('SIGINT'));
        process.on('SIGTERM', () => shutdown('SIGTERM'));

        await workerLoop();

    } catch (error) {
        console.error('âŒ RAG worker failed to start:', error.message);
        process.exit(1);
    }
}

main();
