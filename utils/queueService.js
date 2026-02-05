/**
 * Queue Service - Redis-based Job Queue Integration
 * 
 * Connects to the same Redis instance as the Go distributed task queue.
 * Uses matching key patterns for cross-language compatibility.
 * 
 * GRACEFUL DEGRADATION: If Redis is unavailable, async processing is disabled
 * and the server falls back to synchronous mode.
 */

const Redis = require('ioredis');
const { v4: uuidv4 } = require('uuid');

// Lazy Redis connection - only connect when needed
let redis = null;
let redisAvailable = null; // null = unknown, true/false = known state

/**
 * Check if Redis is available
 */
async function isRedisAvailable() {
    if (redisAvailable !== null) {
        return redisAvailable;
    }

    try {
        const client = getRedisClient();
        await client.ping();
        redisAvailable = true;
        return true;
    } catch (error) {
        console.error('[DEBUG] Redis Ping Error:', error.message);
        redisAvailable = false;
        return false;
    }
}

/**
 * Get Redis client (lazy initialization)
 */
function getRedisClient() {
    if (!redis) {
        const redisConfig = {
            host: process.env.REDIS_HOST || 'localhost',
            port: process.env.REDIS_PORT || 6379,
            maxRetriesPerRequest: 1,
            retryStrategy(times) {
                if (times > 2) {
                    return null; // Stop retrying
                }
                return Math.min(times * 100, 500);
            },
            lazyConnect: true,
            enableOfflineQueue: true,
        };
        console.log(`[DEBUG] Connecting to Redis at ${redisConfig.host}:${redisConfig.port}`);

        redis = new Redis(redisConfig);

        redis.on('error', () => {
            // Silently handle errors - we check availability explicitly
            redisAvailable = false;
        });

        redis.on('end', () => {
            redisAvailable = false;
        });

        redis.on('connect', () => {
            console.log('✓ Connected to Redis for job queue');
            redisAvailable = true;
        });
    }

    return redis;
}

/**
 * Enqueue a file processing job
 * 
 * @returns {Promise<string|null>} - Job ID or null if Redis unavailable
 */
async function enqueueJob(jobData) {
    if (!(await isRedisAvailable())) {
        console.warn('⚠️  Redis unavailable - cannot queue job');
        return null;
    }

    const client = getRedisClient();
    const jobId = uuidv4();
    const workerType = jobData.mimetype?.includes('image') ? 'gpu' : 'cpu';

    const job = {
        job_id: jobId,
        task_type: 'PROCESS_FILE',
        requires: workerType,
        priority: jobData.priority || 5,
        payload: {
            fileId: jobData.fileId,
            filepath: jobData.filepath,
            userId: jobData.userId,
            mimetype: jobData.mimetype,
            filename: jobData.filename,
        },
        timeout_ms: 300000,
        metadata: {
            source: 'rag-api',
            created_at: new Date().toISOString(),
        },
    };

    const jobKey = `job:${jobId}`;
    const queue = `queue:${workerType}`;

    try {
        await client.hset(jobKey, {
            payload: JSON.stringify(job),
            metadata: JSON.stringify(job.metadata),
            status: 'queued',
            created_at: Math.floor(Date.now() / 1000),
        });

        await client.zadd(queue, -job.priority, jobId);
    } catch (error) {
        console.error('✗ Redis enqueue failed:', error.message);
        redisAvailable = false;
        return null;
    }

    console.log(`✓ Job ${jobId} enqueued to ${queue}`);
    return jobId;
}

/**
 * Enqueue a RAG query job
 * 
 * @returns {Promise<string|null>} - Job ID or null if Redis unavailable
 */
async function enqueueRagJob(jobData) {
    if (!(await isRedisAvailable())) {
        console.warn('âš ï¸  Redis unavailable - cannot queue RAG job');
        return null;
    }

    const client = getRedisClient();
    const jobId = uuidv4();

    const job = {
        job_id: jobId,
        task_type: jobData.fileId ? 'RAG_QUERY_FILE' : 'RAG_QUERY',
        requires: 'rag',
        priority: jobData.priority || 5,
        payload: {
            userId: jobData.userId,
            question: jobData.question,
            topK: jobData.topK,
            minScore: jobData.minScore,
            fileId: jobData.fileId || null,
        },
        timeout_ms: 60000,
        metadata: {
            source: 'rag-api',
            created_at: new Date().toISOString(),
        },
    };

    const jobKey = `job:${jobId}`;
    const queue = `queue:rag`;

    try {
        await client.hset(jobKey, {
            payload: JSON.stringify(job),
            metadata: JSON.stringify(job.metadata),
            status: 'queued',
            created_at: Math.floor(Date.now() / 1000),
        });

        await client.zadd(queue, -job.priority, jobId);
    } catch (error) {
        console.error('âœ— Redis RAG enqueue failed:', error.message);
        redisAvailable = false;
        return null;
    }

    console.log(`âœ“ RAG job ${jobId} enqueued`);
    return jobId;
}

/**
 * Claim a job from the queue (for Node.js workers)
 */
async function claimJob(workerType, workerId) {
    if (!(await isRedisAvailable())) {
        return null;
    }

    const client = getRedisClient();
    const queues = [`queue:${workerType}`, 'queue:any'];

    for (const queueKey of queues) {
        const result = await client.zpopmax(queueKey, 1);

        if (result && result.length >= 2) {
            const jobId = result[0];
            const jobKey = `job:${jobId}`;

            const payload = await client.hget(jobKey, 'payload');
            if (!payload) {
                continue;
            }

            const job = JSON.parse(payload);

            const runningKey = `running:${workerId}`;
            await client.hset(runningKey, jobId, Math.floor(Date.now() / 1000));
            await client.hset(jobKey, 'status', 'running');
            await client.hset(jobKey, 'started_at', Math.floor(Date.now() / 1000));
            await client.hset(jobKey, 'worker_id', workerId);

            console.log(`✓ Worker ${workerId} claimed job ${jobId}`);
            return job;
        }
    }

    return null;
}

/**
 * Update job progress
 */
async function updateJobProgress(jobId, progress, chunksProcessed) {
    if (!(await isRedisAvailable())) return;

    const client = getRedisClient();
    const jobKey = `job:${jobId}`;
    await client.hset(jobKey, {
        progress: progress,
        chunks_processed: chunksProcessed,
        last_heartbeat: Math.floor(Date.now() / 1000),
    });
}

/**
 * Send heartbeat for a running job
 */
async function sendHeartbeat(jobId, workerId) {
    if (!(await isRedisAvailable())) return;

    const client = getRedisClient();
    await client.hset(`job:${jobId}`, 'last_heartbeat', Math.floor(Date.now() / 1000));
    await client.hset(`running:${workerId}`, jobId, Math.floor(Date.now() / 1000));
}

/**
 * Mark job as completed
 */
async function completeJob(jobId, workerId, result = {}) {
    if (!(await isRedisAvailable())) return;

    const client = getRedisClient();
    await client.hset(`job:${jobId}`, {
        status: 'completed',
        completed_at: Math.floor(Date.now() / 1000),
        result: JSON.stringify(result),
    });
    await client.hdel(`running:${workerId}`, jobId);
    console.log(`✓ Job ${jobId} completed`);
}

/**
 * Mark job as failed
 */
async function failJob(jobId, workerId, error) {
    if (!(await isRedisAvailable())) return;

    const client = getRedisClient();
    await client.hset(`job:${jobId}`, {
        status: 'failed',
        failed_at: Math.floor(Date.now() / 1000),
        error: error,
    });
    await client.hdel(`running:${workerId}`, jobId);
    console.error(`✗ Job ${jobId} failed: ${error}`);
}

/**
 * Get job status
 */
async function getJobStatus(jobId) {
    if (!(await isRedisAvailable())) return null;

    const client = getRedisClient();
    const data = await client.hgetall(`job:${jobId}`);

    if (!data || Object.keys(data).length === 0) {
        return null;
    }

    return {
        jobId,
        status: data.status || 'unknown',
        progress: parseInt(data.progress) || 0,
        chunksProcessed: parseInt(data.chunks_processed) || 0,
        createdAt: data.created_at ? new Date(parseInt(data.created_at) * 1000) : null,
        startedAt: data.started_at ? new Date(parseInt(data.started_at) * 1000) : null,
        completedAt: data.completed_at ? new Date(parseInt(data.completed_at) * 1000) : null,
        lastHeartbeat: data.last_heartbeat ? new Date(parseInt(data.last_heartbeat) * 1000) : null,
        error: data.error || null,
        result: data.result ? JSON.parse(data.result) : null,
    };
}

/**
 * Get queue statistics
 */
async function getQueueStats() {
    if (!(await isRedisAvailable())) {
        return { queued: { cpu: 0, gpu: 0, any: 0, total: 0 } };
    }

    const client = getRedisClient();
    const [cpuQueue, gpuQueue, anyQueue] = await Promise.all([
        client.zcard('queue:cpu'),
        client.zcard('queue:gpu'),
        client.zcard('queue:any'),
    ]);

    return {
        queued: {
            cpu: cpuQueue,
            gpu: gpuQueue,
            any: anyQueue,
            total: cpuQueue + gpuQueue + anyQueue,
        },
    };
}

/**
 * Check Redis connection health
 */
async function checkConnection() {
    return await isRedisAvailable();
}

/**
 * Close Redis connection
 */
async function disconnect() {
    if (redis) {
        await redis.quit();
        redis = null;
        redisAvailable = null;
    }
}

module.exports = {
    enqueueJob,
    enqueueRagJob,
    claimJob,
    updateJobProgress,
    sendHeartbeat,
    completeJob,
    failJob,
    getJobStatus,
    getQueueStats,
    checkConnection,
    isRedisAvailable,
    disconnect,
};


