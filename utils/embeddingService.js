const qdrantService = require('./qdrantService');

const EMBEDDING_API_URL =
    process.env.EMBEDDING_API_URL || 'http://embedding-service:8001';

const BATCH_SIZE = 12;
const EMBEDDING_DIM = 1024;
const REQUEST_TIMEOUT = 30000; // 30 seconds (increased for Docker service)
const MAX_RETRIES = 2;

// Health check cache
let serviceHealthy = true;
let lastHealthCheck = 0;
const HEALTH_CHECK_INTERVAL = 60000; // 1 minute

// ---------------- Health Check ----------------

async function checkServiceHealth(forceCheck = false) {
    const now = Date.now();

    // Skip cache if forceCheck is true
    if (!forceCheck && now - lastHealthCheck < HEALTH_CHECK_INTERVAL) {
        return serviceHealthy;
    }

    try {
        // Embedding service can be slow on first request, use longer timeout
        const res = await fetch(`${EMBEDDING_API_URL}/health`, {
            signal: AbortSignal.timeout(30000) // 30 second timeout for health check
        });

        serviceHealthy = res.ok;
        lastHealthCheck = now;
        return serviceHealthy;
    } catch (error) {
        serviceHealthy = false;
        lastHealthCheck = now;
        console.warn('Embedding service health check failed:', error.message);
        return false;
    }
}

// Reset health status to allow retry
function resetHealthStatus() {
    serviceHealthy = true;
    lastHealthCheck = 0;
}

// ---------------- Single Embedding ----------------

async function generateEmbedding(text, retries = MAX_RETRIES) {
    if (!text || typeof text !== 'string' || !text.trim()) {
        return null;
    }

    // Quick health check
    const healthy = await checkServiceHealth();
    if (!healthy) {
        throw new Error('Embedding service is unavailable');
    }

    try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);

        const res = await fetch(`${EMBEDDING_API_URL}/embed`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ text }),
            signal: controller.signal
        });

        clearTimeout(timeout);

        if (!res.ok) {
            throw new Error(`Embedding service error: ${res.statusText}`);
        }

        const { embedding } = await res.json();

        if (!Array.isArray(embedding) || embedding.length !== EMBEDDING_DIM) {
            return null;
        }

        return embedding;
    } catch (error) {
        if (error.name === 'AbortError') {
            console.warn('Embedding request timeout');
            if (retries > 0) {
                console.log(`Retrying... (${retries} attempts left)`);
                await new Promise(resolve => setTimeout(resolve, 1000));
                return generateEmbedding(text, retries - 1);
            }
            throw new Error('Embedding request timeout after retries');
        }
        throw error;
    }
}

// ---------------- Batch Embeddings ----------------

async function generateBatchEmbeddings(texts) {
    if (!Array.isArray(texts) || texts.length === 0) {
        return [];
    }

    const results = [];

    for (let i = 0; i < texts.length; i += BATCH_SIZE) {
        const batch = texts.slice(i, i + BATCH_SIZE);

        const payload = batch.map(t =>
            typeof t === 'string' && t.trim() ? t : " "
        );

        try {
            const controller = new AbortController();
            const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT * 2); // Longer timeout for batch

            const res = await fetch(`${EMBEDDING_API_URL}/embed/batch`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ texts: payload }),
                signal: controller.signal
            });

            clearTimeout(timeout);

            if (!res.ok) {
                throw new Error(res.statusText);
            }

            const { embeddings } = await res.json();

            for (let j = 0; j < batch.length; j++) {
                const vec = embeddings[j];
                if (Array.isArray(vec) && vec.length === EMBEDDING_DIM) {
                    results.push(vec);
                } else {
                    results.push(null);
                }
            }
        } catch (err) {
            console.error('Embedding batch failed:', err.message);
            results.push(...new Array(batch.length).fill(null));
        }
    }

    return results;
}

// ---------------- Chunk Embeddings ----------------

async function generateChunkEmbeddings(chunks) {
    if (!Array.isArray(chunks) || chunks.length === 0) return chunks;

    const texts = chunks.map(c => c.text || '');
    const embeddings = await generateBatchEmbeddings(texts);

    return chunks.map((chunk, i) => ({
        ...chunk,
        embedding: embeddings[i]
    }));
}

// ---------------- Store in Qdrant ----------------

async function generateAndStoreChunkEmbeddings(chunks, fileId, fileName, userId) {
    const enriched = await generateChunkEmbeddings(chunks);

    const total = enriched.length;
    const successful = enriched.filter(c =>
        Array.isArray(c.embedding) && c.embedding.length === EMBEDDING_DIM
    ).length;
    const failed = total - successful;

    const qdrantIds = await qdrantService.storeChunkEmbeddings(
        enriched,
        fileId,
        fileName,
        userId
    );

    return {
        chunks: enriched.map(({ embedding, ...rest }) => rest),
        qdrantIds,
        stats: {
            total,
            successful,
            failed
        }
    };
}

// ---------------- Cosine Similarity ----------------

function cosineSimilarity(a, b) {
    if (!a || !b || a.length !== b.length) return 0;

    let dot = 0, na = 0, nb = 0;
    for (let i = 0; i < a.length; i++) {
        dot += a[i] * b[i];
        na += a[i] ** 2;
        nb += b[i] ** 2;
    }
    return dot / (Math.sqrt(na) * Math.sqrt(nb) || 1);
}

module.exports = {
    generateEmbedding,
    generateBatchEmbeddings,
    generateChunkEmbeddings,
    generateAndStoreChunkEmbeddings,
    cosineSimilarity,
    checkServiceHealth,
    resetHealthStatus
};
