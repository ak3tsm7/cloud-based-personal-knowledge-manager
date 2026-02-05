require('dotenv').config();
const axios = require('axios');
const performanceLogger = require('../utils/performance-logger');
const TestDataGenerator = require('../utils/test-data-generator');
const fs = require('fs');
const path = require('path');

/**
 * RAG End-to-End Benchmark (Async)
 * Enqueues RAG jobs and polls until completion to measure end-to-end latency.
 */

async function getAuthToken() {
    const email = process.env.BENCHMARK_USER_EMAIL || 'benchmark.user@example.com';
    const password = process.env.BENCHMARK_USER_PASSWORD || 'BenchmarkPass123!';

    try {
        const loginResponse = await axios.post('http://localhost:5000/api/auth/login', {
            email,
            password
        });
        return loginResponse.data.data.token;
    } catch (loginError) {
        try {
            await axios.post('http://localhost:5000/api/auth/register', {
                name: 'Benchmark User',
                email,
                password
            });
            const loginResponse = await axios.post('http://localhost:5000/api/auth/login', {
                email,
                password
            });
            return loginResponse.data.data.token;
        } catch (registerError) {
            throw new Error('Failed to register/login benchmark user');
        }
    }
}

async function enqueueRagQuestion(question, topK, minScore, token) {
    const response = await axios.post(
        'http://localhost:5000/api/rag/ask',
        { question, topK, minScore },
        { headers: { Authorization: `Bearer ${token}` } }
    );
    return {
        jobId: response.data?.jobId || null,
        immediateResult: response.data?.data || null
    };
}

async function pollRagResult(jobId, token, timeoutMs = 60000, immediateResult = null) {
    if (immediateResult) {
        return immediateResult;
    }
    if (!jobId) {
        throw new Error('Missing jobId from RAG enqueue');
    }
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
        const response = await axios.get(
            `http://localhost:5000/api/rag/status/${jobId}`,
            { headers: { Authorization: `Bearer ${token}` } }
        );
        const status = response.data?.data?.status;
        if (status === 'completed') return response.data.data.result;
        if (status === 'failed') throw new Error('RAG job failed');
        await new Promise(resolve => setTimeout(resolve, 500));
    }
    throw new Error('RAG job timed out');
}

async function benchmarkRAGQuery(token) {
    console.log('\n=== RAG Query Benchmark ===');

    const questions = TestDataGenerator.generateTestQuestions(10);
    const topKValues = [5, 10, 20];
    const results = [];

    for (const topK of topKValues) {
        const durations = [];
        const componentTimes = {
            embedding: [],
            retrieval: [],
            llm: [],
            total: []
        };

        for (const question of questions) {
            const startTime = performanceLogger.startTimer();

            try {
                const { jobId, immediateResult } = await enqueueRagQuestion(question, topK, 0.3, token);
                const result = await pollRagResult(jobId, token, 60000, immediateResult);

                const duration = performanceLogger.endTimer(startTime);
                durations.push(duration);

                if (result?.metadata) {
                    if (result.metadata.embeddingTime) {
                        componentTimes.embedding.push(result.metadata.embeddingTime);
                    }
                    if (result.metadata.retrievalTime) {
                        componentTimes.retrieval.push(result.metadata.retrievalTime);
                    }
                    if (result.metadata.llmTime) {
                        componentTimes.llm.push(result.metadata.llmTime);
                    }
                }
                componentTimes.total.push(duration);

            } catch (error) {
                console.error(`Error in RAG query (topK=${topK}):`, error.message);
            }
        }

        const avgDuration = durations.reduce((a, b) => a + b, 0) / durations.length;

        const result = {
            topK,
            queries: questions.length,
            avgDuration: avgDuration.toFixed(2),
            minDuration: Math.min(...durations).toFixed(2),
            maxDuration: Math.max(...durations).toFixed(2),
            queriesPerSecond: (1000 / avgDuration).toFixed(2)
        };

        if (componentTimes.embedding.length > 0) {
            result.componentBreakdown = {
                embedding: (componentTimes.embedding.reduce((a, b) => a + b, 0) / componentTimes.embedding.length).toFixed(2),
                retrieval: (componentTimes.retrieval.reduce((a, b) => a + b, 0) / componentTimes.retrieval.length).toFixed(2),
                llm: (componentTimes.llm.reduce((a, b) => a + b, 0) / componentTimes.llm.length).toFixed(2)
            };
        }

        results.push(result);

        console.log(`TopK: ${topK}`);
        console.log(`  Avg: ${avgDuration.toFixed(2)}ms`);
        console.log(`  Min: ${Math.min(...durations).toFixed(2)}ms`);
        console.log(`  Max: ${Math.max(...durations).toFixed(2)}ms`);
        console.log(`  Queries/sec: ${(1000 / avgDuration).toFixed(2)}`);
    }

    return results;
}

async function benchmarkConcurrentRAGQueries(token) {
    console.log('\n=== Concurrent RAG Query Benchmark ===');

    const concurrencyLevels = [1, 3, 5, 10];
    const question = "What is machine learning?";
    const results = [];

    for (const concurrency of concurrencyLevels) {
        const startTime = performanceLogger.startTimer();

        try {
            const promises = [];
            for (let i = 0; i < concurrency; i++) {
                promises.push((async () => {
                    const { jobId, immediateResult } = await enqueueRagQuestion(question, 5, 0.3, token);
                    await pollRagResult(jobId, token, 60000, immediateResult);
                })());
            }

            await Promise.all(promises);
            const duration = performanceLogger.endTimer(startTime);

            const throughput = (concurrency / (duration / 1000)).toFixed(2);

            results.push({
                concurrency,
                duration: duration.toFixed(2),
                throughput,
                avgLatency: (duration / concurrency).toFixed(2)
            });

            console.log(`Concurrency: ${concurrency}`);
            console.log(`  Total Duration: ${duration.toFixed(2)}ms`);
            console.log(`  Throughput: ${throughput} queries/sec`);
            console.log(`  Avg Latency: ${(duration / concurrency).toFixed(2)}ms`);
        } catch (error) {
            console.error(`Error in concurrent RAG test (${concurrency}):`, error.message);
        }
    }

    return results;
}

async function benchmarkRAGWithDifferentQueries(token) {
    console.log('\n=== RAG Query Complexity Benchmark ===');

    const queryTypes = {
        short: "What is AI?",
        medium: "How does machine learning work in practice?",
        long: "Can you explain the differences between supervised learning, unsupervised learning, and reinforcement learning in machine learning, and provide examples of each?",
        complex: "What are the key architectural differences between transformer models and recurrent neural networks, and how do these differences impact their performance on natural language processing tasks?"
    };

    const results = [];

    for (const [type, question] of Object.entries(queryTypes)) {
        const iterations = 5;
        const durations = [];

        for (let i = 0; i < iterations; i++) {
            const startTime = performanceLogger.startTimer();

            try {
                const { jobId, immediateResult } = await enqueueRagQuestion(question, 5, 0.3, token);
                await pollRagResult(jobId, token, 60000, immediateResult);
                const duration = performanceLogger.endTimer(startTime);
                durations.push(duration);
            } catch (error) {
                console.error(`Error in ${type} query:`, error.message || error);
            }
        }

        const avgDuration = durations.reduce((a, b) => a + b, 0) / durations.length;

        results.push({
            queryType: type,
            questionLength: question.length,
            iterations,
            avgDuration: avgDuration.toFixed(2),
            minDuration: Math.min(...durations).toFixed(2),
            maxDuration: Math.max(...durations).toFixed(2)
        });

        console.log(`Query Type: ${type} (${question.length} chars)`);
        console.log(`  Avg: ${avgDuration.toFixed(2)}ms`);
        console.log(`  Min: ${Math.min(...durations).toFixed(2)}ms`);
        console.log(`  Max: ${Math.max(...durations).toFixed(2)}ms`);
    }

    return results;
}

async function runRAGBenchmark() {
    console.log('\n╔════════════════════════════════════════╗');
    console.log('║      RAG END-TO-END BENCHMARK         ║');
    console.log('╚════════════════════════════════════════╝');

    const results = {
        timestamp: new Date().toISOString(),
        benchmarks: {}
    };

    try {
        const token = await getAuthToken();

        results.benchmarks.standard = await benchmarkRAGQuery(token);
        results.benchmarks.concurrent = await benchmarkConcurrentRAGQueries(token);
        results.benchmarks.complexity = await benchmarkRAGWithDifferentQueries(token);

        const resultsDir = path.join(process.cwd(), 'performance', 'reports');
        if (!fs.existsSync(resultsDir)) {
            fs.mkdirSync(resultsDir, { recursive: true });
        }

        const resultsFile = path.join(resultsDir, 'rag-benchmark-results.json');
        fs.writeFileSync(resultsFile, JSON.stringify(results, null, 2));

        console.log('\n✅ RAG benchmark completed!');
        console.log(`📊 Results saved to: ${resultsFile}`);

        return results;
    } catch (error) {
        console.error('\n❌ RAG benchmark failed:', error.message);
        throw error;
    }
}

if (require.main === module) {
    runRAGBenchmark()
        .then(() => process.exit(0))
        .catch(error => {
            console.error(error);
            process.exit(1);
        });
}

module.exports = { runRAGBenchmark };
