require('dotenv').config();
const axios = require('axios');
const fs = require('fs');
const path = require('path');

/**
 * RAG Query Load Test (Async)
 * Enqueues RAG jobs and polls until completion to measure end-to-end latency.
 */

function percentile(values, p) {
    if (!values.length) return null;
    const sorted = [...values].sort((a, b) => a - b);
    const index = Math.ceil((p / 100) * sorted.length) - 1;
    return sorted[Math.max(0, Math.min(sorted.length - 1, index))];
}

async function getAuthToken() {
    const email = 'testuser@example.com';
    const password = 'TestPassword123!';

    try {
        const response = await axios.post('http://localhost:5000/api/auth/login', {
            email,
            password
        });
        console.log('✅ Login successful!');
        return response.data.data.token;
    } catch (error) {
        console.log('Login failed, attempting to register test user...');
        try {
            await axios.post('http://localhost:5000/api/auth/register', {
                name: 'Test User',
                email,
                password
            });
            const loginResponse = await axios.post('http://localhost:5000/api/auth/login', {
                email,
                password
            });
            console.log('✅ Login successful!');
            return loginResponse.data.data.token;
        } catch (registerError) {
            console.error('Failed to register/login test user:', registerError.response?.data || registerError.message);
            console.error('\nPlease manually create a test user:');
            console.error('  Email: testuser@example.com');
            console.error('  Password: TestPassword123!');
            console.error('\nOr run: npm run setup-test-user');
            return null;
        }
    }
}

async function enqueueRag(question, token) {
    const response = await axios.post(
        'http://localhost:5000/api/rag/ask',
        { question, topK: 5, minScore: 0.3 },
        { headers: { Authorization: `Bearer ${token}` } }
    );
    return response.data.jobId;
}

async function pollRag(jobId, token, timeoutMs = 60000) {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
        const response = await axios.get(
            `http://localhost:5000/api/rag/status/${jobId}`,
            { headers: { Authorization: `Bearer ${token}` } }
        );
        const status = response.data?.data?.status;
        if (status === 'completed') return true;
        if (status === 'failed') throw new Error('RAG job failed');
        await new Promise(resolve => setTimeout(resolve, 500));
    }
    throw new Error('RAG job timed out');
}

async function runScenario(name, connections, durationSeconds, token) {
    console.log(`\n📊 Running: ${name}`);
    console.log(`   Connections: ${connections}`);
    console.log(`   Duration: ${durationSeconds}s\n`);

    const endTime = Date.now() + durationSeconds * 1000;
    const latencies = [];
    let errors = 0;
    let completed = 0;

    async function worker() {
        while (Date.now() < endTime) {
            const start = Date.now();
            try {
                const jobId = await enqueueRag('What is machine learning?', token);
                await pollRag(jobId, token);
                latencies.push(Date.now() - start);
                completed += 1;
            } catch (error) {
                errors += 1;
            }
        }
    }

    const workers = Array.from({ length: connections }, () => worker());
    await Promise.all(workers);

    const avg = latencies.length
        ? latencies.reduce((a, b) => a + b, 0) / latencies.length
        : null;

    return {
        scenario: name,
        connections,
        duration: durationSeconds,
        requests: {
            total: completed + errors,
            completed,
            errors,
            avg: avg ? avg.toFixed(2) : 'N/A',
            p50: percentile(latencies, 50),
            p95: percentile(latencies, 95),
            p99: percentile(latencies, 99),
            max: latencies.length ? Math.max(...latencies) : null
        }
    };
}

async function runRAGLoadTest() {
    console.log('\n╔════════════════════════════════════════╗');
    console.log('║       RAG QUERY LOAD TEST (ASYNC)     ║');
    console.log('╚════════════════════════════════════════╝\n');

    const authToken = await getAuthToken();
    if (!authToken) {
        console.error('❌ Failed to get auth token. Make sure server is running and test user exists.');
        return;
    }

    const scenarios = [
        { name: 'Light Load', connections: 5, duration: 30 },
        { name: 'Medium Load', connections: 15, duration: 30 },
        { name: 'Heavy Load', connections: 30, duration: 30 }
    ];

    const results = [];
    for (const scenario of scenarios) {
        const result = await runScenario(scenario.name, scenario.connections, scenario.duration, authToken);
        results.push(result);

        console.log(`✅ ${scenario.name} completed:`);
        console.log(`   Total Requests: ${result.requests.total}`);
        console.log(`   Completed: ${result.requests.completed}`);
        console.log(`   Errors: ${result.requests.errors}`);
        console.log(`   Avg Latency: ${result.requests.avg}ms`);
        console.log(`   P50: ${result.requests.p50}ms`);
        console.log(`   P95: ${result.requests.p95}ms`);
        console.log(`   P99: ${result.requests.p99}ms`);
    }

    const resultsDir = path.join(process.cwd(), 'performance', 'reports');
    if (!fs.existsSync(resultsDir)) {
        fs.mkdirSync(resultsDir, { recursive: true });
    }

    const resultsFile = path.join(resultsDir, 'rag-load-test-results.json');
    fs.writeFileSync(resultsFile, JSON.stringify({
        timestamp: new Date().toISOString(),
        results
    }, null, 2));

    console.log(`\n📊 Results saved to: ${resultsFile}\n`);
    return results;
}

if (require.main === module) {
    runRAGLoadTest()
        .then(() => process.exit(0))
        .catch(error => {
            console.error(error);
            process.exit(1);
        });
}

module.exports = { runRAGLoadTest };
