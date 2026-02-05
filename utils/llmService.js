const OpenAI = require('openai');
const crypto = require('crypto');

/**
 * LLM Service for RAG System
 * 
 * Uses NVIDIA's API to call various LLM models for question answering
 * with context from vector search results.
 */

// Initialize OpenAI client with NVIDIA API configuration
const apiKey = process.env.NVIDIA_API_KEY || process.env.OPENAI_API_KEY;
if (!apiKey) {
    console.error('Missing NVIDIA_API_KEY (or OPENAI_API_KEY) in environment');
}

const openai = new OpenAI({
    apiKey,
    baseURL: 'https://integrate.api.nvidia.com/v1',
    timeout: 20000, // 20 second timeout
});

// You can use different models - these are available on NVIDIA API
const MODELS = {
    LLAMA_70B: 'meta/llama-3.1-70b-instruct',     // Recommended for best quality
    LLAMA_8B: 'meta/llama-3.1-8b-instruct',       // Faster, good quality
    MISTRAL_7B: 'mistralai/mistral-7b-instruct-v0.3', // Alternative
};

const DEFAULT_MODEL = MODELS.LLAMA_70B; // Use 8B for speed, change to 70B for better answers

// Simple in-memory cache for identical questions
const answerCache = new Map();
const CACHE_TTL = 300000; // 5 minutes
const MAX_CACHE_SIZE = 100;

function getCacheKey(question, context) {
    const hash = crypto.createHash('md5');
    hash.update(question + context.substring(0, 500)); // Use first 500 chars of context
    return hash.digest('hex');
}

function cleanCache() {
    if (answerCache.size > MAX_CACHE_SIZE) {
        const firstKey = answerCache.keys().next().value;
        answerCache.delete(firstKey);
    }
}

/**
 * Generate answer using RAG (Retrieval-Augmented Generation)
 * @param {string} question - User's question
 * @param {string} context - Retrieved context from vector search
 * @param {Object} options - Additional options
 * @returns {Promise<string>} - Generated answer
 */
async function generateAnswer(question, context, options = {}) {
    try {
        const {
            model = DEFAULT_MODEL,
            temperature = 0.2, // Low temperature for factual answers
            maxTokens = 500,
            // The 'result' object is expected to be passed in options for metadata
            result = {},
            useCache = true,
        } = options;

        // Check cache first
        if (useCache) {
            const cacheKey = getCacheKey(question, context);
            const cached = answerCache.get(cacheKey);
            if (cached && Date.now() - cached.timestamp < CACHE_TTL) {
                console.log('Returning cached answer');
                return cached.answer;
            }
        }

        console.log(`Generating answer using ${model}...`);

        // Construct the RAG prompt with file metadata
        const uniqueFiles = result.metadata?.uniqueFiles || 'unknown';
        const uniqueFileNames = result.metadata?.uniqueFileNames || [];

        const systemPrompt = `You are a helpful assistant that answers questions based ONLY on the provided context.

IMPORTANT RULES (STRICT):

1. Answer ONLY using facts that are explicitly stated in the provided context.
2. Do NOT add, infer, guess, summarize, or complete information that is not directly present in the context.
3. If a detail (tool, percentage, skill, technology, name, number) is not explicitly mentioned, do NOT include it.
4. Use the provided context to answer the question to the best of your ability, even if the information is partial, fragmented, or messy (e.g., OCR output).
5. If the context contains only partial information, answer using ONLY that partial information.
6. Only say "I don't have enough information" if the context contains absolutely no relevant information related to the question.
7. If the question asks for information beyond the context, clearly state that the context does not provide that detail.

CRITICAL - UNDERSTANDING FILES vs CHUNKS:
- The context below contains ${result.metadata?.chunksUsed || 'multiple'} text chunks retrieved from ${uniqueFiles} unique file(s)
- Multiple chunks can come from the SAME file
- If asked "how many files", count UNIQUE filenames, not chunks
- The ${uniqueFiles} unique file(s) are: ${uniqueFileNames.join(', ')}
- Do NOT confuse chunks with files!
`;

        const userPrompt = `CONTEXT:
${context}

QUESTION:
${question}

Please provide a clear and accurate answer based only on the context above.`;

        // Call the LLM
        const response = await openai.chat.completions.create({
            model: model,
            messages: [
                { role: 'system', content: systemPrompt },
                { role: 'user', content: userPrompt }
            ],
            temperature: temperature,
            max_tokens: maxTokens,
            stream: false,
        });

        const answer = response.choices[0].message.content;
        console.log('Answer generated successfully');

        // Cache the answer
        if (useCache) {
            const cacheKey = getCacheKey(question, context);
            answerCache.set(cacheKey, {
                answer,
                timestamp: Date.now()
            });
            cleanCache();
        }

        return answer;

    } catch (error) {
        console.error('Error generating answer:', error.message);

        // Handle rate limiting
        if (error.status === 429) {
            throw new Error('Rate limit exceeded. Please try again in a moment.');
        }

        throw error;
    }
}

/**
 * Generate streaming answer (for real-time responses)
 * @param {string} question - User's question
 * @param {string} context - Retrieved context
 * @param {Function} onChunk - Callback for each chunk
 * @param {Object} options - Additional options
 */
async function generateAnswerStream(question, context, onChunk, options = {}) {
    try {
        const {
            model = DEFAULT_MODEL,
            temperature = 0.2,
            maxTokens = 500,
        } = options;

        console.log(`Generating streaming answer using ${model}...`);

        const systemPrompt = `You are a helpful assistant that answers questions based ONLY on the provided context.

IMPORTANT RULES (STRICT):

1. Answer ONLY using facts that are explicitly stated in the provided context.
2. Do NOT add, infer, guess, summarize, or complete information that is not directly present in the context.
3. If a detail (tool, percentage, skill, technology, name, number) is not explicitly mentioned, do NOT include it.
4. Use the provided context to answer the question to the best of your ability, even if the information is partial, fragmented, or messy (e.g., OCR output).
5. If the context contains only partial information, answer using ONLY that partial information.
6. Only say "I don't have enough information" if the context contains absolutely no relevant information related to the question.
7. If the question asks for information beyond the context, clearly state that the context does not provide that detail.
`;

        const userPrompt = `CONTEXT:
${context}

QUESTION:
${question}

Answer:`;

        const stream = await openai.chat.completions.create({
            model: model,
            messages: [
                { role: 'system', content: systemPrompt },
                { role: 'user', content: userPrompt }
            ],
            temperature: temperature,
            max_tokens: maxTokens,
            stream: true,
        });

        let fullAnswer = '';

        for await (const chunk of stream) {
            const content = chunk.choices[0]?.delta?.content || '';
            if (content) {
                fullAnswer += content;
                onChunk(content);
            }
        }

        return fullAnswer;

    } catch (error) {
        console.error('Error in streaming generation:', error.message);
        throw error;
    }
}

/**
 * Simple chat completion (without RAG context)
 * @param {string} message - User message
 * @param {Object} options - Additional options
 * @returns {Promise<string>} - Generated response
 */
async function chat(message, options = {}) {
    try {
        const {
            model = DEFAULT_MODEL,
            temperature = 0.7,
            maxTokens = 500,
        } = options;

        const response = await openai.chat.completions.create({
            model: model,
            messages: [
                { role: 'user', content: message }
            ],
            temperature: temperature,
            max_tokens: maxTokens,
        });

        return response.choices[0].message.content;

    } catch (error) {
        console.error('Error in chat:', error.message);
        throw error;
    }
}

module.exports = {
    generateAnswer,
    generateAnswerStream,
    chat,
    MODELS,
};
