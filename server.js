import express from "express";
import pg from "pg";

const { Pool } = pg;

const app = express();
app.use(express.json({ limit: "2mb" }));

const PORT = process.env.PORT || 10000;

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const DATABASE_URL = process.env.DATABASE_URL;

const BASE_URL =
  process.env.RENDER_EXTERNAL_URL ||
  process.env.BASE_URL ||
  "https://ai-youtube-autopilot-a24y.onrender.com";

const TELEGRAM_API =
  `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}`;

const GEMINI_API =
  "https://generativelanguage.googleapis.com/v1beta";

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 5
});

const runningJobs = new Set();

/* =====================================================
   HELPERS
===================================================== */

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function jobId() {
  return `job_${Date.now()}_${Math.floor(Math.random() * 1000)}`;
}

function clean(value) {
  return String(value ?? "").trim();
}

function isQuotaError(error) {
  const message = String(error?.message || "").toLowerCase();

  return (
    error?.status === 429 &&
    (
      message.includes("quota") ||
      message.includes("exceeded") ||
      message.includes("free_tier")
    )
  );
}

function isTransient(error) {
  const message = String(error?.message || "");

  if (isQuotaError(error)) {
    return false;
  }

  return (
    error?.status === 408 ||
    error?.status === 429 ||
    error?.status >= 500 ||
    message.includes("REQUEST_TIMEOUT") ||
    message.includes("fetch failed") ||
    message.includes("ECONNRESET") ||
    message.includes("503")
  );
}

/* =====================================================
   DATABASE
===================================================== */

async function initDatabase() {
  if (!DATABASE_URL) {
    throw new Error("DATABASE_URL missing");
  }

  await pool.query(`
    CREATE TABLE IF NOT EXISTS jobs (
      id TEXT PRIMARY KEY,
      chat_id TEXT NOT NULL,
      topic TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'queued',
      stage TEXT DEFAULT 'Job created',
      progress INTEGER DEFAULT 0,

      script TEXT,
      script_model TEXT,
      word_count INTEGER,

      tts_model TEXT,
      tts_chunk INTEGER DEFAULT 0,
      tts_total_chunks INTEGER DEFAULT 0,

      error TEXT,
      attempts INTEGER DEFAULT 0,

      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  console.log("✅ PostgreSQL database initialized");
}

async function createJob(job) {
  await pool.query(
    `
    INSERT INTO jobs
    (id, chat_id, topic, status, stage, progress)
    VALUES ($1, $2, $3, $4, $5, $6)
    `,
    [
      job.id,
      String(job.chatId),
      job.topic,
      job.status,
      job.stage,
      job.progress
    ]
  );
}

async function getJob(id) {
  const result = await pool.query(
    `SELECT * FROM jobs WHERE id = $1`,
    [id]
  );

  return result.rows[0] || null;
}

async function updateJob(id, fields) {
  const allowed = [
    "status",
    "stage",
    "progress",
    "script",
    "script_model",
    "word_count",
    "tts_model",
    "tts_chunk",
    "tts_total_chunks",
    "error",
    "attempts"
  ];

  const entries = Object.entries(fields)
    .filter(([key]) => allowed.includes(key));

  if (!entries.length) {
    return;
  }

  const values = [];
  const sets = [];

  entries.forEach(([key, value], index) => {
    values.push(value);
    sets.push(`${key} = $${index + 1}`);
  });

  values.push(id);

  await pool.query(
    `
    UPDATE jobs
    SET ${sets.join(", ")},
        updated_at = NOW()
    WHERE id = $${values.length}
    `,
    values
  );
}

async function getUserJobs(chatId) {
  const result = await pool.query(
    `
    SELECT *
    FROM jobs
    WHERE chat_id = $1
    ORDER BY created_at DESC
    LIMIT 10
    `,
    [String(chatId)]
  );

  return result.rows;
}

async function getStats() {
  const result = await pool.query(`
    SELECT
      COUNT(*) FILTER (WHERE status = 'running') AS running,
      COUNT(*) FILTER (WHERE status = 'paused') AS paused,
      COUNT(*) FILTER (WHERE status = 'completed') AS completed,
      COUNT(*) AS total
    FROM jobs
  `);

  return result.rows[0];
}

/* =====================================================
   TIMEOUT
===================================================== */

async function fetchWithTimeout(
  url,
  options = {},
  timeoutMs = 45000
) {
  const controller = new AbortController();

  const timer = setTimeout(() => {
    controller.abort();
  }, timeoutMs);

  try {
    return await fetch(url, {
      ...options,
      signal: controller.signal
    });
  } catch (error) {
    if (error.name === "AbortError") {
      throw new Error(
        `REQUEST_TIMEOUT_${timeoutMs}MS`
      );
    }

    throw error;
  } finally {
    clearTimeout(timer);
  }
}

/* =====================================================
   TELEGRAM
===================================================== */

async function telegram(method, body = {}) {
  const response = await fetchWithTimeout(
    `${TELEGRAM_API}/${method}`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify(body)
    },
    20000
  );

  const data = await response.json();

  if (!response.ok || !data.ok) {
    throw new Error(
      `Telegram ${method}: ${
        data.description || response.statusText
      }`
    );
  }

  return data.result;
}

async function sendMessage(chatId, text) {
  return telegram("sendMessage", {
    chat_id: chatId,
    text
  });
}

async function sendAudio(chatId, audioBuffer, filename) {
  const form = new FormData();

  form.append("chat_id", String(chatId));

  form.append(
    "audio",
    new Blob([audioBuffer], {
      type: "audio/wav"
    }),
    filename
  );

  const response = await fetchWithTimeout(
    `${TELEGRAM_API}/sendAudio`,
    {
      method: "POST",
      body: form
    },
    30000
  );

  const data = await response.json();

  if (!response.ok || !data.ok) {
    throw new Error(
      `Telegram sendAudio: ${
        data.description || response.statusText
      }`
    );
  }

  return data.result;
}

/* =====================================================
   GEMINI TEXT
===================================================== */

async function geminiText(model, prompt) {
  const url =
    `${GEMINI_API}/models/${model}:generateContent`;

  const response = await fetchWithTimeout(
    url,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-goog-api-key": GEMINI_API_KEY
      },
      body: JSON.stringify({
        contents: [
          {
            parts: [
              {
                text: prompt
              }
            ]
          }
        ],
        generationConfig: {
          maxOutputTokens: 5000
        }
      })
    },
    45000
  );

  const raw = await response.text();

  let data;

  try {
    data = JSON.parse(raw);
  } catch {
    data = null;
  }

  if (!response.ok) {
    const error = new Error(
      `Gemini ${response.status}: ${
        data?.error?.message ||
        raw ||
        response.statusText
      }`
    );

    error.status = response.status;

    throw error;
  }

  const text =
    data?.candidates?.[0]?.content?.parts
      ?.map(part => part.text || "")
      .join("")
      .trim() || "";

  if (!text) {
    throw new Error("GEMINI_EMPTY_RESPONSE");
  }

  return text;
}

/* =====================================================
   RETRY
===================================================== */

async function retryRequest(
  fn,
  label,
  attempts = 2
) {
  let lastError;

  for (
    let attempt = 1;
    attempt <= attempts;
    attempt++
  ) {
    try {
      console.log(
        `${label} attempt ${attempt}/${attempts}`
      );

      return await fn();

    } catch (error) {
      lastError = error;

      console.error(
        `${label} failed: ${error.message}`
      );

      // IMPORTANT:
      // Free-tier quota errors are NOT retried.
      if (isQuotaError(error)) {
        throw error;
      }

      if (
        !isTransient(error) ||
        attempt >= attempts
      ) {
        throw error;
      }

      const wait =
        Math.min(
          12000,
          1500 * (2 ** (attempt - 1))
        ) +
        Math.floor(Math.random() * 1000);

      console.log(
        `${label} retrying after ${wait}ms`
      );

      await sleep(wait);
    }
  }

  throw lastError;
}

/* =====================================================
   SCRIPT MODELS
===================================================== */

const SCRIPT_MODELS = [
  "gemini-3.6-flash",
  "gemini-3.5-flash",
  "gemini-3.5-flash-lite",
  "gemini-3.1-flash-lite",
  "gemini-2.5-flash"
];

/* =====================================================
   SCRIPT GENERATION
===================================================== */

async function generateScript(topic) {
  let lastError = null;

  for (const model of SCRIPT_MODELS) {
    try {
      console.log(
        `Trying script model: ${model}`
      );

      const prompt = `
Create a completely original YouTube narration.

TOPIC:
${topic}

Requirements:
- 600 to 800 spoken words
- Strong opening hook
- Interesting storytelling
- Accurate and responsible facts
- Natural spoken English
- Suitable for a faceless YouTube video
- No copied article wording
- No fake sources
- No unnecessary headings
- No "welcome back"
- Do not mention AI
- Return ONLY the narration

Make every sentence useful and engaging.
      `.trim();

      const script =
        await retryRequest(
          () => geminiText(model, prompt),
          `SCRIPT ${model}`,
          2
        );

      if (script.length < 1200) {
        throw new Error(
          "SCRIPT_TOO_SHORT"
        );
      }

      return {
        script,
        model
      };

    } catch (error) {
      lastError = error;

      console.error(
        `Script model failed ${model}:`,
        error.message
      );

      await sleep(500);
    }
  }

  throw (
    lastError ||
    new Error("ALL_SCRIPT_MODELS_FAILED")
  );
}

/* =====================================================
   QUALITY CHECK
===================================================== */

function checkScript(script) {
  const words =
    script
      .split(/\s+/)
      .filter(Boolean);

  const wordCount = words.length;

  if (wordCount < 250) {
    return {
      passed: false,
      wordCount,
      reason: "Too short"
    };
  }

  if (wordCount > 2500) {
    return {
      passed: false,
      wordCount,
      reason: "Too long"
    };
  }

  return {
    passed: true,
    wordCount,
    reason: "Quality check passed"
  };
}

/* =====================================================
   TEXT CHUNKING
===================================================== */

function splitText(text, maxChars = 1800) {
  const words = text.split(/\s+/);
  const chunks = [];

  let current = "";

  for (const word of words) {
    if (
      current.length +
      word.length +
      1 >
      maxChars
    ) {
      if (current.trim()) {
        chunks.push(current.trim());
      }

      current = word;
    } else {
      current +=
        (current ? " " : "") +
        word;
    }
  }

  if (current.trim()) {
    chunks.push(current.trim());
  }

  return chunks;
}

/* =====================================================
   TTS
===================================================== */

const TTS_MODELS = [
  "gemini-3.1-flash-tts-preview",
  "gemini-2.5-flash-preview-tts"
];

async function geminiTTS(model, text) {
  const url =
    "https://generativelanguage.googleapis.com/v1beta/interactions";

  const response =
    await fetchWithTimeout(
      url,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-goog-api-key": GEMINI_API_KEY
        },
        body: JSON.stringify({
          model,

          input: `
Speak the following YouTube narration naturally.

Voice:
- Professional
- Clear
- Engaging
- American English
- Medium pace

Speak ONLY the narration.

--- BEGIN ---
${text}
--- END ---
          `.trim(),

          response_format: {
            type: "audio"
          },

          generation_config: {
            speech_config: [
              {
                voice: "Kore"
              }
            ]
          }
        })
      },
      60000
    );

  const raw =
    await response.text();

  let data;

  try {
    data = JSON.parse(raw);
  } catch {
    data = null;
  }

  if (!response.ok) {
    const error = new Error(
      `TTS ${response.status}: ${
        data?.error?.message ||
        raw ||
        response.statusText
      }`
    );

    error.status = response.status;

    throw error;
  }

  const base64 =
    data?.output_audio?.data ||
    data?.outputAudio?.data ||
    data?.audio?.data;

  if (!base64) {
    throw new Error(
      "TTS_AUDIO_MISSING"
    );
  }

  return Buffer.from(
    base64,
    "base64"
  );
}

/* =====================================================
   PCM -> WAV
===================================================== */

function pcmToWav(
  pcm,
  sampleRate = 24000,
  channels = 1
) {
  const bits = 16;

  const byteRate =
    sampleRate *
    channels *
    bits / 8;

  const blockAlign =
    channels *
    bits / 8;

  const wav =
    Buffer.alloc(
      44 + pcm.length
    );

  wav.write("RIFF", 0);

  wav.writeUInt32LE(
    36 + pcm.length,
    4
  );

  wav.write("WAVE", 8);

  wav.write("fmt ", 12);

  wav.writeUInt32LE(
    16,
    16
  );

  wav.writeUInt16LE(
    1,
    20
  );

  wav.writeUInt16LE(
    channels,
    22
  );

  wav.writeUInt32LE(
    sampleRate,
    24
  );

  wav.writeUInt32LE(
    byteRate,
    28
  );

  wav.writeUInt16LE(
    blockAlign,
    32
  );

  wav.writeUInt16LE(
    bits,
    34
  );

  wav.write("data", 36);

  wav.writeUInt32LE(
    pcm.length,
    40
  );

  pcm.copy(
    wav,
    44
  );

  return wav;
}

/* =====================================================
   VOICE GENERATION
   CHUNKED + RESUMABLE
===================================================== */

async function generateVoice(job) {
  const chunks =
    splitText(
      job.script,
      1800
    );

  await updateJob(
    job.id,
    {
      tts_total_chunks:
        chunks.length
    }
  );

  let selectedModel =
    job.tts_model || null;

  let startChunk =
    Number(job.tts_chunk || 0);

  const audioParts
