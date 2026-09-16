import express from "express";
import pg from "pg";
import fs from "node:fs/promises";
import { readFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import crypto from "node:crypto";
import ffmpegPath from "ffmpeg-static";

const { Pool } = pg;
const app = express();

app.use(express.json({ limit: "2mb" }));

const PORT = process.env.PORT || 10000;
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const WEBHOOK_URL = process.env.WEBHOOK_URL;
const WEBHOOK_SECRET = process.env.TELEGRAM_WEBHOOK_SECRET;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL
    ? { rejectUnauthorized: false }
    : false,
  connectionTimeoutMillis: 15000,
});

const SCRIPT_MODELS = [
  "gemini-3.1-flash-lite",
  "gemini-3.5-flash-lite",
  "gemini-3.5-flash",
  "gemini-2.5-flash",
];
const TARGET_SCRIPT_WORDS = {
  min: 750,
  max: 900,
};

const sleep = (ms) =>
  new Promise((resolve) => setTimeout(resolve, ms));

async function db(query, params = []) {
  return pool.query(query, params);
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 30000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    return await fetch(url, {
      ...options,
      signal: controller.signal,
    });
  } catch (error) {
    if (error.name === "AbortError") {
      const timeoutError = new Error(`REQUEST_TIMEOUT_${timeoutMs}MS`);
      timeoutError.code = "TIMEOUT";
      throw timeoutError;
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

/* =========================
   DATABASE
========================= */

async function initDatabase() {
  console.log(
    "Starting PostgreSQL database initialization..."
  );

  await db(`
    CREATE TABLE IF NOT EXISTS jobs (
      id TEXT PRIMARY KEY,
      topic TEXT NOT NULL,
      chat_id BIGINT,
      status TEXT NOT NULL DEFAULT 'queued',
      progress INTEGER NOT NULL DEFAULT 0,
      stage TEXT DEFAULT 'queued',
      script TEXT,
      error TEXT,
      tts_total_chunks INTEGER DEFAULT 0,
      tts_completed_chunks INTEGER DEFAULT 0,
      tts_current_chunk INTEGER DEFAULT 0,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await db(`
    ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS chat_id BIGINT
  `);

  await db(`
    ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS script TEXT
  `);

  await db(`
    ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS error TEXT
  `);

  await db(`
    ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS progress INTEGER DEFAULT 0
  `);

  await db(`
    ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS stage TEXT DEFAULT 'queued'
  `);

  await db(`
    ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'queued'
  `);

  await db(`
    ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS tts_total_chunks INTEGER DEFAULT 0
  `);

  await db(`
    ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS tts_completed_chunks INTEGER DEFAULT 0
  `);

  await db(`
    ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS tts_current_chunk INTEGER DEFAULT 0
  `);

  await db(`
    ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS tts_chunk_size INTEGER DEFAULT 0
  `);

  await db(`
    CREATE TABLE IF NOT EXISTS job_media (
      job_id TEXT NOT NULL,
      kind TEXT NOT NULL,
      data BYTEA NOT NULL,
      mime_type TEXT NOT NULL,
      sha256 TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      PRIMARY KEY (job_id, kind)
    )
  `);

  await db(`
    ALTER TABLE job_media
    ADD COLUMN IF NOT EXISTS sha256 TEXT
  `);

  await db(`
    ALTER TABLE job_media
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()
  `);

  await db(`
    ALTER TABLE job_media
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()
  `);

  await db(`
    ALTER TABLE job_media
    ADD COLUMN IF NOT EXISTS data BYTEA
  `);

  await db(`
    ALTER TABLE job_media
    ADD COLUMN IF NOT EXISTS mime_type TEXT
  `);

  await db(`
    CREATE TABLE IF NOT EXISTS telegram_updates (
      update_id BIGINT PRIMARY KEY,
      received_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await db(`
    CREATE TABLE IF NOT EXISTS job_audio_chunks (
      job_id TEXT NOT NULL,
      chunk_index INTEGER NOT NULL,
      audio_data BYTEA,
      mime_type TEXT,
      status TEXT DEFAULT 'pending',
      model TEXT,
      error TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      PRIMARY KEY (job_id, chunk_index)
    )
  `);
  await db(`
    CREATE INDEX IF NOT EXISTS jobs_chat_updated_idx
    ON jobs(chat_id, updated_at DESC)
  `);

  await db(`
    CREATE INDEX IF NOT EXISTS jobs_queue_idx
    ON jobs(status, created_at)
  `);

  await db(`
    CREATE INDEX IF NOT EXISTS job_audio_chunks_status_idx
    ON job_audio_chunks(job_id, status, chunk_index)
  `);

  await db(`
    CREATE INDEX IF NOT EXISTS job_media_kind_idx
    ON job_media(job_id, kind)
  `);

  await db(`
    ALTER TABLE job_audio_chunks
    ADD COLUMN IF NOT EXISTS job_id TEXT
  `);

  await db(`
    ALTER TABLE job_audio_chunks
    ADD COLUMN IF NOT EXISTS chunk_index INTEGER
  `);

  await db(`
    ALTER TABLE job_audio_chunks
    ADD COLUMN IF NOT EXISTS audio_data BYTEA
  `);

  await db(`
    ALTER TABLE job_audio_chunks
    ADD COLUMN IF NOT EXISTS mime_type TEXT
  `);

  await db(`
    ALTER TABLE job_audio_chunks
    ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'pending'
  `);

  await db(`
    ALTER TABLE job_audio_chunks
    ADD COLUMN IF NOT EXISTS model TEXT
  `);

  await db(`
    ALTER TABLE job_audio_chunks
    ADD COLUMN IF NOT EXISTS error TEXT
  `);

  await db(`
    ALTER TABLE job_audio_chunks
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()
  `);

  await db(`
    ALTER TABLE job_audio_chunks
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()
  `);

  await db(`
    DO $$
    BEGIN
      IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'job_audio_chunks'
          AND column_name = 'audio'
      ) THEN
        UPDATE job_audio_chunks
        SET audio_data = audio
        WHERE audio_data IS NULL AND audio IS NOT NULL;
      END IF;
    END $$;
  `);

  await db(`
    UPDATE jobs
    SET
      tts_total_chunks =
        COALESCE(tts_total_chunks, 0),
      tts_completed_chunks =
        COALESCE(tts_completed_chunks, 0),
      tts_current_chunk =
        COALESCE(tts_current_chunk, 0),
      progress =
        COALESCE(progress, 0),
      stage =
        COALESCE(stage, 'queued'),
      status =
        COALESCE(status, 'queued'),
      updated_at =
        COALESCE(updated_at, NOW())
  `);

  await db(`
    UPDATE job_audio_chunks
    SET
      status =
        COALESCE(status, 'pending'),
      created_at =
        COALESCE(created_at, NOW()),
      updated_at =
        COALESCE(updated_at, NOW())
  `);

  const check = await db(`
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'job_audio_chunks'
    ORDER BY ordinal_position
  `);

  const columns =
    check.rows.map(
      (row) => row.column_name
    );

  const required = [
    "job_id",
    "chunk_index",
    "audio_data",
    "mime_type",
    "status",
    "model",
    "error",
    "created_at",
    "updated_at",
  ];

  const missing =
    required.filter(
      (column) =>
        !columns.includes(column)
    );

  if (missing.length > 0) {
    throw new Error(
      `DATABASE MIGRATION FAILED. Missing job_audio_chunks columns: ${missing.join(", ")}`
    );
  }

  console.log(
    "job_audio_chunks schema verified:",
    columns.join(", ")
  );

  console.log(
    "PostgreSQL database initialized and migrations checked"
  );
}

/* =========================
   TELEGRAM
========================= */

async function telegram(
  method,
  body = {}
) {
  if (!TELEGRAM_BOT_TOKEN) {
    throw new Error(
      "TELEGRAM_BOT_TOKEN missing"
    );
  }

  const response = await fetchWithTimeout(
    `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/${method}`,
    {
      method: "POST",
      headers: {
        "Content-Type":
          "application/json",
      },
      body: JSON.stringify(body),
    },
    15000
  );

  const raw =
    await response.text();

  let data;

  try {
    data = JSON.parse(raw);
  } catch {
    throw new Error(
      `Telegram invalid response: ${raw}`
    );
  }

  if (!data.ok) {
    throw new Error(
      `Telegram API error: ${raw}`
    );
  }

  return data.result;
}

async function sendMessage(
  chatId,
  text
) {
  if (!chatId) return;

  return telegram(
    "sendMessage",
    {
      chat_id: chatId,
      text,
      disable_web_page_preview: true,
    }
  );
}

async function sendDocument(
  chatId,
  buffer,
  filename,
  mimeType,
  caption = ""
) {
  if (!chatId) return;

  const form =
    new FormData();

  form.append(
    "chat_id",
    String(chatId)
  );

  form.append(
    "document",
    new Blob(
      [buffer],
      { type: mimeType }
    ),
    filename
  );

  if (caption) {
    form.append(
      "caption",
      caption
    );
  }

  const response =
    await fetchWithTimeout(
      `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendDocument`,
      {
        method: "POST",
        body: form,
      },
      30000
    );

  const data =
    await response.json();

  if (!data.ok) {
    throw new Error(
      `Telegram document error: ${JSON.stringify(data)}`
    );
  }

  return data.result;
}

async function sendVideo(
  chatId,
  buffer,
  filename,
  caption = ""
) {
  if (!chatId) return;

  const form = new FormData();
  form.append("chat_id", String(chatId));
  form.append(
    "video",
    new Blob([buffer], { type: "video/mp4" }),
    filename
  );

  if (caption) form.append("caption", caption);

  const response = await fetchWithTimeout(
    `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendVideo`,
    { method: "POST", body: form },
    60000
  );
  const data = await response.json();

  if (!data.ok) {
    throw new Error(`Telegram video error: ${JSON.stringify(data)}`);
  }

  return data.result;
}

/* =========================
   GEMINI REST
========================= */

async function geminiRequest(
  model,
  body,
  timeoutMs = 45000
) {
  if (!GEMINI_API_KEY) {
    throw new Error(
      "GEMINI_API_KEY missing"
    );
  }

  const controller =
    new AbortController();

  const timer =
    setTimeout(
      () =>
        controller.abort(),
      timeoutMs
    );

  try {
    const response =
      await fetch(
        `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent`,
        {
          method: "POST",
          headers: {
            "Content-Type":
              "application/json",
            "x-goog-api-key":
              GEMINI_API_KEY,
          },
          body: JSON.stringify(body),
          signal:
            controller.signal,
        }
      );

    const raw =
      await response.text();

    if (!response.ok) {
      const error =
        new Error(
          `Gemini ${response.status}: ${raw}`
        );

      error.status =
        response.status;
      error.retryAfterMs =
        Number(response.headers.get("retry-after")) * 1000 || 0;

      throw error;
    }

    return JSON.parse(raw);
  } catch (error) {
    if (
      error.name ===
      "AbortError"
    ) {
      const timeoutError =
        new Error(
          `REQUEST_TIMEOUT_${timeoutMs}MS`
        );

      timeoutError.code =
        "TIMEOUT";

      throw timeoutError;
    }

    throw error;
  } finally {
    clearTimeout(timer);
  }
}

async function retryGemini(
  model,
  body,
  timeoutMs = 45000,
  maxAttempts = 2
) {
  let lastError;

  for (
    let attempt = 1;
    attempt <= maxAttempts;
    attempt++
  ) {
    try {
      return await geminiRequest(
        model,
        body,
        timeoutMs
      );
    } catch (error) {
      lastError =
        error;

      const retryable =
        error.code === "TIMEOUT" ||
        error.status === 429 ||
        error.status === 500 ||
        error.status === 502 ||
        error.status === 503;

      if (
        !retryable ||
        attempt === maxAttempts
      ) {
        throw error;
      }

      const wait =
        3000 +
        Math.floor(
          Math.random() * 4000
        );

      console.log(
        `Gemini retry in ${wait}ms after: ${error.message}`
      );

      await sleep(wait);
    }
  }

  throw lastError;
}

/* =========================
   SCRIPT
========================= */

async function generateScript(
  topic
) {
  const threeFacts =
    /(?:\b3\b|three|तीन)\s+(?:interesting\s+)?facts|(?:3|three|तीन)\s+(?:रोचक\s+)?तथ्य/i.test(
      String(topic)
    );

  const lengthRule =
    threeFacts
      ? "- Even for 3 facts, target 750-850 words by explaining each fact with useful context."
      : "- Target 750-900 words so the narration runs approximately 5-6 minutes at a natural Hindi speaking pace.";

  const prompt = `
Create an original YouTube narration script about:

${topic}

Requirements:
- Completely original wording.
- Strong opening hook.
- Clear factual explanations.
- No copied article wording.
- No fake citations.
- Suitable for a faceless YouTube video.
${lengthRule}
- Natural spoken narration.
- No stage directions.
- Return only the narration, with no title, markdown, or commentary.
`;

  let lastError;

  for (
    const model of SCRIPT_MODELS
  ) {
    try {
      const result =
        await retryGemini(
          model,
          {
            contents: [
              {
                role: "user",
                parts: [
                  {
                    text: prompt,
                  },
                ],
              },
            ],
            generationConfig: {
              temperature: 0.8,
              maxOutputTokens: 5000,
            },
          },
          45000,
          2
        );

      const text =
        result
          ?.candidates?.[0]
          ?.content?.parts
          ?.map(
            (part) =>
              part.text || ""
          )
          .join("")
          .trim();

      if (text) {
        return {
          script: text,
          model,
        };
      }
    } catch (error) {
      console.log(
        `Script model failed: ${model}: ${error.message}`
      );

      lastError =
        error;
    }
  }

  throw (
    lastError ||
    new Error(
      "All script models failed"
    )
  );
}

/* =========================
   QUALITY CHECK
========================= */

function qualityCheck(
  script
) {
  const words =
    script
      .trim()
      .split(/\s+/)
      .filter(Boolean);

  if (
    words.length < TARGET_SCRIPT_WORDS.min ||
    words.length > TARGET_SCRIPT_WORDS.max
  ) {
    throw new Error(
      `Script must contain ${TARGET_SCRIPT_WORDS.min}-${TARGET_SCRIPT_WORDS.max} words; received ${words.length}`
    );
  }

  return words.length;
}

/* =========================
   TTS CHUNKING
========================= */

function splitIntoChunks(
  text,
  maxWords = 180
) {
  const clean =
    String(text || "")
      .replace(/\s+/g, " ")
      .trim();

  if (!clean) return [];

  const sentences =
    clean.match(
      /[^.!?]+[.!?]+|[^.!?]+$/g
    ) || [clean];

  const chunks = [];
  let current = [];

  for (
    const sentence of sentences
  ) {
    const words =
      sentence
        .trim()
        .split(/\s+/);

    if (
      words.length >
      maxWords
    ) {
      if (current.length) {
        chunks.push(
          current.join(" ")
        );

        current = [];
      }

      for (
        let i = 0;
        i < words.length;
        i += maxWords
      ) {
        chunks.push(
          words
            .slice(
              i,
              i + maxWords
            )
            .join(" ")
        );
      }

      continue;
    }

    if (
      current.length > 0 &&
      current.length +
        words.length >
        maxWords
    ) {
      chunks.push(
        current.join(" ")
      );

      current = [];
    }

    current.push(
      ...words
    );
  }

  if (current.length) {
    chunks.push(
      current.join(" ")
    );
  }

  return chunks;
}

/* =========================
   TTS
   ELEVENLABS PRIMARY
   GOOGLE CLOUD FALLBACK
========================= */

const ELEVENLABS_API_KEY =
  process.env.ELEVENLABS_API_KEY;

const ELEVENLABS_MODEL_ID =
  process.env.ELEVENLABS_MODEL_ID ||
  "eleven_flash_v2_5";

const ELEVENLABS_HINDI_VOICE_ID =
  process.env.ELEVENLABS_HINDI_VOICE_ID;

const ELEVENLABS_ENGLISH_VOICE_ID =
  process.env.ELEVENLABS_ENGLISH_VOICE_ID;

const GOOGLE_TTS_HI_VOICE_NAME =
  process.env.GOOGLE_TTS_HI_VOICE_NAME ||
  "hi-IN-Neural2-A";

const GOOGLE_TTS_EN_VOICE_NAME =
  process.env.GOOGLE_TTS_EN_VOICE_NAME ||
  "en-US-Neural2-J";

const GOOGLE_TTS_SERVICE_ACCOUNT_JSON =
  process.env.GOOGLE_TTS_SERVICE_ACCOUNT_JSON ||
  "";

const GOOGLE_TTS_SERVICE_ACCOUNT_FILE =
  process.env.GOOGLE_TTS_SERVICE_ACCOUNT_FILE ||
  "/etc/secrets/google-tts.json";

function isHindiText(text) {
  return /[\u0900-\u097F]/.test(
    String(text || "")
  );
}

function base64Url(value) {
  return Buffer
    .from(value)
    .toString("base64")
    .replace(/=/g, "")
    .replace(/\+/g, "-")
    .replace(/\//g, "_");
}

function loadGoogleServiceAccount() {
  if (
    GOOGLE_TTS_SERVICE_ACCOUNT_JSON.trim()
  ) {
    try {
      return JSON.parse(
        GOOGLE_TTS_SERVICE_ACCOUNT_JSON
      );
    } catch (error) {
      throw new Error(
        `Invalid GOOGLE_TTS_SERVICE_ACCOUNT_JSON: ${error.message}`
      );
    }
  }

  try {
    const raw =
      readFileSync(
        GOOGLE_TTS_SERVICE_ACCOUNT_FILE,
        "utf8"
      );

    return JSON.parse(raw);
  } catch {
    return null;
  }
}

async function googleAccessToken() {
  const credentials =
    loadGoogleServiceAccount();

  if (
    !credentials?.client_email ||
    !credentials?.private_key
  ) {
    const error = new Error("Google Cloud TTS credentials missing");
    error.code = "CONFIGURATION";
    throw error;
  }

  const now =
    Math.floor(
      Date.now() / 1000
    );

  const header =
    base64Url(
      JSON.stringify({
        alg: "RS256",
        typ: "JWT",
      })
    );

  const claim =
    base64Url(
      JSON.stringify({
        iss:
          credentials.client_email,
        scope:
          "https://www.googleapis.com/auth/cloud-platform",
        aud:
          "https://oauth2.googleapis.com/token",
        iat: now,
        exp: now + 3600,
      })
    );

  const unsigned =
    `${header}.${claim}`;

  const signer =
    crypto.createSign(
      "RSA-SHA256"
    );

  signer.update(unsigned);
  signer.end();

  const signature =
    signer.sign(
      credentials.private_key
    );

  const assertion =
    `${unsigned}.${base64Url(signature)}`;

  const response = await fetchWithTimeout(
    "https://oauth2.googleapis.com/token",
    {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: new URLSearchParams({
        grant_type:
          "urn:ietf:params:oauth:grant-type:jwt-bearer",
        assertion,
      }),
    },
    15000
  );

  const raw =
    await response.text();

  if (!response.ok) {
    throw new Error(
      `Google OAuth ${response.status}: ${raw}`
    );
  }

  const data =
    JSON.parse(raw);

  if (!data.access_token) {
    throw new Error(
      "Google OAuth returned no access token"
    );
  }

  return data.access_token;
}

async function elevenLabsTTS(
  text
) {
  if (!ELEVENLABS_API_KEY) {
    const error = new Error("ELEVENLABS_API_KEY missing");
    error.code = "CONFIGURATION";
    throw error;
  }

  const clean =
    String(text || "")
      .trim();

  if (!clean) {
    throw new Error(
      "Empty ElevenLabs TTS text"
    );
  }

  const hindi =
    isHindiText(clean);

  const voiceId =
    hindi
      ? ELEVENLABS_HINDI_VOICE_ID
      : ELEVENLABS_ENGLISH_VOICE_ID;

  if (!voiceId) {
    const error = new Error(
      `ElevenLabs ${hindi ? "Hindi" : "English"} voice ID missing`
    );
    error.code = "CONFIGURATION";
    throw error;
  }

  try {
    const response = await fetchWithTimeout(
      `https://api.elevenlabs.io/v1/text-to-speech/${encodeURIComponent(voiceId)}`,
      {
        method: "POST",
        headers: {
          "xi-api-key": ELEVENLABS_API_KEY,
          "Content-Type": "application/json",
          Accept: "audio/pcm",
        },
        body: JSON.stringify({
          text: clean,
          model_id: ELEVENLABS_MODEL_ID,
          output_format: "pcm_16000",
        }),
      },
      30000
    );

    const rawBuffer =
      Buffer.from(
        await response.arrayBuffer()
      );

    if (!response.ok) {
      const detail =
        rawBuffer
          .toString("utf8")
          .slice(0, 3000);

      const error =
        new Error(
          `ElevenLabs ${response.status}: ${detail}`
        );

      error.status =
        response.status;

      throw error;
    }

    if (!rawBuffer.length) {
      throw new Error(
        "ElevenLabs returned empty audio"
      );
    }

    return {
      buffer: rawBuffer,
      mimeType: "audio/pcm",
      sampleRate: 16000,
      channels: 1,
      bits: 16,
      model:
        `elevenlabs:${ELEVENLABS_MODEL_ID}`,
      provider:
        "elevenlabs",
    };
  } catch (error) {
    throw error;
  }
}

async function googleCloudTTS(
  text
) {
  const clean =
    String(text || "")
      .trim();

  if (!clean) {
    throw new Error(
      "Empty Google Cloud TTS text"
    );
  }

  const token =
    await googleAccessToken();

  const hindi =
    isHindiText(clean);

  const languageCode =
    hindi
      ? "hi-IN"
      : "en-US";

  const name =
    hindi
      ? GOOGLE_TTS_HI_VOICE_NAME
      : GOOGLE_TTS_EN_VOICE_NAME;

  const response = await fetchWithTimeout(
    "https://texttospeech.googleapis.com/v1/text:synthesize",
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        input: { text: clean },
        voice: { languageCode, name },
        audioConfig: {
          audioEncoding: "LINEAR16",
          speakingRate: 1.0,
        },
      }),
    },
    30000
  );

  const raw =
    await response.text();

  if (!response.ok) {
    const error =
      new Error(
        `Google Cloud TTS ${response.status}: ${raw}`
      );

    error.status =
      response.status;
    error.retryAfterMs =
      Number(response.headers.get("retry-after")) * 1000 || 0;

    throw error;
  }

  const data =
    JSON.parse(raw);

  if (!data.audioContent) {
    throw new Error(
      "Google Cloud TTS returned no audioContent"
    );
  }

  const buffer =
    Buffer.from(
      data.audioContent,
      "base64"
    );

  if (!buffer.length) {
    throw new Error(
      "Google Cloud TTS returned empty audio"
    );
  }

  return {
    buffer,
    mimeType:
      "audio/pcm",
    sampleRate:
      24000,
    channels: 1,
    bits: 16,
    model:
      `google-cloud:${name}`,
    provider:
      "google-cloud",
  };
}

function isContentBlockedError(
  error
) {
  const text =
    String(
      error?.message || ""
    ).toLowerCase();

  return (
    text.includes(
      "content_blocked"
    ) ||
    text.includes(
      "request blocked"
    ) ||
    text.includes(
      "policy reason"
    ) ||
    text.includes(
      "content moderation"
    )
  );
}

async function rewriteForSafeTTS(
  text
) {
  const prompt = `
Rewrite the following narration for a general-audience educational YouTube voice-over.
Keep the factual meaning and topic.
Use calm, neutral, non-graphic wording.
Remove or replace sensitive or potentially policy-triggering details.
Do not add new facts.
Return ONLY the rewritten narration.

TEXT:
${text}
`;

  let lastError;

  for (
    const model of
      SCRIPT_MODELS.slice(0, 2)
  ) {
    try {
      const result =
        await retryGemini(
          model,
          {
            contents: [
              {
                role: "user",
                parts: [
                  {
                    text: prompt,
                  },
                ],
              },
            ],
            generationConfig: {
              temperature: 0.3,
              maxOutputTokens: 1200,
            },
          },
          30000,
          2
        );

      const rewritten =
        result
          ?.candidates?.[0]
          ?.content?.parts
          ?.map(
            (part) =>
              part.text || ""
          )
          .join("")
          .trim();

      if (rewritten) {
        return rewritten;
      }
    } catch (error) {
      lastError =
        error;

      console.log(
        `Safe TTS rewrite failed: ${error.message}`
      );
    }
  }

  throw (
    lastError ||
    new Error(
      "Safe TTS rewrite failed"
    )
  );
}

async function generateTTSWithRetry(
  text,
  chunkIndex = 0
) {
  let currentText = String(text || "").trim();

  if (!currentText) {
    throw new Error("Empty TTS chunk");
  }

  const runProvider = async (provider, model, operation) => {
    let lastError;

    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        const result = await operation();
        const normalized = await normalizeAudioResult(result);
        console.log(JSON.stringify({
          event: "tts_success",
          provider,
          model,
          chunk: chunkIndex,
          attempt,
        }));
        return normalized;
      } catch (error) {
        lastError = error;
        const status = Number(error.status) || null;
        const category = isContentBlockedError(error)
          ? "content_blocked"
          : error.code === "TIMEOUT"
            ? "timeout"
            : status === 429
              ? "rate_limit"
              : status && status >= 500
                ? "provider_server"
                : status && status >= 400
                  ? "provider_client"
                  : "network";

        console.log(JSON.stringify({
          event: "tts_failure",
          provider,
          model,
          chunk: chunkIndex,
          attempt,
          status,
          category,
        }));

        const retryable =
          error.code !== "CONFIGURATION" &&
          !isContentBlockedError(error) &&
          (error.code === "TIMEOUT" ||
          error.name === "TypeError" ||
          status === 408 ||
          status === 425 ||
          status === 429 ||
          status >= 500);

        if (!retryable || attempt === 3) break;

        const wait = Math.min(
          8000,
          Number(error.retryAfterMs) || 750 * (2 ** (attempt - 1))
        );
        await sleep(wait + Math.floor(Math.random() * 500));
      }
    }

    throw lastError;
  };

  let elevenError;

  try {
    return await runProvider(
      "elevenlabs",
      ELEVENLABS_MODEL_ID,
      () => elevenLabsTTS(currentText)
    );
  } catch (error) {
    elevenError = error;
  }

  if (isContentBlockedError(elevenError)) {
    try {
      currentText = await rewriteForSafeTTS(currentText);
      return await runProvider(
        "elevenlabs",
        ELEVENLABS_MODEL_ID,
        () => elevenLabsTTS(currentText)
      );
    } catch (error) {
      elevenError = error;
    }
  }

  let googleError;

  try {
    return await runProvider(
      "google-cloud",
      isHindiText(currentText) ? GOOGLE_TTS_HI_VOICE_NAME : GOOGLE_TTS_EN_VOICE_NAME,
      () => googleCloudTTS(currentText)
    );
  } catch (error) {
    googleError = error;
  }

  if (isContentBlockedError(googleError) && !isContentBlockedError(elevenError)) {
    currentText = await rewriteForSafeTTS(currentText);
    return runProvider(
      "google-cloud",
      isHindiText(currentText) ? GOOGLE_TTS_HI_VOICE_NAME : GOOGLE_TTS_EN_VOICE_NAME,
      () => googleCloudTTS(currentText)
    );
  }

  throw new Error(
    `TTS providers exhausted: ElevenLabs ${elevenError?.status || elevenError?.code || "failed"}; Google ${googleError?.status || googleError?.code || "failed"}`
  );
}

async function normalizeAudioResult(result) {
  if (!result?.buffer?.length) {
    throw new Error("TTS returned empty audio");
  }

  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "tts-"));
  const inputPath = path.join(dir, "input");
  const outputPath = path.join(dir, "output.wav");

  try {
    await fs.writeFile(inputPath, result.buffer);

    const inputArgs = result.mimeType === "audio/pcm"
      ? [
          "-f", `s${result.bits || 16}le`,
          "-ar", String(result.sampleRate || 24000),
          "-ac", String(result.channels || 1),
        ]
      : [];

    await runFfmpeg([
      "-y",
      ...inputArgs,
      "-i", inputPath,
      "-ar", "24000",
      "-ac", "1",
      "-c:a", "pcm_s16le",
      outputPath,
    ], 120000);

    return {
      buffer: await fs.readFile(outputPath),
      mimeType: "audio/wav",
      model: result.model,
      provider: result.provider,
    };
  } finally {
    await fs.rm(dir, { recursive: true, force: true }).catch(() => {});
  }
}

async function generateChunkAudio(text, chunkIndex) {
  return generateTTSWithRetry(text, chunkIndex);
}

/* =========================
   WAV HELPERS
========================= */

function isWav(
  buffer
) {
  return (
    buffer.length >= 12 &&
    buffer.toString(
      "ascii",
      0,
      4
    ) === "RIFF" &&
    buffer.toString(
      "ascii",
      8,
      12
    ) === "WAVE"
  );
}

function pcmToWav(
  pcm,
  sampleRate = 24000,
  channels = 1,
  bits = 16
) {
  const blockAlign =
    (channels * bits) /
    8;

  const byteRate =
    sampleRate *
    blockAlign;

  const header =
    Buffer.alloc(44);

  header.write(
    "RIFF",
    0
  );

  header.writeUInt32LE(
    36 + pcm.length,
    4
  );

  header.write(
    "WAVE",
    8
  );

  header.write(
    "fmt ",
    12
  );

  header.writeUInt32LE(
    16,
    16
  );

  header.writeUInt16LE(
    1,
    20
  );

  header.writeUInt16LE(
    channels,
    22
  );

  header.writeUInt32LE(
    sampleRate,
    24
  );

  header.writeUInt32LE(
    byteRate,
    28
  );

  header.writeUInt16LE(
    blockAlign,
    32
  );

  header.writeUInt16LE(
    bits,
    34
  );

  header.write(
    "data",
    36
  );

  header.writeUInt32LE(
    pcm.length,
    40
  );

  return Buffer.concat([
    header,
    pcm,
  ]);
}

function getWavPcm(
  wav
) {
  if (!isWav(wav)) {
    return wav;
  }

  let offset = 12;

  while (
    offset + 8 <=
    wav.length
  ) {
    const id =
      wav.toString(
        "ascii",
        offset,
        offset + 4
      );

    const size =
      wav.readUInt32LE(
        offset + 4
      );

    if (id === "data") {
      const end =
        Math.min(
          offset + 8 + size,
          wav.length
        );

      return wav.subarray(
        offset + 8,
        end
      );
    }

    offset +=
      8 + size;
  }

  throw new Error(
    "WAV data chunk not found"
  );
}

function getWavFormat(
  wav
) {
  if (
    !isWav(wav) ||
    wav.length < 36
  ) {
    return {
      sampleRate: 24000,
      channels: 1,
      bits: 16,
    };
  }

  return {
    sampleRate:
      wav.readUInt32LE(
        24
      ),
    channels:
      wav.readUInt16LE(
        22
      ),
    bits:
      wav.readUInt16LE(
        34
      ),
  };
}

function concatWavBuffers(
  buffers
) {
  if (!buffers.length) {
    throw new Error(
      "No audio buffers to concatenate"
    );
  }

  const first =
    isWav(buffers[0])
      ? buffers[0]
      : pcmToWav(
          buffers[0]
        );

  const format =
    getWavFormat(first);

  const pcm =
    Buffer.concat(
      buffers.map(
        getWavPcm
      )
    );

  return pcmToWav(
    pcm,
    format.sampleRate,
    format.channels,
    format.bits
  );
}
/* =========================
   JOB HELPERS
========================= */

async function getJob(id) {
  const result =
    await db(
      `SELECT * FROM jobs WHERE id = $1`,
      [id]
    );

  return result.rows[0] || null;
}

async function updateJob(
  id,
  fields
) {
  const entries =
    Object.entries(fields);

  if (!entries.length) return;

  const allowed =
    new Set([
      "topic",
      "chat_id",
      "status",
      "progress",
      "stage",
      "script",
      "error",
      "tts_total_chunks",
      "tts_completed_chunks",
      "tts_current_chunk",
      "tts_chunk_size",
    ]);

  const values = [];
  const sets = [];

  for (
    const [key, value]
    of entries
  ) {
    if (!allowed.has(key)) {
      throw new Error(
        `Invalid job field: ${key}`
      );
    }

    values.push(value);

    sets.push(
      `${key} = $${values.length}`
    );
  }

  values.push(id);

  await db(
    `
    UPDATE jobs
    SET ${sets.join(", ")},
        updated_at = NOW()
    WHERE id = $${values.length}
    `,
    values
  );
}

/* =========================
   TTS JOB PROCESSOR
========================= */

async function processTTS(
  job,
  chatId
) {
  const chunkSize =
    job.tts_total_chunks > 0 &&
    !job.tts_chunk_size
      ? 80
      : 180;

  const chunks =
    splitIntoChunks(
      job.script,
      chunkSize
    );

  if (!chunks.length) {
    throw new Error(
      "No script text available for TTS"
    );
  }

  const targetChat =
    chatId || job.chat_id;

  const existingRows =
    await db(
      `SELECT chunk_index
       FROM job_audio_chunks
       WHERE job_id=$1
         AND status='completed'
         AND audio_data IS NOT NULL`,
      [job.id]
    );

  const existingSet =
    new Set(
      existingRows.rows.map(
        (row) =>
          Number(
            row.chunk_index
          )
      )
    );

  let completed =
    existingSet.size;

  await updateJob(
    job.id,
    {
      chat_id: targetChat,
      tts_total_chunks:
        chunks.length,
      tts_chunk_size:
        chunkSize,
      tts_completed_chunks:
        completed,
      stage: "tts",
      progress:
        Math.min(
          85,
          55 +
            Math.floor(
              (completed /
                chunks.length) *
                30
            )
        ),
      status: "tts",
      error: null,
    }
  );

  let next = 0;
  let fatal = null;

  const workers =
    Array.from(
      {
        length:
          Math.min(
            3,
            chunks.length
          ),
      },
      async () => {
        while (true) {
          if (fatal) return;

          const i = next++;

          if (
            i >=
            chunks.length
          ) {
            return;
          }

          if (
            existingSet.has(i)
          ) {
            continue;
          }

          try {
            await updateJob(
              job.id,
              {
                tts_current_chunk:
                  i,
              }
            );

            const result =
              await generateChunkAudio(
                chunks[i],
                i
              );

            await db(
              `INSERT INTO job_audio_chunks
                (
                  job_id,
                  chunk_index,
                  audio_data,
                  mime_type,
                  status,
                  model,
                  error
                )
               VALUES(
                 $1,
                 $2,
                 $3,
                 $4,
                 'completed',
                 $5,
                 NULL
               )
               ON CONFLICT(
                 job_id,
                 chunk_index
               )
               DO UPDATE SET
                 audio_data =
                   EXCLUDED.audio_data,
                 mime_type =
                   EXCLUDED.mime_type,
                 status =
                   'completed',
                 model =
                   EXCLUDED.model,
                 error =
                   NULL,
                 updated_at =
                   NOW()`,
              [
                job.id,
                i,
                result.buffer,
                result.mimeType,
                result.model,
              ]
            );

            existingSet.add(i);
            completed += 1;

            const progress =
              Math.min(
                85,
                55 +
                  Math.floor(
                    (completed /
                      chunks.length) *
                      30
                  )
              );

            await updateJob(
              job.id,
              {
                tts_completed_chunks:
                  completed,
                progress,
                error: null,
              }
            );

            if (
              completed === 1 ||
              completed ===
                chunks.length ||
              completed % 3 === 0
            ) {
              await sendMessage(
                targetChat,
                `🎙️ TTS ${completed}/${chunks.length}\nProgress: ${progress}%`
              );
            }
          } catch (error) {
            fatal = {
              index: i,
              error,
            };

            await db(
              `INSERT INTO job_audio_chunks
                (
                  job_id,
                  chunk_index,
                  status,
                  error
                )
               VALUES(
                 $1,
                 $2,
                 'failed',
                 $3
               )
               ON CONFLICT(
                 job_id,
                 chunk_index
               )
               DO UPDATE SET
                 status='failed',
                 error=EXCLUDED.error,
                 updated_at=NOW()`,
              [
                job.id,
                i,
                error.message,
              ]
            );

            return;
          }
        }
      }
    );

  await Promise.all(
    workers
  );

  if (fatal) {
    await updateJob(
      job.id,
      {
        status: "paused",
        stage: "tts",
        error:
          fatal.error.message,
      }
    );

    await sendMessage(
      targetChat,
      `⏸️ JOB PAUSED SAFELY

TTS chunk ${fatal.index + 1}/${chunks.length} failed.

Completed chunks are saved.

Resume with:
/resume ${job.id}`
    );

    return null;
  }

  const rows =
    await db(
      `SELECT audio_data
       FROM job_audio_chunks
       WHERE job_id=$1
         AND status='completed'
       ORDER BY chunk_index`,
      [job.id]
    );

  if (
    rows.rows.length !==
    chunks.length
  ) {
    throw new Error(
      `Not all TTS chunks completed (${rows.rows.length}/${chunks.length})`
    );
  }

  const audio =
    concatWavBuffers(
      rows.rows.map(
        (row) =>
          row.audio_data
      )
    );

  await updateJob(
    job.id,
    {
      tts_completed_chunks:
        chunks.length,
      progress: 85,
      stage: "tts_complete",
      error: null,
    }
  );

  return {
    audio,
    chunks,
  };
}

/* =========================
   VIDEO / CAPTIONS
========================= */

function wavDurationSeconds(
  wav
) {
  const f =
    getWavFormat(wav);

  const pcm =
    getWavPcm(wav);

  return (
    pcm.length /
    (
      f.sampleRate *
      f.channels *
      (f.bits / 8)
    )
  );
}

function srtTime(
  seconds
) {
  const ms =
    Math.max(
      0,
      Math.round(
        seconds * 1000
      )
    );

  const h =
    Math.floor(
      ms / 3600000
    );

  const m =
    Math.floor(
      (ms % 3600000) /
        60000
    );

  const s =
    Math.floor(
      (ms % 60000) /
        1000
    );

  const x =
    ms % 1000;

  return (
    `${String(h).padStart(2, "0")}:` +
    `${String(m).padStart(2, "0")}:` +
    `${String(s).padStart(2, "0")},` +
    `${String(x).padStart(3, "0")}`
  );
}

function makeSrt(
  chunks,
  audioBuffers,
  title = ""
) {
  let t = 0;
  let n = 1;
  const out = [];

  const cleanTitle = String(title || "")
    .replace(/\s+/g, " ")
    .trim();

  if (cleanTitle) {
    out.push(
      `${n++}\n` +
      `${srtTime(0)} --> ${srtTime(Math.min(4, wavDurationSeconds(audioBuffers[0])))}\n` +
      `${cleanTitle}\n`
    );
  }

  for (
    let i = 0;
    i < chunks.length;
    i++
  ) {
    const words =
      chunks[i]
        .split(/\s+/);

    const dur =
      wavDurationSeconds(
        audioBuffers[i]
      );

    for (
      let p = 0;
      p < words.length;
      p += 12
    ) {
      const line =
        words
          .slice(
            p,
            p + 12
          )
          .join(" ");

      const a =
        t +
        dur *
          (p /
            words.length);

      const b =
        t +
        dur *
          (
            Math.min(
              p + 12,
              words.length
            ) /
            words.length
          );

      out.push(
        `${n++}\n` +
        `${srtTime(a)} --> ${srtTime(b)}\n` +
        `${line}\n`
      );
    }

    t += dur;
  }

  return out.join("\n");
}

function escapeFilterPath(filePath) {
  return filePath
    .replace(/\\/g, "\\\\")
    .replace(/:/g, "\\:")
    .replace(/'/g, "\\'");
}

async function buildSceneVideo({
  dir,
  chunks,
  audioPath,
  videoPath,
  durations,
  captionsPath,
}) {
  const colors = [
    "0x102a43",
    "0x1f3a5f",
    "0x164e63",
    "0x365314",
    "0x713f12",
    "0x4c1d95",
  ];
  const sceneInputs = [];

  for (let i = 0; i < chunks.length; i += 1) {
    sceneInputs.push(
      "-f",
      "lavfi",
      "-t",
      String(Math.max(0.2, durations[i])),
      "-i",
      `color=c=${colors[i % colors.length]}:s=1080x1920:r=30`
    );
  }

  const transition = 0.45;
  const filters = [];

  for (let i = 0; i < chunks.length; i += 1) {
    filters.push(
      `[${i}:v]setpts=PTS-STARTPTS,fps=30,drawbox=x=55:y=180:w=970:h=1560:color=white@0.08:t=4[v${i}]`
    );
  }

  let videoLabel = "v0";
  let accumulated = durations[0];

  for (let i = 1; i < chunks.length; i += 1) {
    const nextLabel = `vx${i}`;
    const offset = Math.max(0, accumulated - transition);
    filters.push(
      `[${videoLabel}][v${i}]xfade=transition=fade:duration=${transition}:offset=${offset}[${nextLabel}]`
    );
    videoLabel = nextLabel;
    accumulated += durations[i] - transition;
  }

  const audioIndex = chunks.length;
  const musicPath = process.env.BACKGROUND_MUSIC_PATH;
  let musicEnabled = false;

  if (musicPath) {
    try {
      await fs.access(musicPath);
      musicEnabled = true;
    } catch {
      console.warn("Configured BACKGROUND_MUSIC_PATH is unavailable; continuing without music");
    }
  }

  const args = ["-y", ...sceneInputs, "-i", audioPath];

  if (musicEnabled) {
    args.push("-stream_loop", "-1", "-i", musicPath);
  }

  const audioFilters = musicEnabled
    ? `[${audioIndex}:a]aresample=24000,volume=0.10[music];[${audioIndex}:a][music]amix=inputs=2:duration=first:dropout_transition=2[aout]`
    : `[${audioIndex}:a]aresample=24000,volume=1.0[aout]`;

  filters.push(
    `[${videoLabel}]subtitles=${escapeFilterPath(captionsPath)}:force_style='FontName=DejaVu Sans,FontSize=18,Alignment=2,MarginV=120,Outline=2,Shadow=1',format=yuv420p[vout]`,
    audioFilters
  );

  args.push(
    "-filter_complex",
    filters.join(";"),
    "-map",
    "[vout]",
    "-map",
    "[aout]",
    "-c:v",
    "libx264",
    "-preset",
    "fast",
    "-crf",
    "25",
    "-r",
    "30",
    "-pix_fmt",
    "yuv420p",
    "-c:a",
    "aac",
    "-b:a",
    "128k",
    "-ar",
    "24000",
    "-ac",
    "1",
    "-movflags",
    "+faststart",
    "-shortest",
    videoPath
  );

  await runFfmpeg(args, 600000);
}

function runFfmpeg(
  args,
  timeoutMs = 300000
) {
  if (!ffmpegPath) {
    throw new Error(
      "ffmpeg-static binary unavailable"
    );
  }

  return new Promise(
    (resolve, reject) => {
      const c =
        spawn(
          ffmpegPath,
          args,
          {
            stdio: [
              "ignore",
              "ignore",
              "pipe",
            ],
          }
        );

      let err = "";

      const timer =
        setTimeout(
          () => {
            c.kill(
              "SIGKILL"
            );

            reject(
              new Error(
                "FFMPEG_TIMEOUT"
              )
            );
          },
          timeoutMs
        );

      c.stderr.on(
        "data",
        (d) => {
          err +=
            d.toString();

          if (
            err.length >
            10000
          ) {
            err =
              err.slice(
                -10000
              );
          }
        }
      );

      c.on(
        "error",
        (e) => {
          clearTimeout(
            timer
          );

          reject(e);
        }
      );

      c.on(
        "close",
        (code) => {
          clearTimeout(
            timer
          );

          if (
            code === 0
          ) {
            resolve();
          } else {
            reject(
              new Error(
                `FFmpeg failed (${code}): ${err.slice(-2500)}`
              )
            );
          }
        }
      );
    }
  );
}

async function getPersistedMedia(
  jobId
) {
  const r =
    await db(
      `SELECT kind,data,mime_type
       FROM job_media
       WHERE job_id=$1
         AND kind IN(
           'video',
           'thumbnail',
           'captions'
         )`,
      [jobId]
    );

  const m = {};

  for (
    const x of r.rows
  ) {
    m[x.kind] = {
      data:
        Buffer.from(
          x.data
        ),
      mimeType:
        x.mime_type,
    };
  }

  return m;
}

async function saveMedia(
  jobId,
  kind,
  data,
  mimeType
) {
  const b =
    Buffer.from(data);

  const sha =
    crypto
      .createHash(
        "sha256"
      )
      .update(b)
      .digest("hex");

  await db(
    `INSERT INTO job_media
      (
        job_id,
        kind,
        data,
        mime_type,
        sha256
      )
     VALUES(
       $1,
       $2,
       $3,
       $4,
       $5
     )
     ON CONFLICT(
       job_id,
       kind
     )
     DO UPDATE SET
       data =
         EXCLUDED.data,
       mime_type =
         EXCLUDED.mime_type,
       sha256 =
         EXCLUDED.sha256,
       updated_at =
         NOW()`,
    [
      jobId,
      kind,
      b,
      mimeType,
      sha,
    ]
  );
}

async function buildVideoPackage(
  job,
  audio,
  chunks
) {
  const existing =
    await getPersistedMedia(
      job.id
    );

  if (
    existing.video &&
    existing.thumbnail &&
    existing.captions
  ) {
    return existing;
  }

  const dir =
    await fs.mkdtemp(
      path.join(
        os.tmpdir(),
        `yt-${job.id}-`
      )
    );

  const audioPath =
    path.join(
      dir,
      "audio.wav"
    );

  const srtPath =
    path.join(
      dir,
      "captions.srt"
    );

  const videoPath =
    path.join(
      dir,
      "video.mp4"
    );

  const thumbPath =
    path.join(
      dir,
      "thumbnail.jpg"
    );

  try {
    const rows =
      await db(
        `SELECT audio_data
         FROM job_audio_chunks
         WHERE job_id=$1
           AND status='completed'
         ORDER BY chunk_index`,
        [job.id]
      );

    const buffers =
      rows.rows.map(
        (r) =>
          Buffer.from(
            r.audio_data
          )
      );

    const srt = makeSrt(chunks, buffers, job.topic);

    await fs.writeFile(
      audioPath,
      audio
    );

    await fs.writeFile(
      srtPath,
      srt
    );

    const dur =
      Math.max(
        1,
        wavDurationSeconds(
          audio
        )
      );

    const durations = buffers.map(wavDurationSeconds);

    await buildSceneVideo({
      dir,
      chunks,
      audioPath,
      videoPath,
      durations,
      captionsPath: srtPath,
    });

    const video =
      await fs.readFile(
        videoPath
      );

    if (
      video.length >
      49 * 1024 * 1024
    ) {
      throw new Error(
        `Final video is too large for Telegram: ${(video.length / 1048576).toFixed(1)}MB`
      );
    }

    await runFfmpeg([
      "-y",
      "-i",
      videoPath,
      "-frames:v",
      "1",
      "-q:v",
      "5",
      thumbPath,
    ]);

    const thumb =
      await fs.readFile(
        thumbPath
      );

    const cap =
      Buffer.from(
        srt,
        "utf8"
      );

    await saveMedia(
      job.id,
      "video",
      video,
      "video/mp4"
    );

    await saveMedia(
      job.id,
      "thumbnail",
      thumb,
      "image/jpeg"
    );

    await saveMedia(
      job.id,
      "captions",
      cap,
      "application/x-subrip"
    );

    return {
      video: {
        data: video,
        mimeType:
          "video/mp4",
      },
      thumbnail: {
        data: thumb,
        mimeType:
          "image/jpeg",
      },
      captions: {
        data: cap,
        mimeType:
          "application/x-subrip",
      },
    };
  } finally {
    await fs.rm(
      dir,
      {
        recursive: true,
        force: true,
      }
    ).catch(() => {});
  }
}

/* =========================
   JOB QUEUE
========================= */

const jobQueue =
  new Map();

const MAX_ACTIVE_JOBS = Math.max(
  1,
  Math.min(3, Number(process.env.JOB_CONCURRENCY) || 2)
);
let activeJobs = 0;
let queuePumpRunning = false;

function processJob(
  id,
  chatId = null
) {
  if (
    !jobQueue.has(id)
  ) {
    jobQueue.set(
      id,
      {
        id,
        chatId,
      }
    );
  }

  pumpJobQueue()
    .catch(
      (error) => {
        console.error(
          "Job queue pump failed:",
          error
        );
      }
    );

  return Promise.resolve(
    true
  );
}

function userFacingError(error) {
  const message = String(error?.message || "Unknown processing error");

  if (message.includes("missing")) {
    return "A required provider configuration is missing. Check the server configuration.";
  }

  if (Number(error?.status) === 401 || Number(error?.status) === 403) {
    return "A configured provider rejected authentication or permissions. Check the server configuration.";
  }

  if (message.includes("TIMEOUT") || error?.code === "TIMEOUT") {
    return "A provider timed out. Completed work was saved; you can resume this job.";
  }

  if (message.includes("FFmpeg")) {
    return "Video rendering failed. Completed work was saved; you can resume this job.";
  }

  return "The job paused safely. Completed work was saved; you can resume it.";
}

async function pumpJobQueue() {
  if (
    queuePumpRunning
  ) {
    return;
  }

  queuePumpRunning =
    true;

  try {
    while (jobQueue.size > 0 && activeJobs < MAX_ACTIVE_JOBS) {
      const item =
        jobQueue
          .values()
          .next()
          .value;

      jobQueue.delete(
        item.id
      );

      activeJobs += 1;
      runJobOnce(item.id, item.chatId)
        .catch((error) => {
          console.error(`Unhandled worker error for ${item.id}:`, error);
        })
        .finally(() => {
          activeJobs -= 1;
          pumpJobQueue().catch((error) => {
            console.error("Job queue restart failed:", error);
          });
        });
    }
  } finally {
    queuePumpRunning =
      false;

  }
}

/* =========================
   FULL JOB
========================= */

async function runJobOnce(
  id,
  chatId = null
) {
  const claim =
    await db(
      `UPDATE jobs
       SET
         status='processing',
         chat_id=COALESCE(
           $2,
           chat_id
         ),
         error=NULL,
         updated_at=NOW()
       WHERE id=$1
         AND status='queued'
       RETURNING *`,
      [
        id,
        chatId,
      ]
    );

  if (
    !claim.rows[0]
  ) {
    return false;
  }

  let job =
    claim.rows[0];

  const targetChat =
    chatId ||
    job.chat_id;

  try {
    if (!job.script) {
      await updateJob(
        id,
        {
          stage: "script",
          progress: 10,
        }
      );

      await sendMessage(
        targetChat,
        "📝 Generating original script\nProgress: 10%"
      );

      const r =
        await generateScript(
          job.topic
        );

      const wc =
        qualityCheck(
          r.script
        );

      await updateJob(
        id,
        {
          script:
            r.script,
          progress: 50,
          stage:
            "quality_checked",
        }
      );

      await sendMessage(
        targetChat,
        `📝 Script generated — ${r.model}\n\n✅ Quality check passed — ${wc} words\nProgress: 50%`
      );

      job =
        await getJob(id);
    }

    const tts =
      await processTTS(
        job,
        targetChat
      );

    if (!tts) {
      return false;
    }

    job =
      await getJob(id);

    await updateJob(
      id,
      {
        stage: "video",
        progress: 88,
        status: "video",
      }
    );

    await sendMessage(
      targetChat,
      "🎬 Building video + captions + thumbnail\nProgress: 88%"
    );

    const media =
      await buildVideoPackage(
        job,
        tts.audio,
        tts.chunks
      );

    await updateJob(
      id,
      {
        stage:
          "delivering",
        progress: 95,
        status:
          "delivering",
      }
    );

    await sendMessage(
      targetChat,
      "📦 Video package ready\nProgress: 95%"
    );

    await sendVideo(
      targetChat,
      media.video.data,
      `${id}.mp4`,
      `🎬 AI YouTube video completed\nJob: ${id}`
    );

    await sendDocument(
      targetChat,
      media.thumbnail.data,
      `${id}-thumbnail.jpg`,
      "image/jpeg",
      `🖼️ Thumbnail\nJob: ${id}`
    );

    await sendDocument(
      targetChat,
      media.captions.data,
      `${id}-captions.srt`,
      "application/x-subrip",
      `📝 Captions\nJob: ${id}`
    );

    await updateJob(
      id,
      {
        status:
          "completed",
        stage:
          "completed",
        progress: 100,
        error: null,
      }
    );

    await sendMessage(
      targetChat,
      `✅ JOB COMPLETED — 100%\n\n🎬 Video + thumbnail + captions delivered.\nJob: ${id}`
    );

    return true;
  } catch (error) {
    console.error(
      `Job ${id} failed:`,
      error
    );

    await updateJob(
      id,
      {
        status:
          "paused",
        error:
          error.message,
      }
    ).catch(() => {});

    await sendMessage(
      targetChat,
      `⏸️ JOB PAUSED SAFELY\n\n${userFacingError(error)}\n\nResume with:\n/resume ${id}`
    ).catch(() => {});

    return false;
  }
}

/* =========================
   CREATE
========================= */

async function createJob(
  topic,
  chatId
) {
  const id =
    `job_${Date.now()}_${Math.floor(Math.random() * 1000)}`;

  await db(
    `
    INSERT INTO jobs
      (
        id,
        topic,
        chat_id,
        status,
        progress,
        stage
      )
    VALUES
      (
        $1,
        $2,
        $3,
        'queued',
        0,
        'queued'
      )
    `,
    [
      id,
      topic,
      chatId,
    ]
  );

  sendMessage(
    chatId,
    `🎬 JOB CREATED\n\nTopic: ${topic}\n\nJob ID: ${id}`
  ).catch(() => {});

  setImmediate(
    () =>
      processJob(
        id,
        chatId
      ).catch(
        async (error) => {
          console.error(
            "Background job error:",
            error
          );

          await updateJob(
            id,
            {
              status:
                "paused",
              error:
                error.message,
            }
          ).catch(() => {});

          await sendMessage(
            chatId,
            `⏸️ JOB PAUSED SAFELY\n\n${userFacingError(error)}\n\nResume with:\n/resume ${id}`
          ).catch(() => {});
        }
      )
  );

  return id;
}

/* =========================
   RESUME
========================= */

async function resumeJob(
  id,
  chatId
) {
  const job =
    await getJob(id);

  if (!job) {
    await sendMessage(
      chatId,
      "❌ Job not found."
    );

    return;
  }

  if (
    job.chat_id &&
    String(
      job.chat_id
    ) !==
      String(chatId)
  ) {
    await sendMessage(
      chatId,
      "❌ This job belongs to another chat."
    );

    return;
  }

  if (
    job.status ===
    "completed"
  ) {
    await sendMessage(
      chatId,
      "✅ This job is already completed."
    );

    return;
  }

  if ([
    "processing",
    "tts",
    "video",
    "delivering",
  ].includes(job.status)) {
    await sendMessage(
      chatId,
      "ℹ️ This job is already running."
    );

    return;
  }

  if (
    job.status ===
    "queued"
  ) {
    await sendMessage(
      chatId,
      "ℹ️ This job is already queued."
    );

    return;
  }

  if (
    job.status !==
    "paused"
  ) {
    await sendMessage(
      chatId,
      `ℹ️ Cannot resume job from status: ${job.status}`
    );

    return;
  }

  const claim =
    await db(
      `UPDATE jobs
       SET
         status='queued',
         chat_id=$2,
         error=NULL,
         updated_at=NOW()
       WHERE id=$1
         AND status='paused'
       RETURNING id`,
      [
        id,
        chatId,
      ]
    );

  if (
    !claim.rows[0]
  ) {
    await sendMessage(
      chatId,
      "ℹ️ Resume already started or job state changed."
    );

    return;
  }

  await sendMessage(
    chatId,
    `▶️ RESUMING JOB\n\n${id}\n\nSaved script, TTS chunks and media will be reused.`
  );

  processJob(
    id,
    chatId
  ).catch(() => {});
}

/* =========================
   STATUS
========================= */

async function status(
  chatId
) {
  const result =
    await db(
      `
      SELECT
        COUNT(*) FILTER (
          WHERE status IN ('processing', 'tts', 'video', 'delivering')
        )::int AS running,

        COUNT(*) FILTER (
          WHERE status = 'paused'
        )::int AS paused,

        COUNT(*) FILTER (
          WHERE status = 'completed'
        )::int AS completed,

        COUNT(*) FILTER (
          WHERE status = 'queued'
        )::int AS queued,

        COUNT(*)::int AS total

      FROM jobs
      WHERE chat_id = $1
      `,
      [chatId]
    );

  const s =
    result.rows[0];

  await sendMessage(
    chatId,
    `🤖 AI YouTube Autopilot

Backend: ONLINE ✅

Running: ${s.running}
Queued: ${s.queued}
Paused: ${s.paused}
Completed: ${s.completed}
Total: ${s.total}

Payment mode: APPROVAL ONLY`
  );
}

async function jobStatus(id, chatId) {
  const job = await getJob(id);

  if (!job) {
    await sendMessage(chatId, "❌ Job not found. Check the job ID and try again.");
    return;
  }

  if (String(job.chat_id) !== String(chatId)) {
    await sendMessage(chatId, "❌ This job belongs to another chat.");
    return;
  }

  const chunks = job.tts_total_chunks
    ? `${job.tts_completed_chunks || 0}/${job.tts_total_chunks}`
    : "not started";

  await sendMessage(
    chatId,
    `Job ${job.id}\nStatus: ${job.status}\nStage: ${job.stage}\nProgress: ${job.progress}%\nTTS chunks: ${chunks}${job.error ? `\nLast error: ${userFacingError({ message: job.error })}` : ""}`
  );
}

const HELP_TEXT = `🤖 AI YouTube Autopilot

/start - show the bot welcome message
/create <topic> - create a 5-6 minute educational video
/status - show your job summary
/status <job_id> - show one of your job's progress
/resume <job_id> - continue a paused job
/help - show this command list`;

/* =========================
   TELEGRAM WEBHOOK
========================= */

app.post(
  "/telegram/webhook",
  async (
    req,
    res
  ) => {
    try {
      if (
        WEBHOOK_SECRET &&
        req.get(
          "X-Telegram-Bot-Api-Secret-Token"
        ) !==
          WEBHOOK_SECRET
      ) {
        return res.sendStatus(
          401
        );
      }

      const updateId =
        req.body?.update_id;

      if (
        Number.isSafeInteger(
          updateId
        )
      ) {
        const ins =
          await db(
            `INSERT INTO telegram_updates(update_id)
             VALUES($1)
             ON CONFLICT DO NOTHING
             RETURNING update_id`,
            [updateId]
          );

        if (
          !ins.rows[0]
        ) {
          return res.sendStatus(
            200
          );
        }
      }

      res.sendStatus(
        200
      );

      const message =
        req.body?.message;

      if (
        !message?.chat?.id ||
        !message?.text
      ) {
        return;
      }

      const chatId =
        message.chat.id;

      const text =
        message.text.trim();

      const firstSpace = text.search(/\s/);
      const rawCommand = firstSpace === -1
        ? text
        : text.slice(0, firstSpace);
      const command = rawCommand.split("@")[0].toLowerCase();
      const argument = firstSpace === -1
        ? ""
        : text.slice(firstSpace).trim();

      /* /start */

      if (
        command === "/start"
      ) {
        await sendMessage(
          chatId,
          `🤖 AI YouTube Autopilot

ONLINE ✅

${HELP_TEXT}

Example:

/create 5 surprising facts about space`
        );

        return;
      }

      /* /help */

      if (command === "/help") {
        await sendMessage(chatId, HELP_TEXT);
        return;
      }

      /* /status */

      if (command === "/status") {
        if (argument) {
          await jobStatus(argument, chatId);
        } else {
          await status(chatId);
        }

        return;
      }

      /* /create */

      if (command === "/create") {
        const topic = argument;

        if (!topic) {
          await sendMessage(
            chatId,
            "Use: /create <topic>"
          );

          return;
        }

        await createJob(
          topic,
          chatId
        );

        return;
      }

      /* /resume */

      if (command === "/resume") {
        const id = argument;

        if (!id) {
          await sendMessage(
            chatId,
            "Use: /resume <job_id>"
          );

          return;
        }

        await resumeJob(
          id,
          chatId
        );

        return;
      }

      /* unknown */

      await sendMessage(
        chatId,
        `Unknown command.

${HELP_TEXT}`
      );
    } catch (error) {
      console.error(
        "Webhook processing error:",
        error
      );
    }
  }
);

/* =========================
   HEALTH CHECK
========================= */

app.get(
  "/",
  (
    req,
    res
  ) => {
    res.json({
      ok: true,
      service:
        "AI YouTube Autopilot",
      status:
        "online",
    });
  }
);

/* =========================
   STARTUP
========================= */

async function startup() {
  try {
    await initDatabase();

    if (
      WEBHOOK_URL &&
      TELEGRAM_BOT_TOKEN
    ) {
      console.log(
        "Setting Telegram webhook:",
        WEBHOOK_URL
      );

      const webhookBody = {
        url:
          WEBHOOK_URL,
        allowed_updates: [
          "message",
        ],
      };

      if (
        WEBHOOK_SECRET
      ) {
        webhookBody.secret_token =
          WEBHOOK_SECRET;
      }

      await telegram(
        "setWebhook",
        webhookBody
      );

      console.log(
        "Telegram webhook configured"
      );
    }

    const recovery =
      await db(
        `
        UPDATE jobs
        SET
          status = 'queued',
          updated_at = NOW()
        WHERE status IN ('processing', 'tts', 'video', 'delivering')
        RETURNING id
        `
      );

    console.log(
      `Recovered ${recovery.rows.length} interrupted job(s) to queued state`
    );

    app.listen(
      PORT,
      () => {
        console.log(
          `AI YouTube Autopilot listening on port ${PORT}`
        );

        console.log(
          "STARTUP COMPLETE"
        );

        setTimeout(
          async () => {
            try {
              const queued =
                await db(
                  `
                  SELECT id, chat_id
                  FROM jobs
                  WHERE status = 'queued'
                  ORDER BY created_at ASC
                  LIMIT 10
                  `
                );

              for (
                const row of
                  queued.rows
              ) {
                processJob(
                  row.id,
                  row.chat_id
                );
              }
            } catch (error) {
              console.error(
                "Queued-job recovery failed:",
                error
              );
            }
          },
          1000
        );
      }
    );
  } catch (error) {
    console.error(
      "STARTUP FAILED:",
      error
    );

    process.exit(
      1
    );
  }
}

/* =========================
   SAFE SHUTDOWN
========================= */

process.on(
  "SIGTERM",
  async () => {
    await pool
      .end()
      .catch(() => {});

    process.exit(
      0
    );
  }
);

process.on(
  "SIGINT",
  async () => {
    await pool
      .end()
      .catch(() => {});

    process.exit(
      0
    );
  }
);

/* =========================
   START
========================= */

startup();

