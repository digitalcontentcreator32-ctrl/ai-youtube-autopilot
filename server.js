import express from "express";
import pg from "pg";
import fs from "node:fs/promises";
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
  ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false,
  connectionTimeoutMillis: 15000,
});

const SCRIPT_MODELS = [
  "gemini-3.5-flash",
  "gemini-3.5-flash-lite",
  "gemini-3.1-flash-lite",
  "gemini-2.5-flash",
];

const TTS_MODELS = [
  "gemini-3.1-flash-tts-preview",
  "gemini-2.5-flash-preview-tts",
  "gemini-2.5-pro-preview-tts",
];

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function db(query, params = []) {
  return pool.query(query, params);
}

/* =========================
   DATABASE
========================= */

async function initDatabase() {
  console.log("Starting PostgreSQL database initialization...");

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
      tts_chunk_size INTEGER DEFAULT 0,
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

  await db(`CREATE TABLE IF NOT EXISTS job_media (job_id TEXT NOT NULL, kind TEXT NOT NULL, data BYTEA NOT NULL, mime_type TEXT NOT NULL, sha256 TEXT, created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW(), PRIMARY KEY (job_id, kind))`);

  // Older deployments may already have job_media without these columns.
  await db(`ALTER TABLE job_media ADD COLUMN IF NOT EXISTS sha256 TEXT`);
  await db(`ALTER TABLE job_media ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()`);
  await db(`ALTER TABLE job_media ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()`);

  await db(`CREATE TABLE IF NOT EXISTS telegram_updates (update_id BIGINT PRIMARY KEY, received_at TIMESTAMPTZ DEFAULT NOW())`);

  await db(`
    ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()
  `);

  await db(`
    ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()
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
    UPDATE jobs
    SET
      tts_total_chunks = COALESCE(tts_total_chunks, 0),
      tts_completed_chunks = COALESCE(tts_completed_chunks, 0),
      tts_current_chunk = COALESCE(tts_current_chunk, 0),
      progress = COALESCE(progress, 0),
      stage = COALESCE(stage, 'queued'),
      status = COALESCE(status, 'queued'),
      updated_at = COALESCE(updated_at, NOW())
  `);

  await db(`
    UPDATE job_audio_chunks
    SET
      status = COALESCE(status, 'pending'),
      created_at = COALESCE(created_at, NOW()),
      updated_at = COALESCE(updated_at, NOW())
  `);

  const check = await db(`
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'job_audio_chunks'
    ORDER BY ordinal_position
  `);

  const columns = check.rows.map(row => row.column_name);

  const required = [
    "job_id",
    "chunk_index",
    "audio_data",
    "mime_type",
    "status",
    "model",
    "error",
    "created_at",
    "updated_at"
  ];

  const missing = required.filter(
    column => !columns.includes(column)
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

async function telegram(method, body = {}) {
  if (!TELEGRAM_BOT_TOKEN) {
    throw new Error("TELEGRAM_BOT_TOKEN missing");
  }

  const response = await fetch(
    `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/${method}`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }
  );

  const raw = await response.text();

  let data;
  try {
    data = JSON.parse(raw);
  } catch {
    throw new Error(`Telegram invalid response: ${raw}`);
  }

  if (!data.ok) {
    throw new Error(`Telegram API error: ${raw}`);
  }

  return data.result;
}

async function sendMessage(chatId, text) {
  if (!chatId) return;

  return telegram("sendMessage", {
    chat_id: chatId,
    text,
    disable_web_page_preview: true,
  });
}

async function sendDocument(
  chatId,
  buffer,
  filename,
  mimeType,
  caption = ""
) {
  if (!chatId) return;

  const form = new FormData();

  form.append("chat_id", String(chatId));

  form.append(
    "document",
    new Blob([buffer], { type: mimeType }),
    filename
  );

  if (caption) {
    form.append("caption", caption);
  }

  const response = await fetch(
    `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendDocument`,
    {
      method: "POST",
      body: form,
    }
  );

  const data = await response.json();

  if (!data.ok) {
    throw new Error(
      `Telegram document error: ${JSON.stringify(data)}`
    );
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
    throw new Error("GEMINI_API_KEY missing");
  }

  const controller = new AbortController();

  const timer = setTimeout(
    () => controller.abort(),
    timeoutMs
  );

  try {
    const response = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-goog-api-key": GEMINI_API_KEY,
        },
        body: JSON.stringify(body),
        signal: controller.signal,
      }
    );

    const raw = await response.text();

    if (!response.ok) {
      const error = new Error(
        `Gemini ${response.status}: ${raw}`
      );

      error.status = response.status;

      throw error;
    }

    return JSON.parse(raw);
  } catch (error) {
    if (error.name === "AbortError") {
      const timeoutError = new Error(
        `REQUEST_TIMEOUT_${timeoutMs}MS`
      );

      timeoutError.code = "TIMEOUT";

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
      lastError = error;

      const retryable =
        error.code === "TIMEOUT" ||
        error.status === 429 ||
        error.status === 500 ||
        error.status === 502 ||
        error.status === 503;

      if (!retryable || attempt === maxAttempts) {
        throw error;
      }

      const wait =
        3000 + Math.floor(Math.random() * 4000);

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

async function generateScript(topic) {
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
- Around 600-800 words.
- Natural spoken narration.
- No stage directions.
`;

  let lastError;

  for (const model of SCRIPT_MODELS) {
    try {
      const result = await retryGemini(
        model,
        {
          contents: [
            {
              role: "user",
              parts: [{ text: prompt }],
            },
          ],
          generationConfig: {
            temperature: 0.8,
            maxOutputTokens: 3000,
          },
        },
        45000,
        2
      );

      const text = result?.candidates?.[0]?.content?.parts
        ?.map((part) => part.text || "")
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

      lastError = error;
    }
  }

  throw lastError ||
    new Error("All script models failed");
}

/* =========================
   QUALITY CHECK
========================= */

function qualityCheck(script) {
  const words = script
    .trim()
    .split(/\s+/)
    .filter(Boolean);

  if (words.length < 150) {
    throw new Error(
      "Script too short for quality gate"
    );
  }

  return words.length;
}

/* =========================
   TTS CHUNKING
========================= */

function splitIntoChunks(
  text,
  maxWords = 450
) {
  const clean = String(text || "")
    .replace(/\s+/g, " ")
    .trim();

  if (!clean) return [];

  const sentences =
    clean.match(/[^.!?]+[.!?]+|[^.!?]+$/g) ||
    [clean];

  const chunks = [];
  let current = [];

  for (const sentence of sentences) {
    const words = sentence
      .trim()
      .split(/\s+/);

    if (words.length > maxWords) {
      if (current.length) {
        chunks.push(current.join(" "));
        current = [];
      }

      for (
        let i = 0;
        i < words.length;
        i += maxWords
      ) {
        chunks.push(
          words
            .slice(i, i + maxWords)
            .join(" ")
        );
      }

      continue;
    }

    if (
      current.length > 0 &&
      current.length + words.length > maxWords
    ) {
      chunks.push(current.join(" "));
      current = [];
    }

    current.push(...words);
  }

  if (current.length) {
    chunks.push(current.join(" "));
  }

  return chunks;
}

/* =========================
   TTS
========================= */

async function generateTTSChunk(text, model) {
  const controller = new AbortController();
  const timeoutMs = 120000;

  const timer = setTimeout(
    () => controller.abort(),
    timeoutMs
  );

  try {
    const response = await fetch(
      "https://generativelanguage.googleapis.com/v1beta/interactions",
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-goog-api-key": GEMINI_API_KEY,
        },
        body: JSON.stringify({
          model,
          input: text,
          response_format: {
            type: "audio",
          },
          generation_config: {
            speech_config: [
              {
                voice: "Kore",
                language: "en-US",
              },
            ],
          },
        }),
        signal: controller.signal,
      }
    );

    const raw = await response.text();

    if (!response.ok) {
      const error = new Error(
        `TTS ${response.status}: ${raw}`
      );

      error.status = response.status;

      throw error;
    }

    const result = JSON.parse(raw);

    const stepContent = Array.isArray(result?.steps)
      ? result.steps.flatMap((step) =>
          Array.isArray(step?.content) ? step.content : []
        )
      : [];

    const outputContent = Array.isArray(result?.outputs)
      ? result.outputs
      : [];

    const audioOutput =
      stepContent.find((item) => item?.type === "audio") ||
      outputContent.find((item) => item?.type === "audio") ||
      (result?.output_audio?.type === "audio"
        ? result.output_audio
        : null);

    const audioData = audioOutput?.data;

    if (!audioData) {
      const stepTypes = stepContent
        .map((item) => item?.type)
        .filter(Boolean);

      throw new Error(
        `TTS returned no audio data. status=${result?.status || "unknown"}; step_types=${stepTypes.join(",") || "none"}; keys=${Object.keys(result || {}).join(",")}`
      );
    }

    const rawBuffer = Buffer.from(audioData, "base64");
    const mimeType =
      audioOutput?.mime_type ||
      "audio/wav";

    const normalizedMimeType =
      String(mimeType)
        .split(";")[0]
        .trim()
        .toLowerCase();

    const parsedRate =
      String(mimeType).match(
        /(?:rate|sample[_-]?rate)\s*=\s*(\d+)/i
      );

    const sampleRate =
      audioOutput?.sample_rate ||
      (parsedRate ? Number(parsedRate[1]) : 24000);

    const buffer = isWav(rawBuffer)
      ? rawBuffer
      : normalizedMimeType === "audio/l16"
        ? pcmToWav(rawBuffer, sampleRate, 1, 16)
        : rawBuffer;

    if (!isWav(buffer)) {
      throw new Error(
        `TTS returned unsupported audio format: ${mimeType}`
      );
    }

    return {
      buffer,
      mimeType: "audio/wav",
      sampleRate,
    };
  } catch (error) {
    if (error.name === "AbortError") {
      const e = new Error(
        `TTS_REQUEST_TIMEOUT_${timeoutMs}MS`
      );

      e.code = "TIMEOUT";

      throw e;
    }

    throw error;
  } finally {
    clearTimeout(timer);
  }
    }
async function generateTTSWithRetry(text) {
  let lastError;

  for (const model of TTS_MODELS) {
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        return {
          ...(await generateTTSChunk(text, model)),
          model
        };
      } catch (error) {
        lastError = error;

        const retryable =
          error.code === "TIMEOUT" ||
          [429, 500, 502, 503].includes(error.status);

        console.log(
          `TTS failed model=${model} attempt=${attempt} status=${error.status || "none"}: ${error.message}`
        );

        if (!retryable) throw error;

        if (attempt < 3) {
          await sleep(
            error.status === 429
              ? 8000 * attempt
              : 3000 + Math.floor(Math.random() * 3000)
          );
        }
      }
    }
  }

  throw lastError ||
    new Error("All TTS models failed");
}

/* =========================
   WAV HELPERS
========================= */

function isWav(buffer) {
  return (
    buffer.length >= 12 &&
    buffer.toString("ascii", 0, 4) === "RIFF" &&
    buffer.toString("ascii", 8, 12) === "WAVE"
  );
}

function pcmToWav(
  pcm,
  sampleRate = 24000,
  channels = 1,
  bits = 16
) {
  const blockAlign =
    (channels * bits) / 8;

  const byteRate =
    sampleRate * blockAlign;

  const header = Buffer.alloc(44);

  header.write("RIFF", 0);
  header.writeUInt32LE(
    36 + pcm.length,
    4
  );

  header.write("WAVE", 8);
  header.write("fmt ", 12);

  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20);
  header.writeUInt16LE(channels, 22);
  header.writeUInt32LE(sampleRate, 24);

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

  header.write("data", 36);

  header.writeUInt32LE(
    pcm.length,
    40
  );

  return Buffer.concat([
    header,
    pcm,
  ]);
}

function getWavPcm(wav) {
  if (!isWav(wav)) {
    return wav;
  }

  let offset = 12;

  while (offset + 8 <= wav.length) {
    const id = wav.toString(
      "ascii",
      offset,
      offset + 4
    );

    const size = wav.readUInt32LE(
      offset + 4
    );

    if (id === "data") {
      const end = Math.min(
        offset + 8 + size,
        wav.length
      );

      return wav.subarray(
        offset + 8,
        end
      );
    }

    offset += 8 + size;
  }

  throw new Error(
    "WAV data chunk not found"
  );
}

function getWavFormat(wav) {
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
    sampleRate: wav.readUInt32LE(24),
    channels: wav.readUInt16LE(22),
    bits: wav.readUInt16LE(34),
  };
}

function concatWavBuffers(buffers) {
  if (!buffers.length) {
    throw new Error(
      "No audio buffers to concatenate"
    );
  }

  const first = isWav(buffers[0])
    ? buffers[0]
    : pcmToWav(buffers[0]);

  const format = getWavFormat(first);

  const pcm = Buffer.concat(
    buffers.map(getWavPcm)
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
  const result = await db(
    `SELECT * FROM jobs WHERE id = $1`,
    [id]
  );

  return result.rows[0] || null;
}

async function updateJob(id, fields) {
  const entries =
    Object.entries(fields);

  if (!entries.length) return;

  const allowed = new Set([
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

  for (const [key, value] of entries) {
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

async function processTTS(job, chatId) {
  // Existing paused jobs keep their old 80-word chunking.
  // New jobs use 180 words to reduce TTS overhead.
  const chunkSize =
    Number(job.tts_chunk_size) > 0
      ? Number(job.tts_chunk_size)
      : (
          Number(job.tts_total_chunks) > 0
            ? 80
            : 180
        );

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

  await updateJob(
    job.id,
    {
      chat_id: targetChat,
      tts_chunk_size: chunkSize,
      tts_total_chunks: chunks.length,
      stage: "tts",
      progress: 55,
      status: "running",
      error: null,
    }
  );

  for (let i = 0; i < chunks.length; i++) {
    const existing =
      await db(
        `
        SELECT audio_data,status
        FROM job_audio_chunks
        WHERE job_id=$1
          AND chunk_index=$2
        `,
        [job.id, i]
      );

    if (
      existing.rows[0]?.status === "completed" &&
      existing.rows[0]?.audio_data
    ) {
      continue;
    }

    await updateJob(
      job.id,
      {
        tts_current_chunk: i,
      }
    );

    try {
      const result =
        await generateTTSWithRetry(
          chunks[i]
        );

      await db(
        `
        INSERT INTO job_audio_chunks
        (
          job_id,
          chunk_index,
          audio_data,
          mime_type,
          status,
          model,
          error
        )
        VALUES
        (
          $1,$2,$3,$4,
          'completed',
          $5,
          NULL
        )
        ON CONFLICT(job_id,chunk_index)
        DO UPDATE SET
          audio_data=EXCLUDED.audio_data,
          mime_type=EXCLUDED.mime_type,
          status='completed',
          model=EXCLUDED.model,
          error=NULL,
          updated_at=NOW()
        `,
        [
          job.id,
          i,
          result.buffer,
          result.mimeType,
          result.model
        ]
      );

      const r =
        await db(
          `
          SELECT COUNT(*)::int AS count
          FROM job_audio_chunks
          WHERE job_id=$1
            AND status='completed'
          `,
          [job.id]
        );

      const completed =
        r.rows[0].count;

      const progress =
        Math.min(
          85,
          55 +
            Math.floor(
              completed /
                chunks.length *
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

      await sendMessage(
        targetChat,
        `🎙️ TTS chunk ${completed}/${chunks.length} completed\nProgress: ${progress}%`
      );

    } catch (error) {
      await db(
        `
        INSERT INTO job_audio_chunks
        (
          job_id,
          chunk_index,
          status,
          error
        )
        VALUES
        (
          $1,$2,'failed',$3
        )
        ON CONFLICT(job_id,chunk_index)
        DO UPDATE SET
          status='failed',
          error=EXCLUDED.error,
          updated_at=NOW()
        `,
        [
          job.id,
          i,
          error.message
        ]
      );

      await updateJob(
        job.id,
        {
          status: "paused",
          stage: "tts",
          error: error.message,
        }
      );

      await sendMessage(
        targetChat,
        `⏸️ JOB PAUSED SAFELY

TTS chunk ${i + 1}/${chunks.length} failed.

Completed chunks are saved.

Resume with:
/resume ${job.id}`
      );

      return null;
    }
  }

  const rows =
    await db(
      `
      SELECT audio_data
      FROM job_audio_chunks
      WHERE job_id=$1
        AND status='completed'
      ORDER BY chunk_index
      `,
      [job.id]
    );

  if (
    rows.rows.length !==
    chunks.length
  ) {
    throw new Error(
      "Not all TTS chunks completed"
    );
  }

  const audio =
    concatWavBuffers(
      rows.rows.map(
        r => r.audio_data
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
function wavDurationSeconds(wav) {
  const f = getWavFormat(wav);
  const pcm = getWavPcm(wav);

  return (
    pcm.length /
    (
      f.sampleRate *
      f.channels *
      (f.bits / 8)
    )
  );
}

function srtTime(seconds) {
  const ms =
    Math.max(
      0,
      Math.round(seconds * 1000)
    );

  const h =
    Math.floor(
      ms / 3600000
    );

  const m =
    Math.floor(
      (ms % 3600000) / 60000
    );

  const s =
    Math.floor(
      (ms % 60000) / 1000
    );

  const x =
    ms % 1000;

  return (
    `${String(h).padStart(2,"0")}:` +
    `${String(m).padStart(2,"0")}:` +
    `${String(s).padStart(2,"0")},` +
    `${String(x).padStart(3,"0")}`
  );
}

function makeSrt(
  chunks,
  audioBuffers
) {
  let t = 0;
  let n = 1;
  const out = [];

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
          .slice(p, p + 12)
          .join(" ");

      const a =
        t +
        dur *
          (p / words.length);

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

/* =========================
   FFMPEG
========================= */

function runFfmpeg(
  args,
  timeoutMs = 600000
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
              "pipe"
            ]
          }
        );

      let err = "";
      let settled = false;

      const finish =
        (fn, value) => {
          if (settled) return;

          settled = true;
          clearTimeout(timer);

          fn(value);
        };

      const timer =
        setTimeout(
          () => {
            c.kill("SIGKILL");

            finish(
              reject,
              new Error(
                "FFMPEG_TIMEOUT"
              )
            );
          },
          timeoutMs
        );

      c.stderr.on(
        "data",
        d => {
          err +=
            d.toString();

          if (
            err.length > 10000
          ) {
            err =
              err.slice(-10000);
          }
        }
      );

      c.on(
        "error",
        e =>
          finish(
            reject,
            e
          )
      );

      c.on(
        "close",
        code => {
          if (code === 0) {
            finish(
              resolve
            );
          } else {
            finish(
              reject,
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
      `
      SELECT
        kind,
        data,
        mime_type
      FROM job_media
      WHERE job_id=$1
        AND kind IN
        (
          'video',
          'thumbnail',
          'captions'
        )
      `,
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
      .createHash("sha256")
      .update(b)
      .digest("hex");

  await db(
    `
    INSERT INTO job_media
    (
      job_id,
      kind,
      data,
      mime_type,
      sha256
    )
    VALUES
    ($1,$2,$3,$4,$5)
    ON CONFLICT(job_id,kind)
    DO UPDATE SET
      data=EXCLUDED.data,
      mime_type=EXCLUDED.mime_type,
      sha256=EXCLUDED.sha256,
      updated_at=NOW()
    `,
    [
      jobId,
      kind,
      b,
      mimeType,
      sha
    ]
  );
}

/* =========================
   ULTRA FAST VIDEO
========================= */

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
        `
        SELECT audio_data
        FROM job_audio_chunks
        WHERE job_id=$1
          AND status='completed'
        ORDER BY chunk_index
        `,
        [job.id]
      );

    const buffers =
      rows.rows.map(
        r =>
          Buffer.from(
            r.audio_data
          )
      );

    if (
      buffers.length !==
      chunks.length
    ) {
      throw new Error(
        "Missing TTS audio chunks for video build"
      );
    }

    const srt =
      makeSrt(
        chunks,
        buffers
      );

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

    // IMPORTANT:
    // Captions are NOT burned into video.
    // SRT is delivered separately.
    // This removes the slow subtitle filter.

    await runFfmpeg(
      [
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",

        "-f",
        "lavfi",

        "-i",
        "color=c=black:s=640x360:r=10",

        "-i",
        audioPath,

        "-t",
        String(dur),

        "-c:v",
        "libx264",

        "-preset",
        "ultrafast",

        "-tune",
        "stillimage",

        "-crf",
        "38",

        "-pix_fmt",
        "yuv420p",

        "-threads",
        "0",

        "-c:a",
        "aac",

        "-b:a",
        "48k",

        "-movflags",
        "+faststart",

        videoPath
      ],
      600000
    );

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

    // Small thumbnail
    await runFfmpeg(
      [
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",

        "-i",
        videoPath,

        "-frames:v",
        "1",

        "-q:v",
        "8",

        thumbPath
      ],
      120000
    );

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
          "video/mp4"
      },

      thumbnail: {
        data: thumb,
        mimeType:
          "image/jpeg"
      },

      captions: {
        data: cap,
        mimeType:
          "application/x-subrip"
      }
    };

  } finally {
    await fs.rm(
      dir,
      {
        recursive: true,
        force: true
      }
    ).catch(
      () => {}
    );
  }
}

/* =========================
   FULL JOB
========================= */

async function processJobInternal(
  id,
  chatId = null
) {
  const claim =
    await db(
      `
      UPDATE jobs
      SET
        status='running',
        chat_id=COALESCE($2,chat_id),
        error=NULL,
        updated_at=NOW()
      WHERE id=$1
        AND status='queued'
      RETURNING *
      `,
      [
        id,
        chatId
      ]
    );

  if (!claim.rows[0]) {
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
          progress: 10
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
          script: r.script,
          progress: 50,
          stage:
            "quality_checked"
        }
      );

      await sendMessage(
        targetChat,
        `📝 Script generated — ${r.model}

✅ Quality check passed — ${wc} words
Progress: 50%`
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
        status: "running"
      }
    );

    await sendMessage(
      targetChat,
      "🎬 Building ULTRA-FAST video + thumbnail (SRT separate)\nProgress: 88%"
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
        stage: "delivery",
        progress: 95,
        status: "running"
      }
    );

    await sendMessage(
      targetChat,
      "📦 Video package ready\nProgress: 95%"
    );

    await sendDocument(
      targetChat,
      media.video.data,
      `${id}.mp4`,
      "video/mp4",
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
        status: "completed",
        stage: "completed",
        progress: 100,
        error: null
      }
    );

    await sendMessage(
      targetChat,
      `✅ JOB COMPLETED — 100%

🎬 Video + thumbnail + captions delivered.
Job: ${id}`
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
        status: "paused",
        error: error.message
      }
    ).catch(
      () => {}
    );

    await sendMessage(
      targetChat,
      `⏸️ JOB PAUSED SAFELY

Reason: ${error.message}

Completed TTS/media are saved.

Resume with:
/resume ${id}`
    ).catch(
      () => {}
    );

    return false;
  }
}

const MAX_CONCURRENT_JOBS =
  Math.max(
    1,
    Math.min(
      2,
      Number(
        process.env.MAX_CONCURRENT_JOBS ||
        1
      )
    )
  );

const activeJobIds =
  new Set();

const jobQueue = [];

function enqueueJob(
  id,
  chatId = null
) {
  if (
    activeJobIds.has(id) ||
    jobQueue.some(
      x => x.id === id
    )
  ) {
    return;
  }

  jobQueue.push({
    id,
    chatId
  });

  drainJobQueue();
}

async function drainJobQueue() {
  while (
    activeJobIds.size <
      MAX_CONCURRENT_JOBS &&
    jobQueue.length
  ) {
    const item =
      jobQueue.shift();

    if (
      activeJobIds.has(
        item.id
      )
    ) {
      continue;
    }

    activeJobIds.add(
      item.id
    );

    processJobInternal(
      item.id,
      item.chatId
    )
      .catch(
        error =>
          console.error(
            `Background job ${item.id} error:`,
            error
          )
      )
      .finally(
        () => {
          activeJobIds.delete(
            item.id
          );

          drainJobQueue();
        }
      );
  }
}

function processJob(
  id,
  chatId = null
) {
  enqueueJob(
    id,
    chatId
  );

  return Promise.resolve(
    true
  );
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
      chatId
    ]
  );

  await sendMessage(
    chatId,
    `🎬 JOB CREATED

Topic: ${topic}

Job ID: ${id}`
  );

  processJob(
    id,
    chatId
  ).catch(
    async error => {
      console.error(
        "Background job error:",
        error
      );

      await updateJob(
        id,
        {
          status: "paused",
          error:
            error.message
        }
      ).catch(
        () => {}
      );

      await sendMessage(
        chatId,
        `⏸️ JOB PAUSED SAFELY

Reason: ${error.message}

Resume with:
/resume ${id}`
      ).catch(
        () => {}
      );
    }
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
    String(job.chat_id) !==
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

  if (
    job.status ===
    "running"
  ) {
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
      `
      UPDATE jobs
      SET
        status='queued',
        chat_id=$2,
        error=NULL,
        updated_at=NOW()
      WHERE id=$1
        AND status='paused'
      RETURNING id
      `,
      [
        id,
        chatId
      ]
    );

  if (!claim.rows[0]) {
    await sendMessage(
      chatId,
      "ℹ️ Resume already started or job state changed."
    );
    return;
  }

  await sendMessage(
    chatId,
    `▶️ RESUMING JOB

${id}

Saved script, TTS chunks and media will be reused.`
  );

  processJob(
    id,
    chatId
  ).catch(
    () => {}
  );
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
          WHERE status = 'running'
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
      `
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

/* =========================
   TELEGRAM WEBHOOK
========================= */

app.post(
  "/telegram/webhook",
  async (req, res) => {
    try {
      if (
        WEBHOOK_SECRET &&
        req.get(
          "X-Telegram-Bot-Api-Secret-Token"
        ) !== WEBHOOK_SECRET
      ) {
        return res.sendStatus(401);
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
            `
            INSERT INTO telegram_updates
            (update_id)
            VALUES ($1)
            ON CONFLICT DO NOTHING
            RETURNING update_id
            `,
            [updateId]
          );

        if (!ins.rows[0]) {
          return res.sendStatus(200);
        }
      }

      res.sendStatus(200);

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

      if (
        text === "/start"
      ) {
        await sendMessage(
          chatId,
          `🤖 AI YouTube Autopilot

ONLINE ✅

Commands:

/create <topic>

/status

/resume <job_id>

Example:

/create 5 surprising facts about space`
        );

        return;
      }

      if (
        text === "/status"
      ) {
        await status(
          chatId
        );

        return;
      }

      if (
        text.startsWith(
          "/create "
        )
      ) {
        const topic =
          text
            .substring(8)
            .trim();

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

      if (
        text.startsWith(
          "/resume "
        )
      ) {
        const id =
          text
            .substring(8)
            .trim();

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

      await sendMessage(
        chatId,
        `Unknown command.

Use:

/start
/create <topic>
/status
/resume <job_id>`
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
  (req, res) => {
    res.json({
      ok: true,
      service:
        "AI YouTube Autopilot",
      status: "online"
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
        url: WEBHOOK_URL,
        allowed_updates: [
          "message"
        ]
      };

      if (WEBHOOK_SECRET) {
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
          status='queued',
          updated_at=NOW()
        WHERE status='running'
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
                  SELECT
                    id,
                    chat_id
                  FROM jobs
                  WHERE status='queued'
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
                ).catch(
                  async error => {
                    await updateJob(
                      row.id,
                      {
                        status:
                          "paused",
                        error:
                          error.message
                      }
                    ).catch(
                      () => {}
                    );

                    await sendMessage(
                      row.chat_id,
                      `⏸️ JOB PAUSED SAFELY

Reason: ${error.message}

Resume with:
/resume ${row.id}`
                    ).catch(
                      () => {}
                    );
                  }
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

    process.exit(1);
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
      .catch(
        () => {}
      );

    process.exit(0);
  }
);

process.on(
  "SIGINT",
  async () => {
    await pool
      .end()
      .catch(
        () => {}
      );

    process.exit(0);
  }
);

/* =========================
   START
========================= */

startup();
