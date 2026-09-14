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
    CREATE TABLE IF NOT EXISTS telegram_updates (
      update_id BIGINT PRIMARY KEY,
      received_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await db(`
    ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()
  `);

  await db(`
    ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()
  `);

  /* =========================
     AUDIO CHUNKS TABLE
  ========================= */

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

  /* =========================
     FINAL SCHEMA VERIFICATION
  ========================= */

  const check = await db(`
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'job_audio_chunks'
    ORDER BY ordinal_position
  `);

  const columns =
    check.rows.map(
      row => row.column_name
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
    "updated_at"
  ];

  const missing =
    required.filter(
      column =>
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

    const sampleRate =
      audioOutput?.sample_rate ||
      24000;

    const buffer = isWav(rawBuffer)
      ? rawBuffer
      : mimeType === "audio/l16"
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
          model,
        };
      } catch (error) {
        lastError = error;

        const retryable =
          error.code === "TIMEOUT" ||
          [429, 500, 502, 503].includes(error.status);

        console.log(
          `TTS failed model=${model} attempt=${attempt} status=${error.status || "none"}: ${error.message}`
        );

        if (!retryable) {
          throw error;
        }

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

  throw (
    lastError ||
    new Error("All TTS models failed")
  );
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
  bitsPerSample = 16
) {
  const byteRate =
    sampleRate *
    channels *
    bitsPerSample /
    8;

  const blockAlign =
    channels *
    bitsPerSample /
    8;

  const header = Buffer.alloc(44);

  header.write("RIFF", 0);
  header.writeUInt32LE(
    36 + pcm.length,
    4
  );
  header.write("WAVE", 8);
  header.write("fmt ", 12);

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
    bitsPerSample,
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

function parseWav(buffer) {
  if (!isWav(buffer)) {
    throw new Error(
      "Invalid WAV file"
    );
  }

  let offset = 12;

  let sampleRate = null;
  let channels = null;
  let bitsPerSample = null;
  let dataStart = null;
  let dataLength = null;

  while (
    offset + 8 <= buffer.length
  ) {
    const chunkId =
      buffer.toString(
        "ascii",
        offset,
        offset + 4
      );

    const chunkSize =
      buffer.readUInt32LE(
        offset + 4
      );

    const chunkStart =
      offset + 8;

    if (chunkId === "fmt ") {
      if (chunkSize >= 16) {
        const audioFormat =
          buffer.readUInt16LE(
            chunkStart
          );

        channels =
          buffer.readUInt16LE(
            chunkStart + 2
          );

        sampleRate =
          buffer.readUInt32LE(
            chunkStart + 4
          );

        bitsPerSample =
          buffer.readUInt16LE(
            chunkStart + 14
          );

        if (audioFormat !== 1) {
          throw new Error(
            `Unsupported WAV audio format: ${audioFormat}`
          );
        }
      }
    }

    if (chunkId === "data") {
      dataStart = chunkStart;
      dataLength = Math.min(
        chunkSize,
        buffer.length - chunkStart
      );
      break;
    }

    offset =
      chunkStart +
      chunkSize +
      (chunkSize % 2);
  }

  if (
    !sampleRate ||
    !channels ||
    !bitsPerSample ||
    dataStart === null
  ) {
    throw new Error(
      "Incomplete WAV metadata"
    );
  }

  return {
    sampleRate,
    channels,
    bitsPerSample,
    data: buffer.subarray(
      dataStart,
      dataStart + dataLength
    ),
  };
}

function buildWav(
  pcm,
  sampleRate,
  channels,
  bitsPerSample
) {
  return pcmToWav(
    pcm,
    sampleRate,
    channels,
    bitsPerSample
  );
}

function mergeWavs(wavs) {
  if (!wavs.length) {
    throw new Error(
      "No WAV files to merge"
    );
  }

  const parsed =
    wavs.map(parseWav);

  const first = parsed[0];

  for (const wav of parsed) {
    if (
      wav.sampleRate !== first.sampleRate ||
      wav.channels !== first.channels ||
      wav.bitsPerSample !== first.bitsPerSample
    ) {
      throw new Error(
        "TTS WAV format mismatch between chunks"
      );
    }
  }

  const pcm = Buffer.concat(
    parsed.map(
      (wav) => wav.data
    )
  );

  return buildWav(
    pcm,
    first.sampleRate,
    first.channels,
    first.bitsPerSample
  );
}

function wavDurationSeconds(
  wavBuffer
) {
  const wav = parseWav(wavBuffer);

  const bytesPerSample =
    wav.bitsPerSample / 8;

  const frameSize =
    wav.channels *
    bytesPerSample;

  if (!frameSize || !wav.sampleRate) {
    return 0;
  }

  const frames =
    wav.data.length / frameSize;

  return frames / wav.sampleRate;
}

/* =========================
   DATABASE MEDIA
========================= */

function sha256(buffer) {
  return crypto
    .createHash("sha256")
    .update(buffer)
    .digest("hex");
}

async function saveMedia(
  jobId,
  kind,
  data,
  mimeType
) {
  await db(
    `
      INSERT INTO job_media
        (job_id, kind, data, mime_type, sha256, updated_at)
      VALUES
        ($1, $2, $3, $4, $5, NOW())
      ON CONFLICT (job_id, kind)
      DO UPDATE SET
        data = EXCLUDED.data,
        mime_type = EXCLUDED.mime_type,
        sha256 = EXCLUDED.sha256,
        updated_at = NOW()
    `,
    [
      jobId,
      kind,
      data,
      mimeType,
      sha256(data),
    ]
  );
}

async function getMedia(
  jobId,
  kind
) {
  const result = await db(
    `
      SELECT data, mime_type, sha256
      FROM job_media
      WHERE job_id = $1
        AND kind = $2
      LIMIT 1
    `,
    [jobId, kind]
  );

  if (!result.rows.length) {
    return null;
  }

  return {
    data: Buffer.from(
      result.rows[0].data
    ),
    mimeType:
      result.rows[0].mime_type,
    sha256:
      result.rows[0].sha256,
  };
}

/* =========================
   JOB HELPERS
========================= */

async function updateJob(
  jobId,
  fields
) {
  const allowed = [
    "status",
    "progress",
    "stage",
    "script",
    "error",
    "tts_total_chunks",
    "tts_completed_chunks",
    "tts_current_chunk",
  ];

  const entries =
    Object.entries(fields)
      .filter(([key]) =>
        allowed.includes(key)
      );

  if (!entries.length) {
    return;
  }

  const values = [];
  const sets = [];

  entries.forEach(
    ([key, value], index) => {
      values.push(value);
      sets.push(
        `${key} = $${index + 2}`
      );
    }
  );

  values.unshift(jobId);

  sets.push(
    "updated_at = NOW()"
  );

  await db(
    `
      UPDATE jobs
      SET ${sets.join(", ")}
      WHERE id = $1
    `,
    values
  );
}

async function getJob(jobId) {
  const result = await db(
    `
      SELECT *
      FROM jobs
      WHERE id = $1
      LIMIT 1
    `,
    [jobId]
  );

  return result.rows[0] || null;
}

async function createJob(
  topic,
  chatId
) {
  const id =
    crypto.randomUUID();

  await db(
    `
      INSERT INTO jobs
        (
          id,
          topic,
          chat_id,
          status,
          progress,
          stage,
          created_at,
          updated_at
        )
      VALUES
        (
          $1,
          $2,
          $3,
          'queued',
          0,
          'queued',
          NOW(),
          NOW()
        )
    `,
    [
      id,
      topic,
      chatId,
    ]
  );

  return getJob(id);
}

/* =========================
   TELEGRAM UPDATE DEDUPE
========================= */

async function claimTelegramUpdate(
  updateId
) {
  if (
    updateId === undefined ||
    updateId === null
  ) {
    return true;
  }

  const result = await db(
    `
      INSERT INTO telegram_updates
        (update_id)
      VALUES
        ($1)
      ON CONFLICT (update_id)
      DO NOTHING
      RETURNING update_id
    `,
    [String(updateId)]
  );

  return result.rowCount === 1;
}

/* =========================
   SRT
========================= */

function formatSrtTime(seconds) {
  const safe =
    Math.max(
      0,
      Number(seconds) || 0
    );

  const hours =
    Math.floor(
      safe / 3600
    );

  const minutes =
    Math.floor(
      (safe % 3600) / 60
    );

  const secs =
    Math.floor(
      safe % 60
    );

  const millis =
    Math.floor(
      (safe -
        Math.floor(safe)) *
        1000
    );

  return (
    `${String(hours).padStart(2, "0")}:` +
    `${String(minutes).padStart(2, "0")}:` +
    `${String(secs).padStart(2, "0")},` +
    `${String(millis).padStart(3, "0")}`
  );
}

function createSubtitleGroups(
  text,
  wordsPerSubtitle = 12
) {
  const words =
    text
      .trim()
      .split(/\s+/)
      .filter(Boolean);

  const groups = [];

  for (
    let i = 0;
    i < words.length;
    i += wordsPerSubtitle
  ) {
    groups.push(
      words
        .slice(
          i,
          i + wordsPerSubtitle
        )
        .join(" ")
    );
  }

  return groups;
}

function buildSrt(
  script,
  chunkBuffers,
  chunkTexts
) {
  const entries = [];

  let globalTime = 0;

  for (
    let chunkIndex = 0;
    chunkIndex < chunkBuffers.length;
    chunkIndex++
  ) {
    const chunkText =
      chunkTexts[chunkIndex] || "";

    const duration =
      wavDurationSeconds(
        chunkBuffers[chunkIndex]
      );

    const groups =
      createSubtitleGroups(
        chunkText,
        12
      );

    if (!groups.length) {
      globalTime += duration;
      continue;
    }

    const groupDuration =
      duration / groups.length;

    for (
      let i = 0;
      i < groups.length;
      i++
    ) {
      const start =
        globalTime +
        i * groupDuration;

      const end =
        globalTime +
        (i + 1) * groupDuration;

      entries.push({
        index:
          entries.length + 1,
        start,
        end,
        text: groups[i],
      });
    }

    globalTime += duration;
  }

  return entries
    .map(
      (entry) =>
        `${entry.index}\n` +
        `${formatSrtTime(entry.start)} --> ${formatSrtTime(entry.end)}\n` +
        `${entry.text}\n`
    )
    .join("\n");
}

/* =========================
   FFMPEG
========================= */

function runFfmpeg(
  args,
  timeoutMs = 300000
) {
  return new Promise(
    (resolve, reject) => {
      if (!ffmpegPath) {
        reject(
          new Error(
            "ffmpeg-static executable not found"
          )
        );
        return;
      }

      const child =
        spawn(
          ffmpegPath,
          args,
          {
            stdio: [
              "ignore",
              "pipe",
              "pipe",
            ],
          }
        );

      let stdout = "";
      let stderr = "";

      child.stdout.on(
        "data",
        (data) => {
          stdout += data.toString();
        }
      );

      child.stderr.on(
        "data",
        (data) => {
          stderr += data.toString();
        }
      );

      let finished = false;

      const timer =
        setTimeout(() => {
          if (finished) return;

          finished = true;

          child.kill("SIGKILL");

          reject(
            new Error(
              `FFmpeg timeout after ${timeoutMs}ms`
            )
          );
        }, timeoutMs);

      child.on(
        "error",
        (error) => {
          if (finished) return;

          finished = true;
          clearTimeout(timer);

          reject(error);
        }
      );

      child.on(
        "close",
        (code, signal) => {
          if (finished) return;

          finished = true;
          clearTimeout(timer);

          if (code !== 0) {
            reject(
              new Error(
                `FFmpeg failed code=${code} signal=${signal || "none"}\n${stderr.slice(-5000)}`
              )
            );
            return;
          }

          resolve({
            stdout,
            stderr,
          });
        }
      );
    }
  );
}

/* =========================
   VIDEO PACKAGE
========================= */

async function buildVideoPackage(
  job,
  script,
  audioBuffer,
  chunkBuffers,
  chunkTexts
) {
  const existingVideo =
    await getMedia(
      job.id,
      "video"
    );

  const existingThumbnail =
    await getMedia(
      job.id,
      "thumbnail"
    );

  const existingCaptions =
    await getMedia(
      job.id,
      "captions"
    );

  if (
    existingVideo &&
    existingThumbnail &&
    existingCaptions
  ) {
    console.log(
      `Reusing persisted media for job ${job.id}`
    );

    return {
      video: existingVideo.data,
      thumbnail:
        existingThumbnail.data,
      captions:
        existingCaptions.data,
    };
  }

  const tempDir =
    await fs.mkdtemp(
      path.join(
        os.tmpdir(),
        `yt-${job.id}-`
      )
    );

  const audioPath =
    path.join(
      tempDir,
      "narration.wav"
    );

  const srtPath =
    path.join(
      tempDir,
      "captions.srt"
    );

  const videoPath =
    path.join(
      tempDir,
      "video.mp4"
    );

  const thumbnailPath =
    path.join(
      tempDir,
      "thumbnail.jpg"
    );

  try {
    await fs.writeFile(
      audioPath,
      audioBuffer
    );

    const srt =
      buildSrt(
        script,
        chunkBuffers,
        chunkTexts
      );

    const srtBuffer =
      Buffer.from(
        srt,
        "utf8"
      );

    await fs.writeFile(
      srtPath,
      srtBuffer
    );

    /*
      Use a simple, reliable 1280x720
      background with burned subtitles.

      This keeps the pipeline deterministic
      and avoids external image/video
      dependencies.
    */

    const escapedSrtPath =
      srtPath
        .replace(/\\/g, "/")
        .replace(/:/g, "\\:")
        .replace(/'/g, "\\'");

    const subtitleFilter =
      `subtitles='${escapedSrtPath}':force_style='FontName=Arial,FontSize=28,Alignment=2,MarginV=70,Outline=2,Shadow=1'`;

    await runFfmpeg([
      "-y",

      "-f",
      "lavfi",

      "-i",
      "color=c=black:s=1280x720:r=30",

      "-i",
      audioPath,

      "-vf",
      subtitleFilter,

      "-c:v",
      "libx264",

      "-preset",
      "veryfast",

      "-crf",
      "28",

      "-maxrate",
      "2200k",

      "-bufsize",
      "4400k",

      "-pix_fmt",
      "yuv420p",

      "-c:a",
      "aac",

      "-b:a",
      "128k",

      "-shortest",

      "-movflags",
      "+faststart",

      videoPath,
    ]);

    const video =
      await fs.readFile(
        videoPath
      );

    if (video.length > 49 * 1024 * 1024) {
      throw new Error(
        `Generated video is too large for Telegram document upload: ${video.length} bytes`
      );
    }

    await runFfmpeg([
      "-y",

      "-ss",
      "1",

      "-i",
      videoPath,

      "-frames:v",
      "1",

      "-q:v",
      "3",

      thumbnailPath,
    ]);

    const thumbnail =
      await fs.readFile(
        thumbnailPath
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
      thumbnail,
      "image/jpeg"
    );

    await saveMedia(
      job.id,
      "captions",
      srtBuffer,
      "application/x-subrip"
    );

    return {
      video,
      thumbnail,
      captions:
        srtBuffer,
    };
  } finally {
    await fs.rm(
      tempDir,
      {
        recursive: true,
        force: true,
      }
    ).catch(() => {});
  }
}

/* =========================
   TTS JOB
========================= */

async function generateJobNarration(
  job,
  script
) {
  const chunks =
    splitIntoChunks(
      script,
      80
    );

  if (!chunks.length) {
    throw new Error(
      "No TTS chunks generated"
    );
  }

  await updateJob(
    job.id,
    {
      stage: "tts",
      progress: 50,
      tts_total_chunks:
        chunks.length,
      tts_completed_chunks: 0,
      tts_current_chunk: 0,
      error: null,
    }
  );

  const buffers = [];

  for (
    let i = 0;
    i < chunks.length;
    i++
  ) {
    const chunkIndex = i + 1;

    const existing =
      await db(
        `
          SELECT
            audio_data,
            mime_type,
            status,
            model
          FROM job_audio_chunks
          WHERE job_id = $1
            AND chunk_index = $2
          LIMIT 1
        `,
        [
          job.id,
          chunkIndex,
        ]
      );

    let audioBuffer = null;

    if (
      existing.rows.length &&
      existing.rows[0].audio_data &&
      existing.rows[0].status ===
        "completed"
    ) {
      audioBuffer =
        Buffer.from(
          existing.rows[0].audio_data
        );

      console.log(
        `TTS chunk ${chunkIndex}/${chunks.length} reused`
      );
    } else {
      await updateJob(
        job.id,
        {
          tts_current_chunk:
            chunkIndex,
        }
      );

      const result =
        await generateTTSWithRetry(
          chunks[i]
        );

      audioBuffer =
        result.buffer;

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
              error,
              updated_at
            )
          VALUES
            ($1, $2, $3, $4, 'completed', $5, NULL, NOW())
          ON CONFLICT
            (job_id, chunk_index)
          DO UPDATE SET
            audio_data = EXCLUDED.audio_data,
            mime_type = EXCLUDED.mime_type,
            status = 'completed',
            model = EXCLUDED.model,
            error = NULL,
            updated_at = NOW()
        `,
        [
          job.id,
          chunkIndex,
          audioBuffer,
          result.mimeType,
          result.model,
        ]
      );

      console.log(
        `TTS chunk ${chunkIndex}/${chunks.length} completed`
      );
    }

    buffers.push(
      audioBuffer
    );

    const progress =
      50 +
      Math.floor(
        (chunkIndex /
          chunks.length) *
          35
      );

    await updateJob(
      job.id,
      {
        tts_completed_chunks:
          chunkIndex,
        tts_current_chunk:
          chunkIndex,
        progress,
      }
    );

    if (job.chat_id) {
      await sendMessage(
        job.chat_id,
        `TTS chunk ${chunkIndex}/${chunks.length} completed — ${progress}%`
      ).catch(() => {});
    }
  }

  const merged =
    mergeWavs(buffers);

  return {
    audio: merged,
    chunks: buffers,
    texts: chunks,
  };
}

/* =========================
   PROCESS JOB
========================= */

const activeJobs =
  new Set();

async function processJob(
  jobId
) {
  if (activeJobs.has(jobId)) {
    console.log(
      `Job ${jobId} already active`
    );
    return;
  }

  activeJobs.add(jobId);

  try {
    /*
      Atomic claim prevents two workers
      from processing the same job.
    */

    const claimed =
      await db(
        `
          UPDATE jobs
          SET
            status = 'running',
            stage = 'starting',
            updated_at = NOW()
          WHERE id = $1
            AND status IN ('queued', 'paused')
          RETURNING *
        `,
        [jobId]
      );

    if (!claimed.rows.length) {
      console.log(
        `Job ${jobId} could not be claimed`
      );
      return;
    }

    let job =
      claimed.rows[0];

    console.log(
      `Processing job ${job.id}: ${job.topic}`
    );

    await updateJob(
      job.id,
      {
        progress: 5,
        stage: "script",
        error: null,
      }
    );

    let script =
      job.script;

    if (!script) {
      const generated =
        await generateScript(
          job.topic
        );

      script =
        generated.script;

      await updateJob(
        job.id,
        {
          script,
          progress: 25,
          stage: "quality_check",
        }
      );

      console.log(
        `Script generated with ${generated.model}`
      );
    } else {
      await updateJob(
        job.id,
        {
          progress: 25,
          stage: "quality_check",
        }
      );
    }

    const wordCount =
      qualityCheck(script);

    console.log(
      `Quality check passed: ${wordCount} words`
    );

    await updateJob(
      job.id,
      {
        progress: 40,
        stage: "tts",
      }
    );

    const narration =
      await generateJobNarration(
        job,
        script
      );

    await updateJob(
      job.id,
      {
        progress: 88,
        stage: "video",
      }
    );

    if (job.chat_id) {
      await sendMessage(
        job.chat_id,
        "AI narration completed. Building video, captions and thumbnail..."
      ).catch(() => {});
    }

    const media =
      await buildVideoPackage(
        job,
        script,
        narration.audio,
        narration.chunks,
        narration.texts
      );

    await updateJob(
      job.id,
      {
        progress: 93,
        stage: "telegram_delivery",
      }
    );

    if (job.chat_id) {
      await sendDocument(
        job.chat_id,
        media.video,
        `${job.id}.mp4`,
        "video/mp4",
        `🎬 Video ready\nJob: ${job.id}`
      );

      await sendDocument(
        job.chat_id,
        media.thumbnail,
        `${job.id}-thumbnail.jpg`,
        "image/jpeg",
        "🖼 Thumbnail"
      );

      await sendDocument(
        job.chat_id,
        media.captions,
        `${job.id}.srt`,
        "application/x-subrip",
        "📝 Captions"
      );
    }

    /*
      Completed only after all Telegram
      deliveries succeed.
    */

    await updateJob(
      job.id,
      {
        status: "completed",
        progress: 100,
        stage: "completed",
        error: null,
      }
    );

    console.log(
      `TTS COMPLETED — job ${job.id}`
    );

    if (job.chat_id) {
      await sendMessage(
        job.chat_id,
        "✅ FULL PIPELINE COMPLETED — Script → TTS → WAV → Video → Captions → Thumbnail → Telegram\n\nJob completed successfully."
      ).catch(() => {});
    }
  } catch (error) {
    console.error(
      `Job ${jobId} failed:`,
      error
    );

    try {
      await updateJob(
        jobId,
        {
          status: "paused",
          stage: "paused",
          error:
            error?.message ||
            String(error),
        }
      );
    } catch (dbError) {
      console.error(
        "Failed to save job error:",
        dbError
      );
    }

    const failedJob =
      await getJob(
        jobId
      ).catch(() => null);

    if (failedJob?.chat_id) {
      await sendMessage(
        failedJob.chat_id,
        `⚠️ Job paused safely.\n\nError: ${error?.message || error}\n\nUse /resume to continue from saved progress.`
      ).catch(() => {});
    }
  } finally {
    activeJobs.delete(jobId);
  }
}

/* =========================
   RESUME
========================= */

async function resumeJob(
  jobId,
  chatId
) {
  const job =
    await getJob(
      jobId
    );

  if (!job) {
    await sendMessage(
      chatId,
      `Job ${jobId} not found.`
    );
    return;
  }

  if (
    String(job.chat_id) !==
    String(chatId)
  ) {
    await sendMessage(
      chatId,
      "❌ You cannot resume another user's job."
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
      "⏳ This job is already running."
    );
    return;
  }

  const updated =
    await db(
      `
        UPDATE jobs
        SET
          status = 'queued',
          stage = 'queued',
          error = NULL,
          updated_at = NOW()
        WHERE id = $1
          AND chat_id = $2
          AND status = 'paused'
        RETURNING *
      `,
      [
        jobId,
        chatId,
      ]
    );

  if (!updated.rows.length) {
    await sendMessage(
      chatId,
      "This job cannot be resumed right now."
    );
    return;
  }

  await sendMessage(
    chatId,
    `▶️ Resuming job ${jobId} from saved progress...`
  );

  processJob(
    jobId
  ).catch(
    (error) =>
      console.error(
        "Resume process error:",
        error
      )
  );
}

/* =========================
   STATUS
========================= */

async function sendStatus(
  chatId
) {
  const result =
    await db(
      `
        SELECT
          COUNT(*)::int AS total,
          COUNT(*) FILTER (
            WHERE status = 'queued'
          )::int AS queued,
          COUNT(*) FILTER (
            WHERE status = 'running'
          )::int AS running,
          COUNT(*) FILTER (
            WHERE status = 'paused'
          )::int AS paused,
          COUNT(*) FILTER (
            WHERE status = 'completed'
          )::int AS completed
        FROM jobs
        WHERE chat_id = $1
      `,
      [chatId]
    );

  const stats =
    result.rows[0];

  const latest =
    await db(
      `
        SELECT
          id,
          topic,
          status,
          progress,
          stage,
          error,
          created_at,
          updated_at
        FROM jobs
        WHERE chat_id = $1
        ORDER BY created_at DESC
        LIMIT 3
      `,
      [chatId]
    );

  let message =
    "📊 Your job status\n\n" +
    `Total: ${stats.total}\n` +
    `Queued: ${stats.queued}\n` +
    `Running: ${stats.running}\n` +
    `Paused: ${stats.paused}\n` +
    `Completed: ${stats.completed}\n`;

  if (latest.rows.length) {
    message +=
      "\nLatest jobs:\n\n";

    for (const row of latest.rows) {
      message +=
        `🆔 ${row.id}\n` +
        `Topic: ${row.topic}\n` +
        `Status: ${row.status}\n` +
        `Progress: ${row.progress}%\n` +
        `Stage: ${row.stage}\n`;

      if (row.error) {
        message +=
          `Error: ${row.error.slice(0, 300)}\n`;
      }

      message += "\n";
    }
  }

  await sendMessage(
    chatId,
    message
  );
}

/* =========================
   TELEGRAM COMMANDS
========================= */

async function handleTelegramUpdate(
  update
) {
  const message =
    update?.message;

  if (!message?.chat?.id) {
    return;
  }

  const chatId =
    message.chat.id;

  const text =
    String(
      message.text || ""
    ).trim();

  if (!text) {
    return;
  }

  if (
    text === "/start" ||
    text.startsWith("/start ")
  ) {
    await sendMessage(
      chatId,
      "🤖 AI YouTube Autopilot is online.\n\n/create <topic> — create a video\n/status — show your jobs\n/resume <job_id> — resume a paused job"
    );

    return;
  }

  if (
    text === "/status"
  ) {
    await sendStatus(
      chatId
    );

    return;
  }

  if (
    text.startsWith("/create")
  ) {
    const topic =
      text
        .replace(
          /^\/create(?:@\w+)?/i,
          ""
        )
        .trim();

    if (!topic) {
      await sendMessage(
        chatId,
        "Usage:\n/create 3 interesting facts about space"
      );
      return;
    }

    const job =
      await createJob(
        topic,
        chatId
      );

    await sendMessage(
      chatId,
      `🆕 Job created\n\nID: ${job.id}\nTopic: ${topic}\n\nStarting pipeline...`
    );

    processJob(
      job.id
    ).catch(
      (error) =>
        console.error(
          "Create process error:",
          error
        )
    );

    return;
  }

  if (
    text.startsWith("/resume")
  ) {
    const jobId =
      text
        .replace(
          /^\/resume(?:@\w+)?/i,
          ""
        )
        .trim();

    if (!jobId) {
      await sendMessage(
        chatId,
        "Usage:\n/resume <job_id>"
      );
      return;
    }

    await resumeJob(
      jobId,
      chatId
    );

    return;
  }

  await sendMessage(
    chatId,
    "Unknown command.\n\nUse /start, /create <topic>, /status or /resume <job_id>."
  );
}

/* =========================
   WEBHOOK
========================= */

app.post(
  "/telegram/webhook",
  async (req, res) => {
    try {
      if (WEBHOOK_SECRET) {
        const receivedSecret =
          req.get(
            "X-Telegram-Bot-Api-Secret-Token"
          );

        if (
          receivedSecret !==
          WEBHOOK_SECRET
        ) {
          console.warn(
            "Rejected Telegram webhook: invalid secret"
          );

          return res
            .status(401)
            .json({
              ok: false,
            });
        }
      }

      const update =
        req.body;

      const claimed =
        await claimTelegramUpdate(
          update?.update_id
        );

      if (!claimed) {
        return res
          .status(200)
          .json({
            ok: true,
            duplicate: true,
          });
      }

      res
        .status(200)
        .json({
          ok: true,
        });

      handleTelegramUpdate(
        update
      ).catch(
        (error) =>
          console.error(
            "Telegram update handler error:",
            error
          )
      );
    } catch (error) {
      console.error(
        "Telegram webhook error:",
        error
      );

      return res
        .status(500)
        .json({
          ok: false,
        });
    }
  }
);

/* =========================
   HEALTH
========================= */

app.get(
  "/",
  async (req, res) => {
    res.json({
      ok: true,
      service:
        "ai-youtube-autopilot",
      version:
        "telegram-gemini-tts-final-2026-09-14-v4",
      status:
        "online",
      ffmpeg:
        Boolean(ffmpegPath),
    });
  }
);

app.get(
  "/health",
  async (req, res) => {
    try {
      await db(
        "SELECT 1"
      );

      res.json({
        ok: true,
        database: "online",
        ffmpeg:
          Boolean(ffmpegPath),
      });
    } catch (error) {
      res
        .status(503)
        .json({
          ok: false,
          database: "offline",
          error:
            error.message,
        });
    }
  }
);

/* =========================
   STARTUP RECOVERY
========================= */

async function recoverJobs() {
  /*
    Render/container restarts can leave
    jobs marked running. Put them back into
    queued so they can safely continue.
  */

  const result =
    await db(
      `
        UPDATE jobs
        SET
          status = 'queued',
          stage = 'queued',
          updated_at = NOW()
        WHERE status = 'running'
        RETURNING id
      `
    );

  if (result.rows.length) {
    console.log(
      `Recovered ${result.rows.length} interrupted jobs`
    );
  }

  const queued =
    await db(
      `
        SELECT id
        FROM jobs
        WHERE status = 'queued'
        ORDER BY created_at ASC
        LIMIT 10
      `
    );

  for (const row of queued.rows) {
    processJob(
      row.id
    ).catch(
      (error) =>
        console.error(
          "Recovered job error:",
          error
        )
    );
  }
}

/* =========================
   WEBHOOK SETUP
========================= */

async function configureWebhook() {
  if (
    !WEBHOOK_URL ||
    !TELEGRAM_BOT_TOKEN
  ) {
    console.log(
      "WEBHOOK_URL or TELEGRAM_BOT_TOKEN missing; skipping webhook setup"
    );
    return;
  }

  const url =
    `${WEBHOOK_URL.replace(/\/$/, "")}/telegram/webhook`;

  const body = {
    url,
    drop_pending_updates: false,
  };

  if (WEBHOOK_SECRET) {
    body.secret_token =
      WEBHOOK_SECRET;
  }

  try {
    const result =
      await telegram(
        "setWebhook",
        body
      );

    console.log(
      "Telegram webhook configured:",
      result
    );
  } catch (error) {
    console.error(
      "Telegram webhook setup failed:",
      error.message
    );
  }
}

/* =========================
   ENV VALIDATION
========================= */

function validateEnvironment() {
  const missing = [];

  if (!process.env.DATABASE_URL) {
    missing.push(
      "DATABASE_URL"
    );
  }

  if (!TELEGRAM_BOT_TOKEN) {
    missing.push(
      "TELEGRAM_BOT_TOKEN"
    );
  }

  if (!GEMINI_API_KEY) {
    missing.push(
      "GEMINI_API_KEY"
    );
  }

  if (missing.length) {
    throw new Error(
      `Missing required environment variables: ${missing.join(", ")}`
    );
  }

  if (!ffmpegPath) {
    throw new Error(
      "ffmpeg-static is unavailable"
    );
  }
}

/* =========================
   SERVER START
========================= */

async function start() {
  validateEnvironment();

  await initDatabase();

  app.listen(
    PORT,
    "0.0.0.0",
    () => {
      console.log(
        `AI YouTube Autopilot listening on port ${PORT}`
      );
    }
  );

  await configureWebhook();

  await recoverJobs();
}

start().catch(
  (error) => {
    console.error(
      "FATAL STARTUP ERROR:",
      error
    );

    process.exit(1);
  }
);
