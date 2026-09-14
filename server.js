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
const DATABASE_URL = process.env.DATABASE_URL;
const WEBHOOK_URL = process.env.WEBHOOK_URL;
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || "";

if (!DATABASE_URL) {
  console.error("DATABASE_URL is missing");
  process.exit(1);
}

if (!TELEGRAM_BOT_TOKEN) {
  console.error("TELEGRAM_BOT_TOKEN is missing");
  process.exit(1);
}

if (!GEMINI_API_KEY) {
  console.error("GEMINI_API_KEY is missing");
  process.exit(1);
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: DATABASE_URL.includes("localhost")
    ? false
    : { rejectUnauthorized: false },
  max: 5,
});

async function db(text, params = []) {
  return pool.query(text, params);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/* =========================
   GEMINI MODELS
========================= */

const SCRIPT_MODELS = [
  "gemini-3.1-flash-lite",
  "gemini-3.1-flash",
  "gemini-2.5-flash",
];

const TTS_MODELS = [
  "gemini-3.1-flash-tts-preview",
  "gemini-2.5-flash-preview-tts",
  "gemini-2.5-pro-preview-tts",
];

/* =========================
   DATABASE
========================= */

async function initDatabase() {
  await db(`
    CREATE TABLE IF NOT EXISTS jobs (
      id TEXT PRIMARY KEY,
      topic TEXT NOT NULL,
      chat_id TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'queued',
      progress INTEGER NOT NULL DEFAULT 0,
      stage TEXT NOT NULL DEFAULT 'queued',
      script TEXT,
      error TEXT,
      tts_total_chunks INTEGER NOT NULL DEFAULT 0,
      tts_completed_chunks INTEGER NOT NULL DEFAULT 0,
      tts_current_chunk INTEGER NOT NULL DEFAULT 0,
      tts_chunk_size INTEGER NOT NULL DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await db(`
    ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS tts_chunk_size INTEGER NOT NULL DEFAULT 0
  `);

  await db(`
    CREATE TABLE IF NOT EXISTS job_audio_chunks (
      id BIGSERIAL PRIMARY KEY,
      job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
      chunk_index INTEGER NOT NULL,
      audio_data BYTEA,
      mime_type TEXT,
      status TEXT NOT NULL DEFAULT 'pending',
      model TEXT,
      error TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(job_id, chunk_index)
    )
  `);

  await db(`
    CREATE TABLE IF NOT EXISTS job_media (
      id BIGSERIAL PRIMARY KEY,
      job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
      kind TEXT NOT NULL,
      data BYTEA NOT NULL,
      mime_type TEXT NOT NULL,
      sha256 TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(job_id, kind)
    )
  `);

  await db(`
    CREATE TABLE IF NOT EXISTS telegram_updates (
      update_id BIGINT PRIMARY KEY,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  console.log("Database initialized");
}

/* =========================
   TELEGRAM
========================= */

async function telegram(method, body = null) {
  const url =
    `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/${method}`;

  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body || {}),
  });

  const text = await response.text();

  if (!response.ok) {
    throw new Error(
      `Telegram ${method} ${response.status}: ${text}`
    );
  }

  let json;

  try {
    json = JSON.parse(text);
  } catch {
    throw new Error(
      `Telegram returned invalid JSON: ${text.slice(0, 500)}`
    );
  }

  if (!json.ok) {
    throw new Error(
      `Telegram ${method} failed: ${text}`
    );
  }

  return json.result;
}

async function sendMessage(chatId, text) {
  return telegram("sendMessage", {
    chat_id: chatId,
    text,
  });
}

async function sendDocument(
  chatId,
  buffer,
  filename,
  mimeType,
  caption = ""
) {
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

  const text = await response.text();

  if (!response.ok) {
    throw new Error(
      `Telegram sendDocument ${response.status}: ${text}`
    );
  }

  const json = JSON.parse(text);

  if (!json.ok) {
    throw new Error(
      `Telegram sendDocument failed: ${text}`
    );
  }

  return json.result;
}

/* =========================
   GEMINI REST
========================= */

async function geminiGenerate(model, prompt) {
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
        input: prompt,
      }),
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

  const result = JSON.parse(raw);

  const texts = [];

  if (Array.isArray(result?.steps)) {
    for (const step of result.steps) {
      if (!Array.isArray(step?.content)) continue;

      for (const item of step.content) {
        if (
          item?.type === "text" &&
          typeof item?.text === "string"
        ) {
          texts.push(item.text);
        }
      }
    }
  }

  if (Array.isArray(result?.outputs)) {
    for (const item of result.outputs) {
      if (
        item?.type === "text" &&
        typeof item?.text === "string"
      ) {
        texts.push(item.text);
      }
    }
  }

  if (
    typeof result?.output_text === "string"
  ) {
    texts.push(result.output_text);
  }

  const text = texts.join("\n").trim();

  if (!text) {
    throw new Error(
      `Gemini returned no text. keys=${Object.keys(result || {}).join(",")}`
    );
  }

  return text;
}

/* =========================
   SCRIPT GENERATION
========================= */

async function generateScript(topic) {
  const prompt = `
Create an original educational YouTube narration about:

${topic}

Requirements:
- General audience.
- Clear and engaging.
- Factual and useful.
- Natural spoken narration.
- No dangerous instructions.
- No graphic descriptions.
- No copyrighted text.
- Start directly with the topic.
- End naturally.
- Do not include title, labels, notes, or meta commentary.
- Return ONLY the narration.
`;

  let lastError;

  for (const model of SCRIPT_MODELS) {
    try {
      const script = await geminiGenerate(
        model,
        prompt
      );

      return {
        script,
        model,
      };
    } catch (error) {
      lastError = error;

      console.log(
        `Script model failed: ${model}`,
        error.message
      );

      if (
        ![429, 500, 502, 503].includes(
          error.status
        )
      ) {
        continue;
      }

      await sleep(1500);
    }
  }

  throw (
    lastError ||
    new Error("All script models failed")
  );
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
  maxWords = 180
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

async function generateTTSChunk(
  text,
  model
) {
  const controller =
    new AbortController();

  const timeoutMs = 120000;

  const timer = setTimeout(
    () => controller.abort(),
    timeoutMs
  );

  try {
    const language =
      /[\u0900-\u097F]/.test(
        String(text)
      )
        ? "hi-IN"
        : "en-US";

    const response = await fetch(
      "https://generativelanguage.googleapis.com/v1beta/interactions",
      {
        method: "POST",
        headers: {
          "Content-Type":
            "application/json",
          "x-goog-api-key":
            GEMINI_API_KEY,
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
                language,
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

    const stepContent =
      Array.isArray(result?.steps)
        ? result.steps.flatMap(
            (step) =>
              Array.isArray(
                step?.content
              )
                ? step.content
                : []
          )
        : [];

    const outputContent =
      Array.isArray(result?.outputs)
        ? result.outputs
        : [];

    const audioOutput =
      stepContent.find(
        (item) =>
          item?.type === "audio"
      ) ||
      outputContent.find(
        (item) =>
          item?.type === "audio"
      ) ||
      (
        result?.output_audio?.type ===
        "audio"
          ? result.output_audio
          : null
      );

    const audioData =
      audioOutput?.data;

    if (!audioData) {
      const stepTypes =
        stepContent
          .map(
            (item) =>
              item?.type
          )
          .filter(Boolean);

      throw new Error(
        `TTS returned no audio data. status=${result?.status || "unknown"}; step_types=${stepTypes.join(",") || "none"}; keys=${Object.keys(result || {}).join(",")}`
      );
    }

    const rawBuffer =
      Buffer.from(
        audioData,
        "base64"
      );

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
      (
        parsedRate
          ? Number(parsedRate[1])
          : 24000
      );

    const buffer =
      isWav(rawBuffer)
        ? rawBuffer
        : normalizedMimeType ===
          "audio/l16"
          ? pcmToWav(
              rawBuffer,
              sampleRate,
              1,
              16
            )
          : rawBuffer;

    if (!isWav(buffer)) {
      throw new Error(
        `TTS returned unsupported audio format: ${mimeType}`
      );
    }

    return {
      buffer,
      mimeType:
        "audio/wav",
      sampleRate,
    };
  } catch (error) {
    if (
      error.name ===
      "AbortError"
    ) {
      const e =
        new Error(
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
async function generateTTSWithRetry(
  text
) {
  let lastError;

  for (const model of TTS_MODELS) {
    for (
      let attempt = 1;
      attempt <= 3;
      attempt++
    ) {
      try {
        return {
          ...(await generateTTSChunk(
            text,
            model
          )),
          model,
        };
      } catch (error) {
        lastError = error;

        console.log(
          `TTS failed model=${model} attempt=${attempt} status=${error.status || "none"}: ${error.message}`
        );

        if (
          isContentBlockedError(error)
        ) {
          try {
            const safeText =
              await rewriteForSafeTTS(
                text
              );

            if (
              safeText &&
              safeText.trim() &&
              safeText.trim() !==
                text.trim()
            ) {
              return {
                ...(await generateTTSChunk(
                  safeText,
                  model
                )),
                model,
              };
            }
          } catch (rewriteError) {
            console.log(
              "Safe TTS rewrite failed:",
              rewriteError.message
            );
          }

          throw error;
        }

        const retryable =
          error.code === "TIMEOUT" ||
          [429, 500, 502, 503].includes(
            error.status
          );

        if (!retryable) {
          throw error;
        }

        if (attempt < 3) {
          await sleep(
            error.status === 429
              ? 5000 * attempt
              : 2000 +
                  Math.floor(
                    Math.random() * 2000
                  )
          );
        }
      }
    }
  }

  throw (
    lastError ||
    new Error(
      "All TTS models failed"
    )
  );
}

/* =========================
   CONTENT BLOCK
========================= */

function isContentBlockedError(
  error
) {
  const text = String(
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

  for (const model of SCRIPT_MODELS) {
    try {
      const rewritten =
        await geminiGenerate(
          model,
          prompt
        );

      if (
        rewritten &&
        rewritten.trim()
      ) {
        return rewritten.trim();
      }
    } catch (error) {
      console.log(
        "Safe rewrite model failed:",
        model,
        error.message
      );
    }
  }

  return null;
}

/* =========================
   WAV HELPERS
========================= */

function isWav(buffer) {
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
    (channels * bits) / 8;

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

function getWavPcm(wav) {
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
    sampleRate:
      wav.readUInt32LE(24),
    channels:
      wav.readUInt16LE(22),
    bits:
      wav.readUInt16LE(34),
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
      : pcmToWav(buffers[0]);

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

  return (
    result.rows[0] ||
    null
  );
}

async function updateJob(
  id,
  fields
) {
  const entries =
    Object.entries(fields);

  if (!entries.length) {
    return;
  }

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
  const storedChunkSize =
    Number(
      job.tts_chunk_size || 0
    );

  const maxWords =
    storedChunkSize > 0
      ? storedChunkSize
      : Number(
          job.tts_total_chunks ||
            0
        ) > 0
        ? 80
        : 180;

  const chunks =
    splitIntoChunks(
      job.script,
      maxWords
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
      tts_total_chunks:
        chunks.length,
      tts_chunk_size:
        maxWords,
      stage: "tts",
      progress: 55,
      status: "running",
      error: null,
    }
  );

  const pending = [];

  for (
    let i = 0;
    i < chunks.length;
    i++
  ) {
    const existing =
      await db(
        `
        SELECT
          audio_data,
          status
        FROM job_audio_chunks
        WHERE job_id=$1
          AND chunk_index=$2
        `,
        [job.id, i]
      );

    if (
      !(
        existing.rows[0]
          ?.status ===
          "completed" &&
        existing.rows[0]
          ?.audio_data
      )
    ) {
      pending.push(i);
    }
  }

  let completed =
    chunks.length -
    pending.length;

  await updateJob(
    job.id,
    {
      tts_completed_chunks:
        completed,
    }
  );

  let next = 0;
  let failure = null;

  const worker = async () => {
    while (true) {
      if (failure) return;

      const pos = next++;

      if (
        pos >=
        pending.length
      ) {
        return;
      }

      const i =
        pending[pos];

      await updateJob(
        job.id,
        {
          tts_current_chunk:
            i,
        }
      );

      try {
        const result =
          await generateTTSWithRetry(
            chunks[i]
          );

        await db(
          `
          INSERT INTO
            job_audio_chunks
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
          ON CONFLICT
            (job_id,chunk_index)
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
              NOW()
          `,
          [
            job.id,
            i,
            result.buffer,
            result.mimeType,
            result.model,
          ]
        );

        completed++;

        const progress =
          Math.min(
            85,
            55 +
              Math.floor(
                (
                  completed /
                  chunks.length
                ) * 30
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
          completed % 2 === 0
        ) {
          await sendMessage(
            targetChat,
            `🎙️ TTS progress: ${completed}/${chunks.length}\nProgress: ${progress}%`
          );
        }
      } catch (error) {
        failure = {
          index: i,
          error,
        };
      }
    }
  };

  await Promise.all(
    Array.from(
      {
        length: Math.min(
          3,
          Math.max(
            1,
            pending.length
          )
        ),
      },
      worker
    )
  );

  if (failure) {
    const {
      index,
      error,
    } = failure;

    await db(
      `
      INSERT INTO
        job_audio_chunks
      (
        job_id,
        chunk_index,
        status,
        error
      )
      VALUES
      (
        $1,$2,
        'failed',
        $3
      )
      ON CONFLICT
        (job_id,chunk_index)
      DO UPDATE SET
        status='failed',
        error=EXCLUDED.error,
        updated_at=NOW()
      `,
      [
        job.id,
        index,
        error.message,
      ]
    );

    await updateJob(
      job.id,
      {
        status: "paused",
        stage: "tts",
        error:
          error.message,
      }
    );

    await sendMessage(
      targetChat,
      `⏸️ JOB PAUSED SAFELY

TTS chunk ${index + 1}/${chunks.length} failed.
Completed chunks are saved.

Resume with:
/resume ${job.id}`
    );

    return null;
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
        (r) =>
          r.audio_data
      )
    );

  await updateJob(
    job.id,
    {
      tts_completed_chunks:
        chunks.length,
      progress: 85,
      stage:
        "tts_complete",
      error: null,
    }
  );

  await sendMessage(
    targetChat,
    `🎙️ TTS COMPLETED

${chunks.length} narration chunks generated.
Progress: 85%`
  );

  return {
    audio,
    chunks,
  };
}

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

function srtTime(seconds) {
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

  return `${String(h).padStart(2,"0")}:${String(m).padStart(2,"0")}:${String(s).padStart(2,"0")},${String(x).padStart(3,"0")}`;
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
        `${n++}\n${srtTime(a)} --> ${srtTime(b)}\n${line}\n`
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
  timeoutMs = 900000
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
      let settled = false;

      const finish = (
        fn,
        value
      ) => {
        if (settled) return;

        settled = true;

        clearTimeout(
          timer
        );

        fn(value);
      };

      const timer =
        setTimeout(
          () => {
            c.kill(
              "SIGKILL"
            );

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
        (d) => {
          err += d.toString();

          if (
            err.length >
            12000
          ) {
            err =
              err.slice(
                -12000
              );
          }
        }
      );

      c.on(
        "error",
        (e) =>
          finish(
            reject,
            e
          )
      );

      c.on(
        "close",
        (code) => {
          if (code === 0) {
            finish(
              resolve
            );
          } else {
            finish(
              reject,
              new Error(
                `FFmpeg failed (${code}): ${err.slice(-3000)}`
              )
            );
          }
        }
      );
    }
  );
}

/* =========================
   PERSISTED MEDIA
========================= */

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
    INSERT INTO
      job_media
    (
      job_id,
      kind,
      data,
      mime_type,
      sha256
    )
    VALUES
    (
      $1,$2,$3,$4,$5
    )
    ON CONFLICT
      (job_id,kind)
    DO UPDATE SET
      data =
        EXCLUDED.data,
      mime_type =
        EXCLUDED.mime_type,
      sha256 =
        EXCLUDED.sha256,
      updated_at =
        NOW()
    `,
    [
      jobId,
      kind,
      b,
      mimeType,
      sha,
    ]
  );
}

/* =========================
   FAST VIDEO BUILD
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
        (r) =>
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

    const escaped =
      srtPath.replace(
        /'/g,
        "\\'"
      );

    /*
      SPEED MODE:
      640x360
      24 FPS
      ultrafast
      CRF 32
      64k audio
    */

    await runFfmpeg(
      [
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",

        "-f",
        "lavfi",

        "-i",
        "color=c=black:s=640x360:r=24",

        "-i",
        audioPath,

        "-t",
        String(dur),

        "-vf",
        `subtitles='${escaped}':force_style='FontName=DejaVu Sans,FontSize=20,Alignment=2,MarginV=20'`,

        "-c:v",
        "libx264",

        "-preset",
        "ultrafast",

        "-crf",
        "32",

        "-pix_fmt",
        "yuv420p",

        "-c:a",
        "aac",

        "-b:a",
        "64k",

        videoPath,
      ],
      900000
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

    /*
      Thumbnail extraction is kept
      lightweight.
    */

    await runFfmpeg(
      [
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-ss",
        "1",
        "-i",
        videoPath,
        "-frames:v",
        "1",
        "-q:v",
        "5",
        thumbPath,
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
        chat_id=COALESCE(
          $2,
          chat_id
        ),
        error=NULL,
        updated_at=NOW()
      WHERE id=$1
        AND status='queued'
      RETURNING *
      `,
      [id, chatId]
    );

  if (!claim.rows[0]) {
    return false;
  }

  let job =
    claim.rows[0];

  const targetChat =
    chatId || job.chat_id;

  try {
    if (!job.script) {
      await updateJob(
        id,
        {
          stage:
            "script",
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
        stage:
          "video",
        progress: 88,
        status:
          "running",
      }
    );

    await sendMessage(
      targetChat,
      "🎬 Building FAST video + captions + thumbnail\nProgress: 88%"
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
          "delivery",
        progress: 95,
        status:
          "running",
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
        status:
          "paused",
        error:
          error.message,
      }
    ).catch(() => {});

    await sendMessage(
      targetChat,
      `⏸️ JOB PAUSED SAFELY

Reason: ${error.message}

Completed TTS/media are saved.

Resume with:
/resume ${id}`
    ).catch(() => {});

    return false;
  }
}

/* =========================
   SAFE JOB CONCURRENCY
========================= */

const MAX_CONCURRENT_JOBS =
  Math.max(
    1,
    Number(
      process.env
        .MAX_CONCURRENT_JOBS ||
        1
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
      (x) =>
        x.id === id
    )
  ) {
    return;
  }

  jobQueue.push({
    id,
    chatId,
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
        (error) =>
          console.error(
            `Background job ${item.id} error:`,
            error
          )
      )
      .finally(() => {
        activeJobIds.delete(
          item.id
        );

        drainJobQueue();
      });
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
      stage,
      tts_chunk_size
    )
    VALUES
    (
      $1,
      $2,
      $3,
      'queued',
      0,
      'queued',
      180
    )
    `,
    [
      id,
      topic,
      chatId,
    ]
  );

  await sendMessage(
    chatId,
    `🎬 JOB CREATED

Topic: ${topic}

Job ID: ${id}

⚡ Fast processing started...`
  );

  processJob(
    id,
    chatId
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
      [id, chatId]
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

⚡ Saved script, TTS chunks and media will be reused.`
  );

  processJob(
    id,
    chatId
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
          WHERE status='running'
        )::int AS running,

        COUNT(*) FILTER (
          WHERE status='paused'
        )::int AS paused,

        COUNT(*) FILTER (
          WHERE status='completed'
        )::int AS completed,

        COUNT(*) FILTER (
          WHERE status='queued'
        )::int AS queued,

        COUNT(*)::int AS total

      FROM jobs

      WHERE chat_id=$1
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

⚡ Fast video mode: ON
Payment mode: APPROVAL ONLY`
  );
}

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
            `
            INSERT INTO
              telegram_updates
            (
              update_id
            )
            VALUES
            ($1)
            ON CONFLICT DO NOTHING
            RETURNING update_id
            `,
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

      /*
        Respond immediately to Telegram.
        Job work happens in background.
      */

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

      /* =====================
         /start
      ===================== */

      if (
        text ===
        "/start"
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

      /* =====================
         /status
      ===================== */

      if (
        text ===
        "/status"
      ) {
        await status(
          chatId
        );

        return;
      }

      /* =====================
         /create
      ===================== */

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

      /* =====================
         /resume
      ===================== */

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

      /* =====================
         UNKNOWN
      ===================== */

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
   HEALTH
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
      mode:
        "FAST",
    });
  }
);

app.get(
  "/health",
  (
    req,
    res
  ) => {
    res.json({
      ok: true,
      status:
        "healthy",
      service:
        "AI YouTube Autopilot",
    });
  }
);

/* =========================
   STARTUP
========================= */

async function startup() {
  try {
    await initDatabase();

    /* =====================
       TELEGRAM WEBHOOK
    ===================== */

    if (
      WEBHOOK_URL &&
      TELEGRAM_BOT_TOKEN
    ) {
      console.log(
        "Setting Telegram webhook:",
        WEBHOOK_URL
      );

      const webhookBody =
        {
          url:
            WEBHOOK_URL,

          allowed_updates:
            [
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

    /* =====================
       RECOVERY
    ===================== */

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

    /* =====================
       SERVER
    ===================== */

    app.listen(
      PORT,
      () => {
        console.log(
          `AI YouTube Autopilot listening on port ${PORT}`
        );

        console.log(
          "STARTUP COMPLETE"
        );

        console.log(
          `MAX_CONCURRENT_JOBS=${MAX_CONCURRENT_JOBS}`
        );

        console.log(
          "FAST VIDEO MODE ENABLED"
        );

        /*
          Resume queued jobs.
          Queue itself enforces the
          concurrency limit.
        */

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
                  LIMIT 20
                  `
                );

              for (
                const row
                of queued.rows
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
      .catch(() => {});

    process.exit(0);
  }
);

process.on(
  "SIGINT",
  async () => {
    await pool
      .end()
      .catch(() => {});

    process.exit(0);
  }
);

/* =========================
   START
========================= */

startup();
