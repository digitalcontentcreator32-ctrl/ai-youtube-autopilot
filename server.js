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

const TTS_MODELS = [
  "gemini-3.1-flash-tts-preview",
  "gemini-2.5-flash-preview-tts",
  "gemini-2.5-pro-preview-tts",
];

const WORK_DIR = path.join(os.tmpdir(), "ai-youtube-autopilot");

await fs.mkdir(WORK_DIR, { recursive: true });

async function db(sql, params = []) {
  const client = await pool.connect();

  try {
    return await client.query(sql, params);
  } finally {
    client.release();
  }
}

async function initDatabase() {
  await db(`
    CREATE TABLE IF NOT EXISTS jobs (
      id BIGSERIAL PRIMARY KEY,
      chat_id TEXT,
      topic TEXT,
      status TEXT DEFAULT 'queued',
      script TEXT,
      tts_total_chunks INTEGER DEFAULT 0,
      tts_completed_chunks INTEGER DEFAULT 0,
      tts_chunk_size INTEGER DEFAULT 0,
      video_path TEXT,
      thumbnail_path TEXT,
      captions_path TEXT,
      error TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await db(`
    ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS chat_id TEXT
  `);

  await db(`
    ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS tts_chunk_size INTEGER DEFAULT 0
  `);

  await db(`
    CREATE TABLE IF NOT EXISTS job_media (
      id BIGSERIAL PRIMARY KEY,
      job_id BIGINT REFERENCES jobs(id) ON DELETE CASCADE,
      media_type TEXT,
      file_path TEXT,
      sha256 TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
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
    CREATE TABLE IF NOT EXISTS job_audio_chunks (
      id BIGSERIAL PRIMARY KEY,
      job_id BIGINT REFERENCES jobs(id) ON DELETE CASCADE,
      chunk_index INTEGER NOT NULL,
      text TEXT NOT NULL,
      audio_path TEXT,
      status TEXT DEFAULT 'pending',
      error TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(job_id, chunk_index)
    )
  `);

  await db(`
    CREATE TABLE IF NOT EXISTS telegram_updates (
      update_id BIGINT PRIMARY KEY,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
}

function telegramUrl(method) {
  return `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/${method}`;
}

async function telegram(method, body) {
  const response = await fetch(telegramUrl(method), {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify(body),
  });

  const data = await response.json();

  if (!response.ok || !data.ok) {
    throw new Error(
      `Telegram ${method} failed: ${JSON.stringify(data)}`
    );
  }

  return data.result;
}

async function sendMessage(chatId, text) {
  return telegram("sendMessage", {
    chat_id: chatId,
    text,
  });
}

async function sendDocument(chatId, filePath, caption = "") {
  const form = new FormData();

  const fileBuffer = await fs.readFile(filePath);

  form.append(
    "chat_id",
    String(chatId)
  );

  form.append(
    "document",
    new Blob([fileBuffer]),
    path.basename(filePath)
  );

  if (caption) {
    form.append("caption", caption);
  }

  const response = await fetch(
    telegramUrl("sendDocument"),
    {
      method: "POST",
      body: form,
    }
  );

  const data = await response.json();

  if (!response.ok || !data.ok) {
    throw new Error(
      `Telegram sendDocument failed: ${JSON.stringify(data)}`
    );
  }

  return data.result;
}

async function sendPhoto(chatId, filePath, caption = "") {
  const form = new FormData();

  const fileBuffer = await fs.readFile(filePath);

  form.append(
    "chat_id",
    String(chatId)
  );

  form.append(
    "photo",
    new Blob([fileBuffer]),
    path.basename(filePath)
  );

  if (caption) {
    form.append("caption", caption);
  }

  const response = await fetch(
    telegramUrl("sendPhoto"),
    {
      method: "POST",
      body: form,
    }
  );

  const data = await response.json();

  if (!response.ok || !data.ok) {
    throw new Error(
      `Telegram sendPhoto failed: ${JSON.stringify(data)}`
    );
  }

  return data.result;
}

async function geminiRequest(model, input, extra = {}) {
  const response = await fetch(
    "https://generativelanguage.googleapis.com/v1beta/interactions",
    {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-goog-api-key": GEMINI_API_KEY,
      },
      body: JSON.stringify({
        model,
        input,
        ...extra,
      }),
    }
  );

  const data = await response.json();

  if (!response.ok) {
    throw new Error(
      `Gemini ${response.status}: ${
        data?.error?.message ||
        JSON.stringify(data)
      }`
    );
  }

  return data;
}

function extractTextFromGemini(data) {
  if (typeof data?.output_text === "string") {
    return data.output_text;
  }

  if (typeof data?.text === "string") {
    return data.text;
  }

  const outputs = Array.isArray(data?.outputs)
    ? data.outputs
    : [];

  for (const output of outputs) {
    if (typeof output?.text === "string") {
      return output.text;
    }

    const content = Array.isArray(output?.content)
      ? output.content
      : [];

    for (const item of content) {
      if (typeof item?.text === "string") {
        return item.text;
      }
    }
  }

  const steps = Array.isArray(data?.steps)
    ? data.steps
    : [];

  for (const step of steps) {
    const content = Array.isArray(step?.content)
      ? step.content
      : [];

    for (const item of content) {
      if (typeof item?.text === "string") {
        return item.text;
      }
    }
  }

  return "";
}

async function generateScript(topic) {
  const prompt = `
Create a YouTube narration script about:

${topic}

Requirements:
- General audience
- Interesting and engaging
- Factually careful
- Natural voice-over style
- No headings inside the narration
- No markdown
- No stage directions
- Around 600-800 words
- Start with a strong hook
- End naturally

Return ONLY the narration script.
`;

  let lastError = null;

  for (const model of SCRIPT_MODELS) {
    try {
      const data = await geminiRequest(
        model,
        prompt
      );

      const text =
        extractTextFromGemini(data).trim();

      if (text) {
        return {
          model,
          text,
        };
      }
    } catch (error) {
      lastError = error;
    }
  }

  throw lastError ||
    new Error("Script generation failed");
}

function wordCount(text) {
  return String(text)
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .length;
}

function qualityCheck(script) {
  const count = wordCount(script);

  if (count < 150) {
    throw new Error(
      `Generated script is too short: ${count} words`
    );
  }

  return count;
}

function splitIntoChunks(text, maxWords = 180) {
  const words = String(text)
    .trim()
    .split(/\s+/)
    .filter(Boolean);

  const chunks = [];

  for (
    let i = 0;
    i < words.length;
    i += maxWords
  ) {
    chunks.push(
      words.slice(i, i + maxWords).join(" ")
    );
  }

  return chunks;
}

function isContentBlockedError(error) {
  const text = String(
    error?.message || ""
  ).toLowerCase();

  return (
    text.includes("content_blocked") ||
    text.includes("request blocked") ||
    text.includes("policy reason")
  );
}

async function rewriteForSafeTTS(text) {
  const prompt = `
Rewrite the following narration for a general-audience educational YouTube voice-over.

Keep the factual meaning and topic, but use calm, neutral, non-graphic wording.
Remove or replace sensitive or potentially policy-triggering details.
Do not add new facts.
Return ONLY the rewritten narration.

TEXT:
${text}
`;

  let lastError = null;

  for (const model of SCRIPT_MODELS) {
    try {
      const data = await geminiRequest(
        model,
        prompt
      );

      const rewritten =
        extractTextFromGemini(data).trim();

      if (rewritten) {
        return rewritten;
      }
    } catch (error) {
      lastError = error;
    }
  }

  throw lastError ||
    new Error("Safe TTS rewrite failed");
}

async function generateTTS(text) {
  let lastError = null;

  for (const model of TTS_MODELS) {
    try {
      const data = await geminiRequest(
        model,
        text,
        {
          response_format: {
            type: "audio",
          },
          generation_config: {
            speech_config: [
              {
                voice: "Kore",
                language:
                  /[\u0900-\u097F]/.test(
                    String(text)
                  )
                    ? "hi-IN"
                    : "en-US",
              },
            ],
          },
        }
      );

      return data;
    } catch (error) {
      lastError = error;
    }
  }

  throw lastError ||
    new Error("TTS generation failed");
}
function getAudioFromGemini(data) {
  const outputs = Array.isArray(data?.outputs)
    ? data.outputs
    : [];

  for (const output of outputs) {
    if (output?.audio?.data) {
      return {
        data: output.audio.data,
        mimeType:
          output.audio.mime_type ||
          output.audio.mimeType ||
          "audio/wav",
        sampleRate:
          output.audio.sample_rate ||
          output.audio.sampleRate ||
          24000,
      };
    }

    if (output?.content) {
      const content = Array.isArray(output.content)
        ? output.content
        : [output.content];

      for (const item of content) {
        if (item?.audio?.data) {
          return {
            data: item.audio.data,
            mimeType:
              item.audio.mime_type ||
              item.audio.mimeType ||
              "audio/wav",
            sampleRate:
              item.audio.sample_rate ||
              item.audio.sampleRate ||
              24000,
          };
        }
      }
    }
  }

  if (data?.output_audio?.data) {
    return {
      data: data.output_audio.data,
      mimeType:
        data.output_audio.mime_type ||
        data.output_audio.mimeType ||
        "audio/wav",
      sampleRate:
        data.output_audio.sample_rate ||
        data.output_audio.sampleRate ||
        24000,
    };
  }

  return null;
}

function isWav(buffer) {
  return (
    Buffer.isBuffer(buffer) &&
    buffer.length >= 12 &&
    buffer.subarray(0, 4).toString() === "RIFF" &&
    buffer.subarray(8, 12).toString() === "WAVE"
  );
}

function pcmToWav(
  pcmBuffer,
  sampleRate = 24000,
  channels = 1,
  bitsPerSample = 16
) {
  const blockAlign =
    channels * (bitsPerSample / 8);

  const byteRate =
    sampleRate * blockAlign;

  const wav = Buffer.alloc(
    44 + pcmBuffer.length
  );

  wav.write("RIFF", 0);
  wav.writeUInt32LE(
    36 + pcmBuffer.length,
    4
  );
  wav.write("WAVE", 8);

  wav.write("fmt ", 12);
  wav.writeUInt32LE(16, 16);
  wav.writeUInt16LE(1, 20);
  wav.writeUInt16LE(channels, 22);
  wav.writeUInt32LE(sampleRate, 24);
  wav.writeUInt32LE(byteRate, 28);
  wav.writeUInt16LE(blockAlign, 32);
  wav.writeUInt16LE(bitsPerSample, 34);

  wav.write("data", 36);
  wav.writeUInt32LE(
    pcmBuffer.length,
    40
  );

  pcmBuffer.copy(wav, 44);

  return wav;
}

function decodeBase64Audio(base64) {
  if (!base64) {
    throw new Error(
      "Gemini TTS returned empty audio data"
    );
  }

  return Buffer.from(base64, "base64");
}

async function saveTTSChunkAudio(
  jobId,
  chunkIndex,
  audioBuffer
) {
  const jobDir = path.join(
    WORK_DIR,
    `job-${jobId}`
  );

  await fs.mkdir(jobDir, {
    recursive: true,
  });

  const audioPath = path.join(
    jobDir,
    `chunk-${chunkIndex}.wav`
  );

  await fs.writeFile(
    audioPath,
    audioBuffer
  );

  return audioPath;
}

async function synthesizeChunk(text) {
  let data;

  try {
    data = await generateTTS(text);
  } catch (error) {
    if (isContentBlockedError(error)) {
      const safeText =
        await rewriteForSafeTTS(text);

      data = await generateTTS(
        safeText
      );
    } else {
      throw error;
    }
  }

  const audio = getAudioFromGemini(data);

  if (!audio) {
    throw new Error(
      "Gemini TTS response contained no audio"
    );
  }

  const rawBuffer =
    decodeBase64Audio(audio.data);

  const normalizedMimeType =
    String(audio.mimeType)
      .split(";")[0]
      .trim()
      .toLowerCase();

  const parsedRate =
    String(audio.mimeType).match(
      /(?:rate|sample[_-]?rate)\s*=\s*(\d+)/i
    );

  const sampleRate =
    audio.sampleRate ||
    (parsedRate
      ? Number(parsedRate[1])
      : 24000);

  const buffer = isWav(rawBuffer)
    ? rawBuffer
    : normalizedMimeType === "audio/l16"
      ? pcmToWav(
          rawBuffer,
          sampleRate,
          1,
          16
        )
      : rawBuffer;

  if (!isWav(buffer)) {
    throw new Error(
      `Unsupported TTS audio format: ${normalizedMimeType}`
    );
  }

  return buffer;
}

function sleep(ms) {
  return new Promise(
    resolve => setTimeout(resolve, ms)
  );
}

async function synthesizeChunkWithRetry(
  text,
  attempts = 3
) {
  let lastError = null;

  for (
    let attempt = 1;
    attempt <= attempts;
    attempt++
  ) {
    try {
      return await synthesizeChunk(text);
    } catch (error) {
      lastError = error;

      if (isContentBlockedError(error)) {
        throw error;
      }

      if (attempt < attempts) {
        await sleep(
          800 * attempt
        );
      }
    }
  }

  throw lastError ||
    new Error("TTS retry failed");
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

async function updateJob(
  jobId,
  fields
) {
  const entries =
    Object.entries(fields);

  if (!entries.length) {
    return getJob(jobId);
  }

  const values = [];
  const sets = [];

  let index = 1;

  for (const [key, value] of entries) {
    sets.push(
      `"${key}" = $${index}`
    );
    values.push(value);
    index++;
  }

  values.push(jobId);

  const result = await db(
    `
      UPDATE jobs
      SET ${sets.join(", ")},
          updated_at = NOW()
      WHERE id = $${index}
      RETURNING *
    `,
    values
  );

  return result.rows[0] || null;
}

async function claimJob(jobId) {
  const result = await db(
    `
      UPDATE jobs
      SET status = 'processing',
          updated_at = NOW()
      WHERE id = $1
        AND status IN (
          'queued',
          'paused',
          'processing'
        )
      RETURNING *
    `,
    [jobId]
  );

  return result.rows[0] || null;
}

async function getLatestJobForChat(
  chatId
) {
  const result = await db(
    `
      SELECT *
      FROM jobs
      WHERE chat_id = $1
      ORDER BY id DESC
      LIMIT 1
    `,
    [String(chatId)]
  );

  return result.rows[0] || null;
}

async function getPausedJobForChat(
  chatId,
  jobId = null
) {
  if (jobId) {
    const result = await db(
      `
        SELECT *
        FROM jobs
        WHERE id = $1
          AND chat_id = $2
          AND status = 'paused'
        LIMIT 1
      `,
      [
        jobId,
        String(chatId),
      ]
    );

    return result.rows[0] || null;
  }

  const result = await db(
    `
      SELECT *
      FROM jobs
      WHERE chat_id = $1
        AND status = 'paused'
      ORDER BY id DESC
      LIMIT 1
    `,
    [String(chatId)]
  );

  return result.rows[0] || null;
}

async function getAudioChunks(
  jobId
) {
  const result = await db(
    `
      SELECT *
      FROM job_audio_chunks
      WHERE job_id = $1
      ORDER BY chunk_index ASC
    `,
    [jobId]
  );

  return result.rows;
}

async function getAudioChunk(
  jobId,
  chunkIndex
) {
  const result = await db(
    `
      SELECT *
      FROM job_audio_chunks
      WHERE job_id = $1
        AND chunk_index = $2
      LIMIT 1
    `,
    [jobId, chunkIndex]
  );

  return result.rows[0] || null;
}

async function upsertAudioChunk(
  jobId,
  chunkIndex,
  text,
  audioPath,
  status,
  error = null
) {
  await db(
    `
      INSERT INTO job_audio_chunks (
        job_id,
        chunk_index,
        text,
        audio_path,
        status,
        error,
        updated_at
      )
      VALUES (
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        NOW()
      )
      ON CONFLICT (
        job_id,
        chunk_index
      )
      DO UPDATE SET
        text = EXCLUDED.text,
        audio_path = EXCLUDED.audio_path,
        status = EXCLUDED.status,
        error = EXCLUDED.error,
        updated_at = NOW()
    `,
    [
      jobId,
      chunkIndex,
      text,
      audioPath,
      status,
      error,
    ]
  );
    }
async function processTTSJob(jobId, chatId) {
  const job = await getJob(jobId);

  if (!job) {
    throw new Error(`Job ${jobId} not found`);
  }

  if (!job.script) {
    throw new Error("Job has no generated script");
  }

  const chunkSize =
    job.tts_total_chunks > 0 &&
    !job.tts_chunk_size
      ? 80
      : 180;

  const chunks = splitIntoChunks(
    job.script,
    chunkSize
  );

  await updateJob(jobId, {
    tts_total_chunks: chunks.length,
    tts_chunk_size: chunkSize,
    status: "processing",
    error: null,
  });

  let completed = 0;

  const existing =
    await getAudioChunks(jobId);

  for (const item of existing) {
    if (
      item.status === "completed" &&
      item.audio_path
    ) {
      try {
        await fs.access(item.audio_path);
        completed++;
      } catch {
        await upsertAudioChunk(
          jobId,
          item.chunk_index,
          item.text,
          null,
          "pending",
          null
        );
      }
    }
  }

  await updateJob(jobId, {
    tts_completed_chunks: completed,
  });

  const pendingIndexes = [];

  for (
    let i = 0;
    i < chunks.length;
    i++
  ) {
    const existingChunk =
      await getAudioChunk(jobId, i);

    if (
      existingChunk?.status === "completed" &&
      existingChunk.audio_path
    ) {
      try {
        await fs.access(
          existingChunk.audio_path
        );
        continue;
      } catch {
        // Recreate missing audio.
      }
    }

    pendingIndexes.push(i);
  }

  let cursor = 0;

  async function worker() {
    while (true) {
      const position = cursor++;

      if (
        position >=
        pendingIndexes.length
      ) {
        return;
      }

      const chunkIndex =
        pendingIndexes[position];

      const text = chunks[chunkIndex];

      try {
        const audioBuffer =
          await synthesizeChunkWithRetry(
            text
          );

        const audioPath =
          await saveTTSChunkAudio(
            jobId,
            chunkIndex,
            audioBuffer
          );

        await upsertAudioChunk(
          jobId,
          chunkIndex,
          text,
          audioPath,
          "completed",
          null
        );

        completed++;

        await updateJob(jobId, {
          tts_completed_chunks:
            completed,
        });

        if (
          completed === 1 ||
          completed === chunks.length ||
          completed % 3 === 0
        ) {
          await sendMessage(
            chatId,
            `TTS: ${completed}/${chunks.length} chunks completed.`
          );
        }
      } catch (error) {
        await upsertAudioChunk(
          jobId,
          chunkIndex,
          text,
          null,
          "failed",
          String(
            error?.message || error
          )
        );

        throw error;
      }
    }
  }

  const workers = Math.min(
    3,
    Math.max(
      1,
      pendingIndexes.length
    )
  );

  try {
    await Promise.all(
      Array.from(
        { length: workers },
        () => worker()
      )
    );
  } catch (error) {
    await updateJob(jobId, {
      status: "paused",
      error: String(
        error?.message || error
      ),
    });

    await sendMessage(
      chatId,
      `Job #${jobId} फिलहाल pause हो गया है।\n\nजो TTS chunks बन चुके हैं वे सुरक्षित हैं।\n\n/resume ${jobId} भेजकर वहीं से जारी कर सकते हो।`
    );

    throw error;
  }

  const finalChunks =
    await getAudioChunks(jobId);

  const audioPaths =
    finalChunks
      .filter(
        item =>
          item.status === "completed" &&
          item.audio_path
      )
      .sort(
        (a, b) =>
          a.chunk_index -
          b.chunk_index
      )
      .map(
        item => item.audio_path
      );

  if (
    audioPaths.length !==
    chunks.length
  ) {
    throw new Error(
      `TTS incomplete: ${audioPaths.length}/${chunks.length}`
    );
  }

  return audioPaths;
}

async function concatenateAudio(
  audioPaths,
  outputPath
) {
  if (!audioPaths.length) {
    throw new Error(
      "No audio chunks to concatenate"
    );
  }

  const listPath =
    path.join(
      path.dirname(outputPath),
      "audio-list.txt"
    );

  const content =
    audioPaths
      .map(
        filePath =>
          `file '${String(filePath).replaceAll("'", "'\\''")}'`
      )
      .join("\n");

  await fs.writeFile(
    listPath,
    content,
    "utf8"
  );

  await runFFmpeg([
    "-y",
    "-f",
    "concat",
    "-safe",
    "0",
    "-i",
    listPath,
    "-c:a",
    "aac",
    "-b:a",
    "96k",
    outputPath,
  ]);

  return outputPath;
}

function runFFmpeg(args) {
  return new Promise(
    (resolve, reject) => {
      const child = spawn(
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
        data => {
          stdout += data.toString();
        }
      );

      child.stderr.on(
        "data",
        data => {
          stderr += data.toString();
        }
      );

      child.on(
        "error",
        reject
      );

      child.on(
        "close",
        code => {
          if (code === 0) {
            resolve({
              stdout,
              stderr,
            });
          } else {
            reject(
              new Error(
                `FFmpeg exited with code ${code}: ${stderr.slice(-4000)}`
              )
            );
          }
        }
      );
    }
  );
}

function formatSrtTime(seconds) {
  const safeSeconds =
    Math.max(
      0,
      Number(seconds) || 0
    );

  const hours =
    Math.floor(
      safeSeconds / 3600
    );

  const minutes =
    Math.floor(
      (safeSeconds % 3600) / 60
    );

  const secs =
    Math.floor(
      safeSeconds % 60
    );

  const millis =
    Math.floor(
      (safeSeconds % 1) * 1000
    );

  return [
    String(hours).padStart(2, "0"),
    String(minutes).padStart(2, "0"),
    String(secs).padStart(2, "0"),
  ].join(":") +
    "," +
    String(millis).padStart(
      3,
      "0"
    );
}

function createSrt(script) {
  const chunks =
    splitIntoChunks(
      script,
      40
    );

  const lines = [];

  let currentTime = 0;

  for (
    let i = 0;
    i < chunks.length;
    i++
  ) {
    const text = chunks[i];

    const duration =
      Math.max(
        2,
        wordCount(text) / 2.4
      );

    const start =
      currentTime;

    const end =
      currentTime + duration;

    lines.push(
      String(i + 1)
    );

    lines.push(
      `${formatSrtTime(start)} --> ${formatSrtTime(end)}`
    );

    lines.push(text);
    lines.push("");

    currentTime = end;
  }

  return lines.join("\n");
}

async function createVideo(
  audioPath,
  videoPath
) {
  await runFFmpeg([
    "-y",
    "-f",
    "lavfi",
    "-i",
    "color=c=black:s=640x360:r=10",
    "-i",
    audioPath,
    "-shortest",
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
    "-c:a",
    "aac",
    "-b:a",
    "64k",
    videoPath,
  ]);

  return videoPath;
}

async function createThumbnail(
  videoPath,
  thumbnailPath
) {
  await runFFmpeg([
    "-y",
    "-ss",
    "1",
    "-i",
    videoPath,
    "-frames:v",
    "1",
    "-vf",
    "scale=640:360",
    thumbnailPath,
  ]);

  return thumbnailPath;
}

async function saveMedia(
  jobId,
  mediaType,
  filePath
) {
  const buffer =
    await fs.readFile(filePath);

  const sha256 =
    crypto
      .createHash("sha256")
      .update(buffer)
      .digest("hex");

  await db(
    `
      INSERT INTO job_media (
        job_id,
        media_type,
        file_path,
        sha256,
        created_at,
        updated_at
      )
      VALUES (
        $1,
        $2,
        $3,
        $4,
        NOW(),
        NOW()
      )
    `,
    [
      jobId,
      mediaType,
      filePath,
      sha256,
    ]
  );

  return {
    filePath,
    sha256,
  };
}

async function buildJobMedia(
  jobId,
  audioPaths,
  script
) {
  const jobDir =
    path.join(
      WORK_DIR,
      `job-${jobId}`
    );

  await fs.mkdir(
    jobDir,
    {
      recursive: true,
    }
  );

  const finalAudio =
    path.join(
      jobDir,
      "narration.m4a"
    );

  const videoPath =
    path.join(
      jobDir,
      "video.mp4"
    );

  const thumbnailPath =
    path.join(
      jobDir,
      "thumbnail.jpg"
    );

  const captionsPath =
    path.join(
      jobDir,
      "captions.srt"
    );

  await concatenateAudio(
    audioPaths,
    finalAudio
  );

  await createVideo(
    finalAudio,
    videoPath
  );

  await createThumbnail(
    videoPath,
    thumbnailPath
  );

  await fs.writeFile(
    captionsPath,
    createSrt(script),
    "utf8"
  );

  await saveMedia(
    jobId,
    "audio",
    finalAudio
  );

  await saveMedia(
    jobId,
    "video",
    videoPath
  );

  await saveMedia(
    jobId,
    "thumbnail",
    thumbnailPath
  );

  await saveMedia(
    jobId,
    "captions",
    captionsPath
  );

  await updateJob(jobId, {
    video_path: videoPath,
    thumbnail_path: thumbnailPath,
    captions_path: captionsPath,
  });

  return {
    audioPath: finalAudio,
    videoPath,
    thumbnailPath,
    captionsPath,
  };
}

async function fileSize(
  filePath
) {
  const stat =
    await fs.stat(filePath);

  return stat.size;
}

async function ensureTelegramFileSize(
  filePath
) {
  const size =
    await fileSize(filePath);

  const maxSize =
    49 * 1024 * 1024;

  if (size > maxSize) {
    throw new Error(
      `Video is too large for Telegram delivery: ${Math.round(size / 1024 / 1024)}MB`
    );
  }

  return size;
}

const jobQueue = new Map();
let queuePumpRunning = false;

function enqueueJob(
  jobId,
  chatId
) {
  jobQueue.set(
    String(jobId),
    {
      jobId,
      chatId,
    }
  );

  pumpQueue();
}

async function pumpQueue() {
  if (queuePumpRunning) {
    return;
  }

  queuePumpRunning = true;

  try {
    for (;;) {
      const first =
        jobQueue.entries().next();

      if (first.done) {
        break;
      }

      const [
        key,
        item,
      ] = first.value;

      jobQueue.delete(key);

      try {
        await runJobOnce(
          item.jobId,
          item.chatId
        );
      } catch (error) {
        console.error(
          "Queued job failed:",
          error
        );
      }
    }
  } finally {
    queuePumpRunning = false;

    if (jobQueue.size) {
      pumpQueue();
    }
  }
    }
/* =========================
   FULL JOB
========================= */

async function runJobOnce(jobId, chatId = null) {
  const claimed = await db(
    `
      UPDATE jobs
      SET
        status = 'processing',
        chat_id = COALESCE($2, chat_id),
        error = NULL,
        updated_at = NOW()
      WHERE id = $1
        AND status = 'queued'
      RETURNING *
    `,
    [jobId, chatId]
  );

  if (!claimed.rows[0]) {
    return false;
  }

  let job = claimed.rows[0];
  const targetChat = chatId || job.chat_id;

  try {
    if (!job.script) {
      await updateJob(jobId, {
        status: "processing",
      });

      await sendMessage(
        targetChat,
        "📝 Generating original script..."
      );

      const generated =
        await generateScript(job.topic);

      const wc =
        qualityCheck(generated.text);

      await updateJob(jobId, {
        script: generated.text,
        status: "processing",
      });

      await sendMessage(
        targetChat,
        `📝 Script generated — ${generated.model}\n\n✅ Quality check passed — ${wc} words`
      );

      job = await getJob(jobId);
    }

    await sendMessage(
      targetChat,
      "🎙️ Starting AI narration..."
    );

    const audioPaths =
      await processTTSJob(
        jobId,
        targetChat
      );

    job = await getJob(jobId);

    if (!audioPaths?.length) {
      throw new Error(
        "No narration audio was generated"
      );
    }

    await updateJob(jobId, {
      status: "processing",
    });

    await sendMessage(
      targetChat,
      "🎬 Building video, thumbnail and captions..."
    );

    const media =
      await buildJobMedia(
        jobId,
        audioPaths,
        job.script
      );

    await ensureTelegramFileSize(
      media.videoPath
    );

    await updateJob(jobId, {
      video_path: media.videoPath,
      thumbnail_path:
        media.thumbnailPath,
      captions_path:
        media.captionsPath,
      status: "processing",
    });

    await sendMessage(
      targetChat,
      "📦 Video package ready. Sending files..."
    );

    await sendDocument(
      targetChat,
      media.videoPath,
      `🎬 AI YouTube video completed\nJob: ${jobId}`
    );

    await sendPhoto(
      targetChat,
      media.thumbnailPath,
      `🖼️ Thumbnail\nJob: ${jobId}`
    );

    await sendDocument(
      targetChat,
      media.captionsPath,
      `📝 Captions\nJob: ${jobId}`
    );

    await updateJob(jobId, {
      status: "completed",
      error: null,
    });

    await sendMessage(
      targetChat,
      `✅ JOB COMPLETED — 100%\n\n🎬 Video + thumbnail + captions delivered.\n\nJob: ${jobId}`
    );

    return true;

  } catch (error) {
    console.error(
      `Job ${jobId} failed:`,
      error
    );

    await updateJob(
      jobId,
      {
        status: "paused",
        error:
          String(
            error?.message || error
          ),
      }
    ).catch(() => {});

    await sendMessage(
      targetChat,
      `⏸️ JOB PAUSED SAFELY\n\nReason: ${String(
        error?.message || error
      )}\n\nCompleted TTS chunks are saved.\n\nResume with:\n/resume ${jobId}`
    ).catch(() => {});

    return false;
  }
}

async function processJob(
  jobId,
  chatId = null
) {
  return runJobOnce(
    jobId,
    chatId
  );
}

/* =========================
   CREATE JOB
========================= */

async function createJob(
  topic,
  chatId
) {
  const result = await db(
    `
      INSERT INTO jobs (
        topic,
        chat_id,
        status
      )
      VALUES (
        $1,
        $2,
        'queued'
      )
      RETURNING id
    `,
    [
      topic,
      String(chatId),
    ]
  );

  const jobId =
    result.rows[0].id;

  await sendMessage(
    chatId,
    `🎬 JOB CREATED\n\nTopic: ${topic}\n\nJob ID: ${jobId}`
  );

  enqueueJob(
    jobId,
    chatId
  );

  return jobId;
}

/* =========================
   RESUME JOB
========================= */

async function resumeJob(
  jobId,
  chatId
) {
  const job =
    await getJob(jobId);

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
    job.status === "completed"
  ) {
    await sendMessage(
      chatId,
      "✅ This job is already completed."
    );
    return;
  }

  if (
    job.status === "processing"
  ) {
    await sendMessage(
      chatId,
      "ℹ️ This job is already running."
    );
    return;
  }

  if (
    job.status === "queued"
  ) {
    await sendMessage(
      chatId,
      "ℹ️ This job is already queued."
    );
    return;
  }

  if (
    job.status !== "paused"
  ) {
    await sendMessage(
      chatId,
      `ℹ️ Cannot resume job from status: ${job.status}`
    );
    return;
  }

  const resumed =
    await db(
      `
        UPDATE jobs
        SET
          status = 'queued',
          chat_id = $2,
          error = NULL,
          updated_at = NOW()
        WHERE id = $1
          AND status = 'paused'
        RETURNING id
      `,
      [
        jobId,
        String(chatId),
      ]
    );

  if (!resumed.rows[0]) {
    await sendMessage(
      chatId,
      "ℹ️ Resume already started or job state changed."
    );
    return;
  }

  await sendMessage(
    chatId,
    `▶️ RESUMING JOB\n\nJob: ${jobId}\n\nSaved script and completed TTS chunks will be reused.`
  );

  enqueueJob(
    jobId,
    chatId
  );
}

/* =========================
   STATUS
========================= */

async function status(chatId) {
  const result =
    await db(
      `
        SELECT
          COUNT(*) FILTER (
            WHERE status = 'processing'
          )::int AS processing,

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
      [String(chatId)]
    );

  const s =
    result.rows[0];

  await sendMessage(
    chatId,
    `🤖 AI YouTube Autopilot

Backend: ONLINE ✅

Processing: ${s.processing}
Queued: ${s.queued}
Paused: ${s.paused}
Completed: ${s.completed}
Total: ${s.total}`
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
        Number.isSafeInteger(updateId)
      ) {
        const inserted =
          await db(
            `
              INSERT INTO telegram_updates (
                update_id
              )
              VALUES ($1)
              ON CONFLICT DO NOTHING
              RETURNING update_id
            `,
            [updateId]
          );

        if (!inserted.rows[0]) {
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

      if (text === "/start") {
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

      if (text === "/status") {
        await status(chatId);
        return;
      }

      if (
        text.startsWith("/create ")
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
        text.startsWith("/resume ")
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
      status: "online",
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
          "message",
        ],
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
            status = 'queued',
            updated_at = NOW()
          WHERE status = 'processing'
          RETURNING id
        `
      );

    console.log(
      `Recovered ${recovery.rows.length} interrupted job(s)`
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
                enqueueJob(
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
