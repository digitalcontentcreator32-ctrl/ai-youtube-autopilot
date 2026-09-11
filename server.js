import express from "express";
import pg from "pg";

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
];

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function db(query, params = []) {
  return pool.query(query, params);
}

/* =========================
   DATABASE
========================= */

async function initDatabase() {
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
      tts_total_chunks INTEGER NOT NULL DEFAULT 0,
      tts_completed_chunks INTEGER NOT NULL DEFAULT 0,
      tts_current_chunk INTEGER NOT NULL DEFAULT 0,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  /* ===== JOBS MIGRATION ===== */

  await db(`
    ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS chat_id BIGINT
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

  /* ===== AUDIO CHUNKS TABLE ===== */

  await db(`
    CREATE TABLE IF NOT EXISTS job_audio_chunks (
      job_id TEXT NOT NULL,
      chunk_index INTEGER NOT NULL,
      audio_data BYTEA,
      mime_type TEXT,
      status TEXT NOT NULL DEFAULT 'pending',
      model TEXT,
      error TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      PRIMARY KEY (job_id, chunk_index)
    )
  `);

  /* ===== OLD TABLE MIGRATION ===== */

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
  maxWords = 120
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
            mime_type: "audio/wav",
          },
          generation_config: {
            speech_config: {
              voice_config: {
                prebuilt_voice_config: {
                  voice_name: "Kore",
                },
              },
            },
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

    const audioData =
      result?.output_audio?.data;

    if (!audioData) {
      throw new Error(
        "TTS returned no audio data"
      );
    }

    return {
      buffer: Buffer.from(
        audioData,
        "base64"
      ),
      mimeType:
        result?.output_audio?.mime_type ||
        "audio/wav",
      sampleRate:
        result?.output_audio?.sample_rate ||
        24000,
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
    for (let attempt = 1; attempt <= 2; attempt++) {
      try {
        console.log(
          `TTS request: model=${model}, attempt=${attempt}, words=${text.split(/\s+/).length}`
        );

        return {
          ...(await generateTTSChunk(text, model)),
          model,
        };
      } catch (error) {
        lastError = error;

        console.log(
          `TTS failed: model=${model}, attempt=${attempt}, error=${error.message}`
        );

        const retryable =
          error.code === "TIMEOUT" ||
          error.status === 429 ||
          error.status === 500 ||
          error.status === 502 ||
          error.status === 503;

        if (!retryable) break;

        if (attempt < 2) {
          const wait =
            4000 + Math.floor(Math.random() * 5000);

          await sleep(wait);
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

async function processTTS(
  job,
  chatId
) {
  const chunks = splitIntoChunks(
    job.script,
    120
  );

  if (!chunks.length) {
    throw new Error(
      "No script text available for TTS"
    );
  }

  await updateJob(job.id, {
    chat_id:
      chatId || job.chat_id,

    tts_total_chunks:
      chunks.length,

    stage: "tts",
    progress: 55,
    status: "running",
    error: null,
  });

  for (
    let i = 0;
    i < chunks.length;
    i++
  ) {
    const existing = await db(
      `
      SELECT audio_data, status
      FROM job_audio_chunks
      WHERE job_id = $1
        AND chunk_index = $2
      `,
      [job.id, i]
    );

    if (
      existing.rows[0]?.status ===
        "completed" &&
      existing.rows[0]?.audio_data
    ) {
      console.log(
        `Skipping saved TTS chunk ${i + 1}/${chunks.length}`
      );

      continue;
    }

    await updateJob(job.id, {
      tts_current_chunk: i,
    });

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
            $1,
            $2,
            $3,
            $4,
            'completed',
            $5,
            NULL
          )
        ON CONFLICT
          (job_id, chunk_index)
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

      const countResult =
        await db(
          `
          SELECT COUNT(*)::int AS count
          FROM job_audio_chunks
          WHERE job_id = $1
            AND status = 'completed'
          `,
          [job.id]
        );

      const completed =
        countResult.rows[0].count;

      const progress = Math.min(
        85,
        55 +
          Math.floor(
            (completed /
              chunks.length) *
              30
          )
      );

      await updateJob(job.id, {
        tts_completed_chunks:
          completed,

        progress,
        error: null,
      });

      await sendMessage(
        chatId || job.chat_id,
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
            $1,
            $2,
            'failed',
            $3
          )
        ON CONFLICT
          (job_id, chunk_index)
        DO UPDATE SET
          status =
            'failed',
          error =
            EXCLUDED.error,
          updated_at =
            NOW()
        `,
        [
          job.id,
          i,
          error.message,
        ]
      );

      await updateJob(job.id, {
        status: "paused",
        stage: "tts",
        error: error.message,
      });

      await sendMessage(
        chatId || job.chat_id,
        `⏸️ JOB PAUSED SAFELY

TTS chunk ${i + 1}/${chunks.length} failed.

Already completed chunks are saved.
No paid fallback was used.

Resume with:
/resume ${job.id}`
      );

      return false;
    }
  }

  const audioRows = await db(
    `
    SELECT
      chunk_index,
      audio_data
    FROM job_audio_chunks
    WHERE job_id = $1
      AND status = 'completed'
    ORDER BY chunk_index ASC
    `,
    [job.id]
  );

  if (
    audioRows.rows.length !==
    chunks.length
  ) {
    throw new Error(
      "Not all TTS chunks completed"
    );
  }

  const audio =
    concatWavBuffers(
      audioRows.rows.map(
        (row) => row.audio_data
      )
    );

  await updateJob(job.id, {
    status: "completed",
    stage: "tts_complete",
    progress: 100,
    tts_completed_chunks:
      chunks.length,
    error: null,
  });

  const targetChat =
    chatId || job.chat_id;

  await sendMessage(
    targetChat,
    `✅ TTS COMPLETED

${chunks.length} narration chunks generated.
Audio file is being sent now.`
  );

  if (
    audio.length <=
    49 * 1024 * 1024
  ) {
    await sendDocument(
      targetChat,
      audio,
      `${job.id}.wav`,
      "audio/wav",
      `🎙️ AI narration completed
Job: ${job.id}`
    );
  } else {
    await sendMessage(
      targetChat,
      "⚠️ Narration completed, but the WAV file is above Telegram's upload limit."
    );
  }

  return true;
}

/* =========================
   FULL JOB
========================= */

async function processJob(
  id,
  chatId = null
) {
  let job =
    await getJob(id);

  if (!job) {
    throw new Error(
      "Job not found"
    );
  }

  const targetChat =
    chatId || job.chat_id;

  await updateJob(id, {
    chat_id: targetChat,
    status: "running",
    error: null,
  });

  job = await getJob(id);

  if (!job.script) {
    await updateJob(id, {
      stage: "script",
      progress: 10,
    });

    await sendMessage(
      targetChat,
      "📝 Generating original script\nProgress: 10%"
    );

    const result =
      await generateScript(
        job.topic
      );

    const wordCount =
      qualityCheck(
        result.script
      );

    await updateJob(id, {
      script: result.script,
      progress: 40,
      stage: "script_complete",
    });

    await updateJob(id, {
      progress: 50,
      stage: "quality_checked",
    });

    await sendMessage(
      targetChat,
      `📝 Script generated — ${result.model}
Progress: 40%

✅ Quality check passed — ${wordCount} words
Progress: 50%`
    );

    job = await getJob(id);
  }

  await updateJob(id, {
    stage: "tts",
    progress: 55,
    status: "running",
    error: null,
  });

  await sendMessage(
    targetChat,
    "🎙️ Generating AI narration\nProgress: 55%"
  );

  await processTTS(
    job,
    targetChat
  );
    }
/* =========================
   CREATE
========================= */

async function createJob(topic, chatId) {
  const id =
    `job_${Date.now()}_${Math.floor(Math.random() * 1000)}`;

  // IMPORTANT:
  // Telegram chat_id is saved with the job.
  // This prevents the previous NOT NULL error
  // and allows safe resume after restart.

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

  await sendMessage(
    chatId,
    `🎬 JOB CREATED

Topic: ${topic}

Job ID: ${id}`
  );

  processJob(
    id,
    chatId
  ).catch(async (error) => {
    console.error(
      "Background job error:",
      error
    );

    await updateJob(
      id,
      {
        status: "paused",
        error: error.message,
      }
    ).catch(() => {});

    await sendMessage(
      chatId,
      `⏸️ JOB PAUSED SAFELY

Reason: ${error.message}

Resume with:
/resume ${id}`
    ).catch(() => {});
  });

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

  if (job.status === "completed") {
    await sendMessage(
      chatId,
      "✅ This job is already completed."
    );

    return;
  }

  const savedChat =
    chatId || job.chat_id;

  await updateJob(
    id,
    {
      chat_id: savedChat,
      status: "queued",
      error: null,
    }
  );

  await sendMessage(
    savedChat,
    `▶️ RESUMING JOB

${id}

Saved script and completed TTS chunks will be reused.`
  );

  processJob(
    id,
    savedChat
  ).catch(async (error) => {
    await updateJob(
      id,
      {
        status: "paused",
        error: error.message,
      }
    ).catch(() => {});

    await sendMessage(
      savedChat,
      `⏸️ JOB PAUSED SAFELY

Reason: ${error.message}

Resume with:
/resume ${id}`
    ).catch(() => {});
  });
}

/* =========================
   STATUS
========================= */

async function status(chatId) {
  const result =
    await db(`
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
    `);

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

    // Respond immediately so Telegram
    // does not resend the same update.
    res.sendStatus(200);

    try {

      if (
        WEBHOOK_SECRET &&
        req.get(
          "X-Telegram-Bot-Api-Secret-Token"
        ) !== WEBHOOK_SECRET
      ) {
        console.log(
          "Rejected Telegram webhook: invalid secret"
        );

        return;
      }

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

      /* =====================
         /status
      ===================== */

      if (text === "/status") {

        await status(
          chatId
        );

        return;
      }

      /* =====================
         /create
      ===================== */

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

      /* =====================
         /resume
      ===================== */

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

      /* =====================
         UNKNOWN COMMAND
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

    /* =====================
       RECOVERY
    ===================== */

    // If Render restarts while a job
    // was running, put it back into
    // queued state instead of losing it.

    const recovery =
      await db(`
        UPDATE jobs
        SET
          status = 'queued',
          updated_at = NOW()
        WHERE status = 'running'
        RETURNING id
      `);

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
