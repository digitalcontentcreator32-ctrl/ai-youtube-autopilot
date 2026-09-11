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

  /* =========================
     JOBS MIGRATION
  ========================= */

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

  /*
    IMPORTANT:
    The table may already exist from an older
    version of the application.

    Therefore every required column is checked
    separately below.
  */

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

  /* =========================
     DEFAULT VALUES
  ========================= */

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
      console.log(
        `Generating script with model: ${model}`
      );

      const result = await retryGemini(
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
        },
        60000,
        2
      );

      const text =
        result?.candidates?.[0]?.content?.parts
          ?.map(part => part?.text || "")
          .join("")
          .trim();

      if (!text) {
        throw new Error(
          "Gemini returned empty script"
        );
      }

      return text;
    } catch (error) {
      lastError = error;

      console.error(
        `Script generation failed: model=${model}, error=${error.message}`
      );
    }
  }

  throw lastError ||
    new Error("All script models failed");
    }
/* =========================
   SCRIPT QUALITY CHECK
========================= */

function qualityCheckScript(script) {
  if (!script || !script.trim()) {
    return {
      passed: false,
      reason: "Script is empty",
    };
  }

  const words = script
    .trim()
    .split(/\s+/)
    .filter(Boolean);

  if (words.length < 300) {
    return {
      passed: false,
      reason: `Script too short: ${words.length} words`,
    };
  }

  const lower = script.toLowerCase();

  const blockedPatterns = [
    "lorem ipsum",
    "as an ai language model",
    "i cannot provide",
  ];

  const foundBlocked = blockedPatterns.find(
    item => lower.includes(item)
  );

  if (foundBlocked) {
    return {
      passed: false,
      reason: `Unwanted phrase detected: ${foundBlocked}`,
    };
  }

  return {
    passed: true,
    words: words.length,
  };
}

/* =========================
   TEXT CHUNKING
========================= */

function splitIntoChunks(
  text,
  maxWords = 450
) {
  const words = text
    .trim()
    .split(/\s+/)
    .filter(Boolean);

  const chunks = [];

  let current = [];

  for (const word of words) {
    current.push(word);

    if (current.length >= maxWords) {
      chunks.push(current.join(" "));
      current = [];
    }
  }

  if (current.length > 0) {
    chunks.push(current.join(" "));
  }

  return chunks;
}

/* =========================
   WAV HELPERS
========================= */

function isWav(buffer) {
  if (!Buffer.isBuffer(buffer)) {
    return false;
  }

  if (buffer.length < 12) {
    return false;
  }

  const riff =
    buffer.toString(
      "ascii",
      0,
      4
    );

  const wave =
    buffer.toString(
      "ascii",
      8,
      12
    );

  return (
    riff === "RIFF" &&
    wave === "WAVE"
  );
}

function concatBuffers(buffers) {
  return Buffer.concat(
    buffers.filter(
      buffer =>
        Buffer.isBuffer(buffer) &&
        buffer.length > 0
    )
  );
}

/* =========================
   FINAL GEMINI TTS
========================= */

async function generateTTSChunk(
  text,
  model = TTS_MODELS[0]
) {
  if (!text || !text.trim()) {
    throw new Error(
      "TTS text is empty"
    );
  }

  console.log(
    `TTS request: model=${model}, attempt=1, words=${text.trim().split(/\s+/).length}`
  );

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

        /*
         * IMPORTANT:
         * Audio is requested as an inline
         * response.
         */
        response_format: {
          type: "audio",
          mime_type: "audio/wav",
          delivery: "inline",
        },

        /*
         * IMPORTANT:
         * The current Interactions API expects
         * speech_config as an array.
         */
        generation_config: {
          speech_config: [
            {
              voice: "Kore",
              language: "en-US",
            },
          ],
        },
      }),
    }
  );

  const raw =
    await response.text();

  if (!response.ok) {
    const error =
      new Error(
        `TTS HTTP ${response.status}: ${raw}`
      );

    error.status =
      response.status;

    throw error;
  }

  let result;

  try {
    result = JSON.parse(raw);
  } catch {
    throw new Error(
      `TTS returned invalid JSON: ${raw.slice(0, 500)}`
    );
  }

  /*
   * IMPORTANT:
   *
   * The raw REST response contains audio
   * inside outputs[].
   *
   * Do NOT use:
   *
   * result.output_audio.data
   *
   * That is an SDK convenience property,
   * not the raw REST response structure.
   */
  const audioOutput =
    Array.isArray(result?.outputs)
      ? result.outputs.find(
          output =>
            output?.type === "audio"
        )
      : null;

  const audioData =
    audioOutput?.data;

  if (!audioData) {
    console.error(
      "TTS raw response:",
      JSON.stringify(
        result
      ).slice(0, 3000)
    );

    throw new Error(
      "TTS returned no audio data in outputs[]"
    );
  }

  let buffer;

  try {
    buffer =
      Buffer.from(
        audioData,
        "base64"
      );
  } catch (error) {
    throw new Error(
      `TTS audio base64 decode failed: ${error.message}`
    );
  }

  if (
    !buffer ||
    buffer.length === 0
  ) {
    throw new Error(
      "TTS audio buffer is empty"
    );
  }

  if (!isWav(buffer)) {
    throw new Error(
      `TTS returned audio, but it is not a valid WAV file. bytes=${buffer.length}`
    );
  }

  return {
    buffer,
    mimeType:
      audioOutput?.mime_type ||
      "audio/wav",
    sampleRate:
      audioOutput?.sample_rate ||
      null,
  };
}

/* =========================
   TTS RETRY
========================= */

async function generateTTSWithRetry(
  text
) {
  /*
   * FREE-TIER SAFETY:
   *
   * Use only the primary TTS model.
   *
   * Do not automatically jump to another
   * model after quota/auth/request errors.
   */
  const model =
    TTS_MODELS[0];

  let lastError;

  /*
   * Only transient server/timeout errors
   * receive a retry.
   *
   * 400 / 401 / 403 / 429 are NOT retried.
   */
  const maxAttempts = 2;

  for (
    let attempt = 1;
    attempt <= maxAttempts;
    attempt++
  ) {
    try {
      console.log(
        `TTS request: model=${model}, attempt=${attempt}, words=${text.trim().split(/\s+/).length}`
      );

      return await generateTTSChunk(
        text,
        model
      );
    } catch (error) {
      lastError = error;

      console.error(
        `TTS failed: model=${model}, attempt=${attempt}, error=${error.message}`
      );

      const retryable =
        error.code === "TIMEOUT" ||
        error.status === 500 ||
        error.status === 502 ||
        error.status === 503;

      if (
        !retryable ||
        attempt >= maxAttempts
      ) {
        throw error;
      }

      const wait =
        5000 +
        Math.floor(
          Math.random() * 3000
        );

      console.log(
        `TTS transient error. Waiting ${wait}ms before retry.`
      );

      await sleep(wait);
    }
  }

  throw (
    lastError ||
    new Error(
      "TTS generation failed"
    )
  );
}

/* =========================
   AUDIO CHUNK DATABASE
========================= */

async function saveAudioChunk(
  jobId,
  chunkIndex,
  audio
) {
  await db(
    `
      INSERT INTO job_audio_chunks (
        job_id,
        chunk_index,
        audio_data,
        mime_type,
        status,
        model,
        error,
        created_at,
        updated_at
      )
      VALUES (
        $1,
        $2,
        $3,
        $4,
        'completed',
        $5,
        NULL,
        NOW(),
        NOW()
      )
      ON CONFLICT (
        job_id,
        chunk_index
      )
      DO UPDATE SET
        audio_data = EXCLUDED.audio_data,
        mime_type = EXCLUDED.mime_type,
        status = 'completed',
        model = EXCLUDED.model,
        error = NULL,
        updated_at = NOW()
    `,
    [
      jobId,
      chunkIndex,
      audio.buffer,
      audio.mimeType,
      TTS_MODELS[0],
    ]
  );
}

async function markAudioChunkFailed(
  jobId,
  chunkIndex,
  error
) {
  await db(
    `
      INSERT INTO job_audio_chunks (
        job_id,
        chunk_index,
        status,
        error,
        created_at,
        updated_at
      )
      VALUES (
        $1,
        $2,
        'failed',
        $3,
        NOW(),
        NOW()
      )
      ON CONFLICT (
        job_id,
        chunk_index
      )
      DO UPDATE SET
        status = 'failed',
        error = EXCLUDED.error,
        updated_at = NOW()
    `,
    [
      jobId,
      chunkIndex,
      error?.message ||
        String(error),
    ]
  );
}

async function getCompletedAudioChunks(
  jobId
) {
  const result = await db(
    `
      SELECT
        chunk_index,
        audio_data,
        mime_type
      FROM job_audio_chunks
      WHERE job_id = $1
        AND status = 'completed'
        AND audio_data IS NOT NULL
      ORDER BY chunk_index ASC
    `,
    [jobId]
  );

  return result.rows;
}

/* =========================
   JOB HELPERS
========================= */

async function getJob(id) {
  const result = await db(
    `
      SELECT *
      FROM jobs
      WHERE id = $1
    `,
    [id]
  );

  return result.rows[0] || null;
}

async function updateJob(
  id,
  fields
) {
  const keys =
    Object.keys(fields);

  if (!keys.length) {
    return getJob(id);
  }

  const values =
    Object.values(fields);

  const setParts =
    keys.map(
      (key, index) =>
        `"${key}" = $${index + 2}`
    );

  const result =
    await db(
      `
        UPDATE jobs
        SET
          ${setParts.join(", ")},
          updated_at = NOW()
        WHERE id = $1
        RETURNING *
      `,
      [id, ...values]
    );

  return result.rows[0] || null;
}

async function createJob(
  topic,
  chatId
) {
  const id =
    `job_${Date.now()}_${Math.floor(Math.random() * 1000)}`;

  const result =
    await db(
      `
        INSERT INTO jobs (
          id,
          topic,
          chat_id,
          status,
          progress,
          stage
        )
        VALUES (
          $1,
          $2,
          $3,
          'queued',
          0,
          'queued'
        )
        RETURNING *
      `,
      [
        id,
        topic,
        chatId,
      ]
    );

  return result.rows[0];
}

/* =========================
   SAFE JOB PAUSE
========================= */

async function pauseJobSafely(
  jobId,
  error
) {
  const job =
    await getJob(jobId);

  if (!job) {
    return null;
  }

  const message =
    error?.message ||
    String(error);

  const updated =
    await updateJob(
      jobId,
      {
        status: "paused",
        stage: "paused",
        error: message,
      }
    );

  if (job.chat_id) {
    await sendMessage(
      job.chat_id,
      [
        "⚠️ JOB PAUSED SAFELY",
        "",
        `Job: ${jobId}`,
        `Reason: ${message}`,
        "",
        "Already completed chunks are saved.",
        "No paid fallback was used.",
        "",
        `Resume with: /resume ${jobId}`,
      ].join("\n")
    );
  }

  return updated;
}

/* =========================
   RESUME JOB
========================= */

async function resumeJob(
  jobId
) {
  const job =
    await getJob(jobId);

  if (!job) {
    throw new Error(
      `Job not found: ${jobId}`
    );
  }

  await updateJob(
    jobId,
    {
      status: "queued",
      stage: "queued",
      error: null,
    }
  );

  /*
   * processJob() will inspect already
   * completed audio chunks and continue
   * from the missing chunk.
   */
  return processJob(jobId);
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
    450
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

  // Refresh after status updates so resume/restart always uses the
  // latest persisted script and chat_id.
  job = await getJob(id);

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

        // Resume jobs that were queued/recovered during a previous
        // process lifetime. Existing completed TTS chunks are reused.
        setTimeout(async () => {
          try {
            const queued = await db(`
              SELECT id, chat_id
              FROM jobs
              WHERE status = 'queued'
              ORDER BY created_at ASC
              LIMIT 10
            `);

            for (const row of queued.rows) {
              processJob(
                row.id,
                row.chat_id
              ).catch(async (error) => {

                await updateJob(
                  row.id,
                  {
                    status: "paused",
                    error: error.message,
                  }
                ).catch(() => {});

                await sendMessage(
                  row.chat_id,
                  `⏸️ JOB PAUSED SAFELY

Reason: ${error.message}

Resume with:
/resume ${row.id}`
                ).catch(() => {});

              });
            }

          } catch (error) {

            console.error(
              "Queued-job recovery failed:",
              error
            );

          }

        }, 1000);

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
