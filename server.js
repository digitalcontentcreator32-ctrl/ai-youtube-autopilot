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

/* =====================================================
   DATABASE
===================================================== */

if (!DATABASE_URL) {
  console.error("DATABASE_URL missing");
  process.exit(1);
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: {
    rejectUnauthorized: false
  },
  max: 3
});

async function db(query, params = []) {
  return pool.query(query, params);
}

async function initDatabase() {
  await db(`
    CREATE TABLE IF NOT EXISTS jobs (
      id TEXT PRIMARY KEY,
      chat_id TEXT NOT NULL,
      topic TEXT NOT NULL,

      status TEXT NOT NULL DEFAULT 'queued',
      stage TEXT NOT NULL DEFAULT 'Job created',
      progress INTEGER NOT NULL DEFAULT 0,

      script TEXT,
      script_model TEXT,
      word_count INTEGER,

      tts_model TEXT,

      error TEXT,

      attempts INTEGER NOT NULL DEFAULT 0,

      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      completed_at TIMESTAMPTZ
    )
  `);

  await db(`
    CREATE TABLE IF NOT EXISTS quota_events (
      id BIGSERIAL PRIMARY KEY,
      job_id TEXT,
      provider TEXT,
      model TEXT,
      status INTEGER,
      message TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  console.log("PostgreSQL database initialized");
}

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
      message.includes("free_tier") ||
      message.includes("exceeded")
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
      throw new Error(`REQUEST_TIMEOUT_${timeoutMs}MS`);
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
    const message =
      data?.error?.message ||
      raw ||
      response.statusText;

    const error = new Error(
      `Gemini ${response.status}: ${message}`
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
   RETRY ENGINE
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
        `${label} failed attempt ${attempt}:`,
        error.message
      );

      // Quota exceeded = do NOT waste retries.
      if (isQuotaError(error)) {
        throw error;
      }

      if (!isTransient(error)) {
        throw error;
      }

      if (attempt < attempts) {
        const base =
          Math.min(
            16000,
            1500 * (2 ** (attempt - 1))
          );

        const jitter =
          Math.floor(Math.random() * 1000);

        const wait =
          base + jitter;

        console.log(
          `${label} retrying after ${wait}ms`
        );

        await sleep(wait);
      }
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

async function generateScript(topic, job) {
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
- Do not mention being an AI
- Return ONLY the narration
- Make every sentence useful and engaging.
      `.trim();

      const script =
        await retryRequest(
          () => geminiText(model, prompt),
          `SCRIPT ${model}`,
          2
        );

      if (script.length < 1200) {
        throw new Error("SCRIPT_TOO_SHORT");
      }

      console.log(
        `Script success: ${model}`
      );

      return {
        script,
        model
      };

    } catch (error) {
      lastError = error;

      if (isQuotaError(error)) {
        await recordQuotaEvent(
          job.id,
          model,
          error
        );
      }

      console.error(
        `Script model unavailable: ${model} -> ${error.message}`
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
   TTS
===================================================== */

const TTS_MODELS = [
  "gemini-3.1-flash-tts-preview",
  "gemini-2.5-flash-preview-tts"
];

async function geminiTTS(model, text) {
  const url =
    "https://generativelanguage.googleapis.com/v1beta/interactions";

  const response = await fetchWithTimeout(
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

  const raw = await response.text();

  let data;

  try {
    data = JSON.parse(raw);
  } catch {
    data = null;
  }

  if (!response.ok) {
    const message =
      data?.error?.message ||
      raw ||
      response.statusText;

    const error = new Error(
      `TTS ${response.status}: ${message}`
    );

    error.status = response.status;

    throw error;
  }

  const base64 =
    data?.output_audio?.data ||
    data?.outputAudio?.data ||
    data?.audio?.data;

  if (!base64) {
    throw new Error("TTS_AUDIO_MISSING");
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
    Buffer.alloc(44 + pcm.length);

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
===================================================== */

async function generateVoice(script, job) {
  let lastError = null;

  for (const model of TTS_MODELS) {
    try {
      console.log(
        `Trying TTS model: ${model}`
      );

      const pcm =
        await retryRequest(
          () => geminiTTS(model, script),
          `TTS ${model}`,
          2
        );

      if (!pcm.length) {
        throw new Error(
          "TTS_EMPTY_AUDIO"
        );
      }

      const wav =
        pcmToWav(pcm);

      console.log(
        `TTS success: ${model}`
      );

      return {
        audio: wav,
        model
      };

    } catch (error) {
      lastError = error;

      if (isQuotaError(error)) {
        await recordQuotaEvent(
          job.id,
          model,
          error
        );
      }

      console.error(
        `TTS model failed: ${model} -> ${error.message}`
      );

      await sleep(1000);
    }
  }

  throw (
    lastError ||
    new Error("ALL_TTS_MODELS_FAILED")
  );
}

/* =====================================================
   QUOTA LOGGING
===================================================== */

async function recordQuotaEvent(
  jobIdValue,
  model,
  error
) {
  try {
    await db(
      `
      INSERT INTO quota_events
      (
        job_id,
        provider,
        model,
        status,
        message
      )
      VALUES ($1, $2, $3, $4, $5)
      `,
      [
        jobIdValue,
        "Google Gemini",
        model,
        error?.status || 429,
        error?.message || "Quota exceeded"
      ]
    );
  } catch (dbError) {
    console.error(
      "Quota event DB error:",
      dbError.message
    );
  }
}

/* =====================================================
   DATABASE JOB HELPERS
===================================================== */

async function createJob(
  id,
  chatId,
  topic
) {
  await db(
    `
    INSERT INTO jobs
    (
      id,
      chat_id,
      topic,
      status,
      stage,
      progress
    )
    VALUES
    ($1, $2, $3, 'running', 'Job created', 0)
    `,
    [
      id,
      String(chatId),
      topic
    ]
  );
}

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
  const allowed = [
    "status",
    "stage",
    "progress",
    "script",
    "script_model",
    "word_count",
    "tts_model",
    "error",
    "attempts"
  ];

  const keys =
    Object.keys(fields)
      .filter(k => allowed.includes(k));

  if (!keys.length) {
    return;
  }

  const values = [];
  const sets = [];

  keys.forEach((key, index) => {
    values.push(fields[key]);

    sets.push(
      `${key} = $${index + 1}`
    );
  });

  values.push(id);

  await db(
    `
    UPDATE jobs
    SET
      ${sets.join(", ")},
      updated_at = NOW()
    WHERE id = $${values.length}
    `,
    values
  );
}

async function completeJob(id) {
  await db(
    `
    UPDATE jobs
    SET
      status = 'completed',
      progress = 100,
      stage = 'Test completed successfully',
      completed_at = NOW(),
      updated_at = NOW()
    WHERE id = $1
    `,
    [id]
  );
}

/* =====================================================
   RUNNING JOB LOCK
===================================================== */

const runningJobs = new Set();

/* =====================================================
   JOB PROGRESS
===================================================== */

async function progress(
  job,
  stage,
  percent
) {
  await updateJob(
    job.id,
    {
      stage,
      progress: percent,
      status: "running"
    }
  );

  console.log(
    `[${job.id}] ${stage} ${percent}%`
  );

  try {
    await sendMessage(
      job.chat_id,
      `🎬 ${stage}\nProgress: ${percent}%`
    );
  } catch (error) {
    console.error(
      "Progress message failed:",
      error.message
    );
  }
}

/* =====================================================
   MAIN JOB
===================================================== */

async function runJob(job) {
  if (!job) {
    return;
  }

  if (runningJobs.has(job.id)) {
    console.log(
      `Job already running: ${job.id}`
    );

    return;
  }

  runningJobs.add(job.id);

  try {
    job =
      await getJob(job.id);

    if (!job) {
      return;
    }

    /*
      STEP 1
      Script generation
    */

    if (!job.script) {
      await progress(
        job,
        "Creator Agent started",
        5
      );

      await progress(
        job,
        "Generating original script",
        10
      );

      const result =
        await generateScript(
          job.topic,
          job
        );

      await updateJob(
        job.id,
        {
          script: result.script,
          script_model: result.model,
          progress: 40,
          stage:
            `Script generated — ${result.model}`,
          status: "running",
          error: null
        }
      );

      job =
        await getJob(job.id);

    } else {
      console.log(
        `[${job.id}] Script already saved. Skipping generation.`
      );
    }

    /*
      STEP 2
      Quality check
    */

    const quality =
      checkScript(job.script);

    if (!quality.passed) {
      throw new Error(
        `QUALITY_FAILED_${quality.reason}`
      );
    }

    await updateJob(
      job.id,
      {
        word_count: quality.wordCount,
        stage:
          `Quality check passed — ${quality.wordCount} words`,
        progress: 50,
        status: "running",
        error: null
      }
    );

    /*
      STEP 3
      TTS
    */

    await progress(
      job,
      "Generating AI narration",
      55
    );

    const voice =
      await generateVoice(
        job.script,
        job
      );

    await updateJob(
      job.id,
      {
        tts_model: voice.model,
        progress: 80,
        stage:
          `Narration ready — ${voice.model}`,
        status: "running",
        error: null
      }
    );

    /*
      STEP 4
      Telegram audio delivery
    */

    await sendAudio(
      job.chat_id,
      voice.audio,
      `${job.id}.wav`
    );

    await completeJob(
      job.id
    );

    await sendMessage(
      job.chat_id,
      [
        "✅ JOB COMPLETED",
        "",
        `Topic: ${job.topic}`,
        `Script: ${job.script_model}`,
        `TTS: ${voice.model}`,
        `Words: ${job.word_count}`,
        "",
        "🎧 Audio sent successfully."
      ].join("\n")
    );

  } catch (error) {
    console.error(
      `[${job.id}] FINAL ERROR`,
      error
    );

    await updateJob(
      job.id,
      {
        status: "paused",
        error: error.message,
        stage: "Job paused safely"
      }
    );

    try {
      await sendMessage(
        job.chat_id,
        [
          "⏸️ JOB PAUSED SAFELY",
          "",
          `Topic: ${job.topic}`,
          "",
          `Reason: ${error.message}`,
          "",
          "No paid fallback was used.",
          "",
          `Resume later with:`,
          `/resume ${job.id}`
        ].join("\n")
      );
    } catch {}
  } finally {
    runningJobs.delete(job.id);
  }
}

/* =====================================================
   AUTOMATIC RECOVERY
===================================================== */

async function recoverJobs() {
  const result =
    await db(
      `
      SELECT *
      FROM jobs
      WHERE status = 'running'
      ORDER BY created_at ASC
      `
    );

  if (!result.rows.length) {
    console.log(
      "No jobs waiting for recovery."
    );

    return;
  }

  console.log(
    `Recovering ${result.rows.length} interrupted job(s)...`
  );

  for (const job of result.rows) {
    console.log(
      `Recovering job: ${job.id}`
    );

    /*
      Script is already stored if it was completed
      before the restart, so runJob will skip it.
    */

    runJob(job).catch(error => {
      console.error(
        `Recovery error ${job.id}:`,
        error.message
      );
    });

    await sleep(500);
  }
}

/* =====================================================
   TELEGRAM COMMANDS
===================================================== */

async function handleMessage(message) {
  if (!message?.chat?.id) {
    return;
  }

  const chatId =
    message.chat.id;

  const text =
    clean(message.text);

  if (!text) {
    return;
  }

  console.log(
    `Telegram: ${text}`
  );

  /* -----------------------------------------------
     START
  ------------------------------------------------ */

  if (text === "/start") {
    await sendMessage(
      chatId,
      [
        "🤖 AI YouTube Autopilot",
        "",
        "/create <topic>",
        "/status",
        "/jobs",
        "/resume <job_id>",
        "",
        "Example:",
        "/create 5 surprising facts about space"
      ].join("\n")
    );

    return;
  }

  /* -----------------------------------------------
     STATUS
  ------------------------------------------------ */

  if (text === "/status") {
    const result =
      await db(
        `
        SELECT
          COUNT(*) FILTER (
            WHERE status = 'running'
          ) AS running,

          COUNT(*) FILTER (
            WHERE status = 'paused'
          ) AS paused,

          COUNT(*) FILTER (
            WHERE status = 'completed'
          ) AS completed,

          COUNT(*) AS total

        FROM jobs
        WHERE chat_id = $1
        `,
        [String(chatId)]
      );

    const row =
      result.rows[0];

    await sendMessage(
      chatId,
      [
        "📊 SYSTEM STATUS",
        "",
        "Backend: ONLINE",
        `Running: ${row.running}`,
        `Paused: ${row.paused}`,
        `Completed: ${row.completed}`,
        `Total: ${row.total}`,
        "",
        "Payment mode: APPROVAL ONLY"
      ].join("\n")
    );

    return;
  }

  /* -----------------------------------------------
     JOBS
  ------------------------------------------------ */

  if (text === "/jobs") {
    const result =
      await db(
        `
        SELECT
          id,
          topic,
          status,
          progress,
          stage
        FROM jobs
        WHERE chat_id = $1
        ORDER BY created_at DESC
        LIMIT 10
        `,
        [String(chatId)]
      );

    if (!result.rows.length) {
      await sendMessage(
        chatId,
        "No jobs found."
      );

      return;
    }

    await sendMessage(
      chatId,
      result.rows
        .map(j =>
          [
            `ID: ${j.id}`,
            `Topic: ${j.topic}`,
            `Status: ${j.status}`,
            `Progress: ${j.progress}%`,
            `Stage: ${j.stage}`
          ].join("\n")
        )
        .join("\n\n")
    );

    return;
  }

  /* -----------------------------------------------
     CREATE
  ------------------------------------------------ */

  if (text.startsWith("/create ")) {
    const topic =
      text
        .slice(8)
        .trim();

    if (!topic) {
      await sendMessage(
        chatId,
        "Please provide a topic."
      );

      return;
    }

    const id =
      jobId();

    await createJob(
      id,
      chatId,
      topic
    );

    await sendMessage(
      chatId,
      [
        "🎬 JOB CREATED",
        "",
        `ID: ${id}`,
        `Topic: ${topic}`,
        "",
        "Starting Creator Agent..."
      ].join("\n")
    );

    const job =
      await getJob(id);

    runJob(job).catch(error => {
      console.error(
        "Background job error:",
        error
      );
    });

    return;
  }

  /* -----------------------------------------------
     RESUME
  ------------------------------------------------ */

  if (text.startsWith("/resume ")) {
    const id =
      text
        .slice(8)
        .trim();

    if (!id) {
      await sendMessage(
        chatId,
        "Usage: /resume <job_id>"
      );

      return;
    }

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
      String(job.chat_id) !==
      String(chatId)
    ) {
      await sendMessage(
        chatId,
        "❌ This job does not belong to this chat."
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

    if (runningJobs.has(job.id)) {
      await sendMessage(
        chatId,
        "▶️ This job is already running."
      );

      return;
    }

    await updateJob(
      job.id,
      {
        status: "running",
        error: null,
        stage: "Resuming job"
      }
    );

    await sendMessage(
      chatId,
      [
        "▶️ JOB RESUMED",
        "",
        `ID: ${job.id}`,
        "Continuing from saved state..."
      ].join("\n")
    );

    const updated =
      await getJob(job.id);

    runJob(updated).catch(error => {
      console.error(
        `Resume error ${job.id}:`,
        error
      );
    });

    return;
  }

  /* -----------------------------------------------
     UNKNOWN
  ------------------------------------------------ */

  await sendMessage(
    chatId,
    "Unknown command.\n\nUse /start."
  );
}

/* =====================================================
   TELEGRAM WEBHOOK
===================================================== */

app.post(
  "/telegram/webhook",
  (req, res) => {
    /*
      Telegram needs an immediate 2xx response.
    */
    res.sendStatus(200);

    if (req.body?.message) {
      handleMessage(
        req.body.message
      ).catch(error => {
        console.error(
          "Webhook error:",
          error
        );
      });
    }
  }
);

/* =====================================================
   HEALTH
===================================================== */

app.get(
  "/",
  (req, res) => {
    res.json({
      ok: true,
      service:
        "AI YouTube Autopilot",
      telegram:
        "webhook",
      database:
        "postgresql",
      status:
        "online"
    });
  }
);

app.get(
  "/health",
  async (req, res) => {
    try {
      await db("SELECT 1");

      res.json({
        ok: true,
        database: "connected",
        uptime:
          process.uptime()
      });

    } catch (error) {
      res.status(500).json({
        ok: false,
        database: "error",
        error: error.message
      });
    }
  }
);

/* =====================================================
   WEBHOOK SETUP
===================================================== */

async function setupWebhook() {
  if (!TELEGRAM_BOT_TOKEN) {
    throw new Error(
      "TELEGRAM_BOT_TOKEN missing"
    );
  }

  if (!GEMINI_API_KEY) {
    throw new Error(
      "GEMINI_API_KEY missing"
    );
  }

  const webhookUrl =
    `${BASE_URL.replace(/\/$/, "")}/telegram/webhook`;

  console.log(
    `Setting Telegram webhook: ${webhookUrl}`
  );

  await telegram(
    "setWebhook",
    {
      url: webhookUrl,
      allowed_updates: [
        "message"
      ],
      drop_pending_updates: false
    }
  );

  const info =
    await telegram(
      "getWebhookInfo"
    );

  console.log(
    "Telegram webhook configured:"
  );

  console.log(
    JSON.stringify(
      info,
      null,
      2
    )
  );
}

/* =====================================================
   START
===================================================== */

async function start() {
  try {
    await initDatabase();

    app.listen(
      PORT,
      async () => {
        console.log(
          `AI YouTube Autopilot listening on port ${PORT}`
        );

        try {
          await setupWebhook();

          console.log(
            "Telegram webhook configured"
          );

          await recoverJobs();

          console.log(
            "✅ STARTUP COMPLETE"
          );

        } catch (error) {
          console.error(
            "❌ STARTUP ERROR:",
            error.message
          );
        }
      }
    );

  } catch (error) {
    console.error(
      "❌ FATAL STARTUP ERROR:",
      error
    );

    process.exit(1);
  }
}

start();
