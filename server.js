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
  ssl: DATABASE_URL
    ? { rejectUnauthorized: false }
    : false,
  max: 5,
  connectionTimeoutMillis: 10000
});

const runningJobs = new Set();

/* =====================================================
   HELPERS
===================================================== */

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function clean(value) {
  return String(value ?? "").trim();
}

function jobId() {
  return `job_${Date.now()}_${Math.floor(Math.random() * 1000)}`;
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

  await pool.query(`
    CREATE INDEX IF NOT EXISTS jobs_chat_id_idx
    ON jobs(chat_id)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS jobs_status_idx
    ON jobs(status)
  `);

  /*
    Render restart ke time jo job running thi,
    use automatically resumable banaya jayega.
  */
  await pool.query(`
    UPDATE jobs
    SET
      status = 'queued',
      stage = CASE
        WHEN script IS NOT NULL
        THEN 'Recovering saved script'
        ELSE 'Recovering interrupted job'
      END,
      updated_at = NOW()
    WHERE status = 'running'
  `);

  console.log("✅ PostgreSQL database initialized");
}

async function getJob(id) {
  const result = await pool.query(
    `SELECT * FROM jobs WHERE id = $1`,
    [id]
  );

  return result.rows[0] || null;
}

function dbJob(row) {
  if (!row) return null;

  return {
    id: row.id,
    chatId: row.chat_id,
    topic: row.topic,
    status: row.status,
    stage: row.stage,
    progress: row.progress,
    script: row.script || "",
    scriptModel: row.script_model || "",
    wordCount: row.word_count || 0,
    ttsModel: row.tts_model || "",
    error: row.error || "",
    attempts: row.attempts || 0
  };
}

async function createJob(chatId, topic) {
  const id = jobId();

  await pool.query(
    `
    INSERT INTO jobs
      (id, chat_id, topic, status, stage, progress)
    VALUES
      ($1, $2, $3, 'queued', 'Job created', 0)
    `,
    [id, String(chatId), topic]
  );

  return getJob(id);
}

async function updateJob(id, fields) {
  const allowed = {
    status: "status",
    stage: "stage",
    progress: "progress",
    script: "script",
    scriptModel: "script_model",
    wordCount: "word_count",
    ttsModel: "tts_model",
    error: "error",
    attempts: "attempts"
  };

  const sets = [];
  const values = [];

  for (const [key, value] of Object.entries(fields)) {
    if (!(key in allowed)) continue;

    values.push(value);
    sets.push(
      `${allowed[key]} = $${values.length}`
    );
  }

  if (!sets.length) return;

  values.push(id);

  await pool.query(
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

async function saveProgress(job, stage, percent) {
  job.stage = stage;
  job.progress = percent;

  await updateJob(job.id, {
    stage,
    progress: percent
  });

  console.log(
    `[${job.id}] ${stage} ${percent}%`
  );

  try {
    await sendMessage(
      job.chatId,
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
   TIMEOUT
===================================================== */

async function fetchWithTimeout(
  url,
  options = {},
  timeoutMs = 45000
) {
  const controller = new AbortController();

  const timer = setTimeout(
    () => controller.abort(),
    timeoutMs
  );

  try {
    return await fetch(
      url,
      {
        ...options,
        signal: controller.signal
      }
    );
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

async function telegram(
  method,
  body = {}
) {
  const response =
    await fetchWithTimeout(
      `${TELEGRAM_API}/${method}`,
      {
        method: "POST",
        headers: {
          "Content-Type":
            "application/json"
        },
        body:
          JSON.stringify(body)
      },
      20000
    );

  const data =
    await response.json();

  if (
    !response.ok ||
    !data.ok
  ) {
    throw new Error(
      `Telegram ${method}: ${
        data.description ||
        response.statusText
      }`
    );
  }

  return data.result;
}

async function sendMessage(
  chatId,
  text
) {
  return telegram(
    "sendMessage",
    {
      chat_id: chatId,
      text
    }
  );
}

async function sendAudio(
  chatId,
  audioBuffer,
  filename
) {
  const form =
    new FormData();

  form.append(
    "chat_id",
    String(chatId)
  );

  form.append(
    "audio",
    new Blob(
      [audioBuffer],
      {
        type: "audio/wav"
      }
    ),
    filename
  );

  const response =
    await fetchWithTimeout(
      `${TELEGRAM_API}/sendAudio`,
      {
        method: "POST",
        body: form
      },
      30000
    );

  const data =
    await response.json();

  if (
    !response.ok ||
    !data.ok
  ) {
    throw new Error(
      `Telegram sendAudio: ${
        data.description ||
        response.statusText
      }`
    );
  }

  return data.result;
}

/* =====================================================
   GEMINI
===================================================== */

async function geminiText(
  model,
  prompt
) {
  const response =
    await fetchWithTimeout(
      `${GEMINI_API}/models/${model}:generateContent`,
      {
        method: "POST",
        headers: {
          "Content-Type":
            "application/json",
          "x-goog-api-key":
            GEMINI_API_KEY
        },
        body:
          JSON.stringify({
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

  const raw =
    await response.text();

  let data = null;

  try {
    data =
      JSON.parse(raw);
  } catch {}

  if (!response.ok) {
    const error =
      new Error(
        `Gemini ${response.status}: ${
          data?.error?.message ||
          raw ||
          response.statusText
        }`
      );

    error.status =
      response.status;

    throw error;
  }

  const text =
    data?.candidates?.[0]
      ?.content?.parts
      ?.map(
        part => part.text || ""
      )
      .join("")
      .trim() || "";

  if (!text) {
    throw new Error(
      "GEMINI_EMPTY_RESPONSE"
    );
  }

  return text;
}

/* =====================================================
   ERROR CLASSIFICATION
===================================================== */

function isQuotaError(error) {
  const message =
    String(
      error?.message || ""
    ).toLowerCase();

  return (
    error?.status === 429 &&
    (
      message.includes("quota") ||
      message.includes("exceeded") ||
      message.includes("rate limit")
    )
  );
}

function isTransient(error) {
  if (
    isQuotaError(error)
  ) {
    return false;
  }

  const message =
    String(
      error?.message || ""
    );

  return (
    error?.status === 408 ||
    error?.status === 429 ||
    (
      error?.status >= 500 &&
      error?.status <= 599
    ) ||
    message.includes(
      "REQUEST_TIMEOUT"
    ) ||
    message.includes(
      "fetch failed"
    ) ||
    message.includes(
      "ECONNRESET"
    )
  );
}

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
        `${label} failed:`,
        error.message
      );

      /*
        Quota exhausted hone par
        same request repeat nahi karenge.
      */
      if (
        !isTransient(error)
      ) {
        throw error;
      }

      if (
        attempt < attempts
      ) {
        const wait =
          Math.min(
            12000,
            1500 *
              (2 ** (attempt - 1))
          ) +
          Math.floor(
            Math.random() * 1000
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
   SCRIPT
===================================================== */

async function generateScript(
  topic
) {
  let lastError = null;

  for (
    const model of SCRIPT_MODELS
  ) {
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
          () =>
            geminiText(
              model,
              prompt
            ),
          `SCRIPT ${model}`,
          2
        );

      if (
        script.length < 1200
      ) {
        throw new Error(
          "SCRIPT_TOO_SHORT"
        );
      }

      return {
        script,
        model
      };

    } catch (error) {
      lastError =
        error;

      console.error(
        `Script model failed: ${model} -> ${error.message}`
      );

      await sleep(500);
    }
  }

  throw (
    lastError ||
    new Error(
      "ALL_SCRIPT_MODELS_FAILED"
    )
  );
}

/* =====================================================
   QUALITY
===================================================== */

function checkScript(
  script
) {
  const words =
    script
      .split(/\s+/)
      .filter(Boolean);

  const wordCount =
    words.length;

  if (
    wordCount < 250
  ) {
    return {
      passed: false,
      wordCount,
      reason:
        "Too short"
    };
  }

  if (
    wordCount > 2500
  ) {
    return {
      passed: false,
      wordCount,
      reason:
        "Too long"
    };
  }

  return {
    passed: true,
    wordCount,
    reason:
      "Quality check passed"
  };
}

/* =====================================================
   TTS
===================================================== */

const TTS_MODELS = [
  "gemini-3.1-flash-tts-preview",
  "gemini-2.5-flash-preview-tts"
];

async function geminiTTS(
  model,
  text
) {
  const response =
    await fetchWithTimeout(
      `${GEMINI_API}/interactions`,
      {
        method: "POST",
        headers: {
          "Content-Type":
            "application/json",
          "x-goog-api-key":
            GEMINI_API_KEY
        },
        body:
          JSON.stringify({
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

  let data = null;

  try {
    data =
      JSON.parse(raw);
  } catch {}

  if (!response.ok) {
    const error =
      new Error(
        `TTS ${response.status}: ${
          data?.error?.message ||
          raw ||
          response.statusText
        }`
      );

    error.status =
      response.status;

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

  wav.write(
    "RIFF",
    0
  );

  wav.writeUInt32LE(
    36 + pcm.length,
    4
  );

  wav.write(
    "WAVE",
    8
  );

  wav.write(
    "fmt ",
    12
  );

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

  wav.write(
    "data",
    36
  );

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

async function generateVoice(
  script
) {
  let lastError = null;

  for (
    const model of TTS_MODELS
  ) {
    try {
      console.log(
        `Trying TTS model: ${model}`
      );

      const pcm =
        await retryRequest(
          () =>
            geminiTTS(
              model,
              script
            ),
          `TTS ${model}`,
          2
        );

      if (!pcm.length) {
        throw new Error(
          "TTS_EMPTY_AUDIO"
        );
      }

      return {
        audio:
          pcmToWav(pcm),
        model
      };

    } catch (error) {
      lastError =
        error;

      console.error(
        `TTS model failed: ${model} -> ${error.message}`
      );

      await sleep(1000);
    }
  }

  throw (
    lastError ||
    new Error(
      "ALL_TTS_MODELS_FAILED"
    )
  );
}

/* =====================================================
   MAIN JOB
===================================================== */

async function runJob(
  id
) {
  if (
    runningJobs.has(id)
  ) {
    return;
  }

  runningJobs.add(id);

  try {
    let job =
      dbJob(
        await getJob(id)
      );

    if (!job) {
      console.log(
        `Job ${id} not found`
      );
      return;
    }

    await updateJob(
      id,
      {
        status: "running",
        error: "",
        attempts:
          job.attempts + 1
      }
    );

    /*
      SCRIPT
    */

    if (!job.script) {

      await saveProgress(
        job,
        "Generating original script",
        10
      );

      const result =
        await generateScript(
          job.topic
        );

      await updateJob(
        id,
        {
          script:
            result.script,
          scriptModel:
            result.model,
          stage:
            `Script generated — ${result.model}`,
          progress: 40
        }
      );

      job.script =
        result.script;

      job.scriptModel =
        result.model;

      job.progress =
        40;

    } else {

      console.log(
        `[${id}] Saved script found. Skipping script generation.`
      );

      await saveProgress(
        job,
        "Saved script recovered",
        40
      );
    }

    /*
      QUALITY
    */

    if (!job.wordCount) {

      const quality =
        checkScript(
          job.script
        );

      if (
        !quality.passed
      ) {
        throw new Error(
          `QUALITY_FAILED_${quality.reason}`
        );
      }

      await updateJob(
        id,
        {
          wordCount:
            quality.wordCount,
          stage:
            `Quality check passed — ${quality.wordCount} words`,
          progress: 50
        }
      );

      job.wordCount =
        quality.wordCount;

    } else {

      await saveProgress(
        job,
        `Quality already passed — ${job.wordCount} words`,
        50
      );
    }

    /*
      TTS
    */

    await saveProgress(
      job,
      "Generating AI narration",
      55
    );

    const voice =
      await generateVoice(
        job.script
      );

    await updateJob(
      id,
      {
        ttsModel:
          voice.model,
        stage:
          `Narration ready — ${voice.model}`,
        progress: 80
      }
    );

    job.ttsModel =
      voice.model;

    /*
      Telegram audio send
    */

    await sendAudio(
      job.chatId,
      voice.audio,
      `${job.id}.wav`
    );

    await updateJob(
      id,
      {
        status:
          "completed",
        stage:
          "Test completed successfully",
        progress: 100
      }
    );

    await sendMessage(
      job.chatId,
      [
        "✅ JOB COMPLETED",
        "",
        `Topic: ${job.topic}`,
        `Script: ${job.scriptModel || "saved script"}`,
        `TTS: ${job.ttsModel}`,
        `Words: ${job.wordCount}`,
        "",
        "🎧 Audio sent successfully."
      ].join("\n")
    );

  } catch (error) {

    console.error(
      `[${id}] FINAL ERROR`,
      error
    );

    const job =
      dbJob(
        await getJob(id)
      );

    if (!job) return;

    await updateJob(
      id,
      {
        status:
          "paused",
        stage:
          "Paused safely",
        error:
          error.message
      }
    );

    try {
      await sendMessage(
        job.chatId,
        [
          isQuotaError(error)
            ? "⏸️ FREE QUOTA PAUSED"
            : "⏸️ JOB PAUSED SAFELY",
          "",
          `Topic: ${job.topic}`,
          "",
          `Reason: ${error.message}`,
          "",
          "No paid fallback was used.",
          "Saved job state is preserved.",
          "",
          `Use /resume ${job.id} to retry.`
        ].join("\n")
      );
    } catch {}
  } finally {
    runningJobs.delete(id);
  }
}

/* =====================================================
   TELEGRAM COMMANDS
===================================================== */

async function handleMessage(
  message
) {
  if (
    !message?.chat?.id
  ) {
    return;
  }

  const chatId =
    message.chat.id;

  const text =
    clean(message.text);

  if (!text) return;

  console.log(
    `Telegram: ${text}`
  );

  if (
    text === "/start"
  ) {

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

  if (
    text === "/status"
  ) {

    const result =
      await pool.query(`
        SELECT
          COUNT(*)::int AS total,
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
      `);

    const s =
      result.rows[0];

    await sendMessage(
      chatId,
      [
        "📊 SYSTEM STATUS",
        "",
        "Backend: ONLINE",
        `Running: ${s.running}`,
        `Paused: ${s.paused}`,
        `Completed: ${s.completed}`,
        `Total: ${s.total}`,
        "",
        "Payment mode: APPROVAL ONLY"
      ].join("\n")
    );

    return;
  }

  if (
    text === "/jobs"
  ) {

    const result =
      await pool.query(
        `
        SELECT *
        FROM jobs
        WHERE chat_id = $1
        ORDER BY created_at DESC
        LIMIT 10
        `,
        [String(chatId)]
      );

    if (
      !result.rows.length
    ) {
      await sendMessage(
        chatId,
        "No jobs found."
      );
      return;
    }

    await sendMessage(
      chatId,
      result.rows
        .map(row => {
          const job =
            dbJob(row);

          return [
            `ID: ${job.id}`,
            `Topic: ${job.topic}`,
            `Status: ${job.status}`,
            `Stage: ${job.stage}`,
            `Progress: ${job.progress}%`
          ].join("\n");
        })
        .join("\n\n")
    );

    return;
  }

  if (
    text.startsWith(
      "/resume "
    )
  ) {

    const id =
      clean(
        text.slice(8)
      );

    const job =
      dbJob(
        await getJob(id)
      );

    if (
      !job ||
      String(job.chatId) !==
        String(chatId)
    ) {
      await sendMessage(
        chatId,
        "Job not found."
      );
      return;
    }

    if (
      job.status ===
      "completed"
    ) {
      await sendMessage(
        chatId,
        "This job is already completed."
      );
      return;
    }

    await updateJob(
      id,
      {
        status:
          "queued",
        error: "",
        stage:
          "Resume requested"
      }
    );

    await sendMessage(
      chatId,
      `▶️ Resuming ${id} from saved state.`
    );

    runJob(id)
      .catch(error =>
        console.error(
          "Resume error:",
          error
        )
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
        .slice(8)
        .trim();

    if (!topic) {
      await sendMessage(
        chatId,
        "Please provide a topic."
      );
      return;
    }

    const row =
      await createJob(
        chatId,
        topic
      );

    const job =
      dbJob(row);

    await sendMessage(
      chatId,
      [
        "🎬 JOB CREATED",
        "",
        `ID: ${job.id}`,
        `Topic: ${topic}`,
        "",
        "Starting Creator Agent...",
        "💾 Job saved in PostgreSQL."
      ].join("\n")
    );

    runJob(job.id)
      .catch(error =>
        console.error(
          "Background job error:",
          error
        )
      );

    return;
  }

  await sendMessage(
    chatId,
    "Unknown command.\n\nUse /start."
  );
}

/* =====================================================
   WEBHOOK
===================================================== */

app.post(
  "/telegram/webhook",
  (req, res) => {

    res.sendStatus(200);

    if (
      req.body?.message
    ) {
      handleMessage(
        req.body.message
      ).catch(error =>
        console.error(
          "Webhook error:",
          error
        )
      );
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
      status:
        "online"
    });
  }
);

app.get(
  "/health",
  async (req, res) => {
    try {
      await pool.query(
        "SELECT 1"
      );

      res.json({
        ok: true,
        database:
          "connected",
        uptime:
          process.uptime()
      });

    } catch (error) {

      res.status(503)
        .json({
          ok: false,
          database:
            "error",
          error:
            error.message
        });
    }
  }
);

/* =====================================================
   TELEGRAM WEBHOOK SETUP
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
    `${BASE_URL.replace(
      /\/$/,
      ""
    )}/telegram/webhook`;

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
      drop_pending_updates:
        false
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
   RECOVER JOBS
===================================================== */

async function recoverJobs() {

  const result =
    await pool.query(`
      SELECT id
      FROM jobs
      WHERE status = 'queued'
      ORDER BY created_at ASC
      LIMIT 5
    `);

  if (
    !result.rows.length
  ) {
    console.log(
      "No jobs waiting for recovery."
    );
    return;
  }

  console.log(
    `Recovering ${result.rows.length} job(s)...`
  );

  for (
    const row of result.rows
  ) {

    runJob(row.id)
      .catch(error =>
        console.error(
          `Recovery error ${row.id}:`,
          error
        )
      );

    await sleep(500);
  }
}

/* =====================================================
   START
===================================================== */

async function start() {

  await initDatabase();

  app.listen(
    PORT,
    async () => {

      console.log(
        `AI YouTube Autopilot listening on port ${PORT}`
      );

      try {

        await setupWebhook();

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
}

start().catch(
  error => {
    console.error(
      "❌ FATAL STARTUP ERROR:",
      error
    );

    process.exit(1);
  }
);
