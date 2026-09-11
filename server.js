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
  throw new Error("DATABASE_URL missing");
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: {
    rejectUnauthorized: false
  },
  max: 5
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
      stage TEXT DEFAULT 'Job created',
      progress INTEGER DEFAULT 0,

      script TEXT,
      script_model TEXT,
      word_count INTEGER,

      tts_model TEXT,
      tts_total_chunks INTEGER DEFAULT 0,
      tts_current_chunk INTEGER DEFAULT 0,

      error TEXT,

      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await db(`
    CREATE TABLE IF NOT EXISTS job_audio_chunks (
      job_id TEXT NOT NULL,
      chunk_index INTEGER NOT NULL,
      audio BYTEA NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),

      PRIMARY KEY(job_id, chunk_index),

      CONSTRAINT fk_job
        FOREIGN KEY(job_id)
        REFERENCES jobs(id)
        ON DELETE CASCADE
    )
  `);

  console.log("✅ PostgreSQL database initialized");
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

/* =====================================================
   TIMEOUT FETCH
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

  const response =
    await fetchWithTimeout(
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

  const data =
    await response.json();

  if (!response.ok || !data.ok) {
    throw new Error(
      `Telegram ${method}: ${
        data.description ||
        response.statusText
      }`
    );
  }

  return data.result;
}

async function sendMessage(chatId, text) {

  return telegram(
    "sendMessage",
    {
      chat_id: chatId,
      text
    }
  );
}

/*
  WAV is sent as a document.
  Telegram sendAudio expects MP3/M4A.
*/

async function sendAudioDocument(
  chatId,
  audioBuffer,
  filename
) {

  const form = new FormData();

  form.append(
    "chat_id",
    String(chatId)
  );

  form.append(
    "document",
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
      `${TELEGRAM_API}/sendDocument`,
      {
        method: "POST",
        body: form
      },
      60000
    );

  const data =
    await response.json();

  if (!response.ok || !data.ok) {
    throw new Error(
      `Telegram sendDocument: ${
        data.description ||
        response.statusText
      }`
    );
  }

  return data.result;
}

/* =====================================================
   GEMINI TEXT
===================================================== */

async function geminiText(
  model,
  prompt
) {

  const url =
    `${GEMINI_API}/models/${model}:generateContent`;

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

    const message =
      data?.error?.message ||
      raw ||
      response.statusText;

    const error =
      new Error(
        `Gemini ${response.status}: ${message}`
      );

    error.status =
      response.status;

    throw error;
  }

  const text =
    data?.candidates?.[0]
      ?.content
      ?.parts
      ?.map(p => p.text || "")
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
    String(error?.message || "")
      .toLowerCase();

  return (
    message.includes("quota") ||
    message.includes("exceeded your current") ||
    message.includes("free_tier")
  );
}

function isTransient(error) {

  if (isQuotaError(error)) {
    return false;
  }

  const message =
    String(error?.message || "");

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

      /*
        QUOTA ERROR:
        Never waste requests retrying the same
        exhausted free quota.
      */

      if (isQuotaError(error)) {
        throw error;
      }

      if (!isTransient(error)) {
        throw error;
      }

      if (attempt < attempts) {

        const base =
          Math.min(
            15000,
            1500 *
            (2 ** (attempt - 1))
          );

        const jitter =
          Math.floor(
            Math.random() * 1000
          );

        await sleep(
          base + jitter
        );
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
- Do not mention being an AI
- Return ONLY the narration

Make every sentence useful and engaging.
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

      lastError =
        error;

      console.error(
        `Script failed ${model}:`,
        error.message
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

function checkScript(script) {

  const words =
    script
      .split(/\s+/)
      .filter(Boolean);

  const wordCount =
    words.length;

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
    reason:
      "Quality check passed"
  };
}

/* =====================================================
   CHUNKING
===================================================== */

function splitIntoChunks(
  text,
  maxWords = 140
) {

  const sentences =
    text.match(
      /[^.!?]+[.!?]+|[^.!?]+$/g
    ) || [text];

  const chunks = [];

  let current = [];
  let count = 0;

  for (const sentence of sentences) {

    const words =
      sentence
        .trim()
        .split(/\s+/)
        .filter(Boolean);

    if (!words.length) {
      continue;
    }

    if (
      count > 0 &&
      count + words.length > maxWords
    ) {

      chunks.push(
        current.join(" ")
      );

      current = [];
      count = 0;
    }

    current.push(
      sentence.trim()
    );

    count += words.length;
  }

  if (current.length) {
    chunks.push(
      current.join(" ")
    );
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

async function geminiTTS(
  model,
  text
) {

  const url =
    `${GEMINI_API}/interactions`;

  const response =
    await fetchWithTimeout(
      url,
      {
        method: "POST",

        headers: {
          "Content-Type":
            "application/json",

          "x-goog-api-key":
            GEMINI_API_KEY
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

      /*
        Each request is now only ~140 words.
        90 seconds is a safety ceiling.
      */

      90000
    );

  const raw =
    await response.text();

  let data;

  try {
    data =
      JSON.parse(raw);
  } catch {
    data = null;
  }

  if (!response.ok) {

    const message =
      data?.error?.message ||
      raw ||
      response.statusText;

    const error =
      new Error(
        `TTS ${response.status}: ${message}`
      );

    error.status =
      response.status;

    throw error;
  }

  const output =
    data?.output_audio ||
    data?.outputAudio ||
    data?.audio;

  const base64 =
    output?.data;

  if (!base64) {
    throw new Error(
      "TTS_AUDIO_MISSING"
    );
  }

  return {
    audio:
      Buffer.from(
        base64,
        "base64"
      ),

    mimeType:
      output?.mime_type ||
      output?.mimeType ||
      "audio/wav",

    sampleRate:
      output?.sample_rate ||
      output?.sampleRate ||
      24000
  };
}

/* =====================================================
   WAV HELPERS
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

function wavData(buffer) {

  if (
    buffer.length >= 44 &&
    buffer.toString(
      "ascii",
      0,
      4
    ) === "RIFF"
  ) {

    return buffer.subarray(44);
  }

  return buffer;
}

function combineWavBuffers(
  buffers,
  sampleRate = 24000
) {

  if (!buffers.length) {
    throw new Error(
      "NO_AUDIO_CHUNKS"
    );
  }

  const pcmParts =
    buffers.map(
      wavData
    );

  const pcm =
    Buffer.concat(
      pcmParts
    );

  return pcmToWav(
    pcm,
    sampleRate,
    1
  );
}

/* =====================================================
   DATABASE JOB FUNCTIONS
===================================================== */

async function getJob(id) {

  const result =
    await db(
      `SELECT *
       FROM jobs
       WHERE id = $1`,
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

  if (!entries.length) {
    return;
  }

  const values = [];
  const sets = [];

  let index = 1;

  for (
    const [key, value]
    of entries
  ) {

    sets.push(
      `${key} = $${index}`
    );

    values.push(value);

    index++;
  }

  sets.push(
    `updated_at = NOW()`
  );

  values.push(id);

  await db(
    `UPDATE jobs
     SET ${sets.join(", ")}
     WHERE id = $${index}`,
    values
  );
}

/* =====================================================
   PROGRESS
===================================================== */

async function progress(
  job,
  stage,
  percent
) {

  job.stage =
    stage;

  job.progress =
    percent;

  await updateJob(
    job.id,
    {
      stage,
      progress: percent
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
   SAVE AUDIO CHUNK
===================================================== */

async function saveAudioChunk(
  jobIdValue,
  index,
  audio
) {

  await db(
    `
    INSERT INTO job_audio_chunks
      (job_id, chunk_index, audio)

    VALUES
      ($1, $2, $3)

    ON CONFLICT
      (job_id, chunk_index)

    DO NOTHING
    `,
    [
      jobIdValue,
      index,
      audio
    ]
  );
}

async function getAudioChunks(
  jobIdValue
) {

  const result =
    await db(
      `
      SELECT
        chunk_index,
        audio
      FROM job_audio_chunks
      WHERE job_id = $1
      ORDER BY chunk_index
      `,
      [jobIdValue]
    );

  return result.rows;
}

/* =====================================================
   TTS GENERATION WITH RESUME
===================================================== */

async function generateVoiceResumable(
  job
) {

  const chunks =
    splitIntoChunks(
      job.script,
      140
    );

  const total =
    chunks.length;

  await updateJob(
    job.id,
    {
      tts_total_chunks:
        total
    }
  );

  let current =
    Number(
      job.tts_current_chunk || 0
    );

  if (current >= total) {

    const existing =
      await getAudioChunks(
        job.id
      );

    if (
      existing.length === total
    ) {

      return combineWavBuffers(
        existing.map(
          x => x.audio
        )
      );
    }
  }

  for (
    let i = current;
    i < total;
    i++
  ) {

    const percent =
      55 +
      Math.floor(
        ((i + 1) / total) * 25
      );

    await progress(
      job,
      `Generating narration chunk ${i + 1}/${total}`,
      percent
    );

    let generated = null;
    let lastError = null;

    for (
      const model of TTS_MODELS
    ) {

      try {

        console.log(
          `TTS chunk ${i + 1}: ${model}`
        );

        generated =
          await retryRequest(
            () =>
              geminiTTS(
                model,
                chunks[i]
              ),
            `TTS ${model} chunk ${i + 1}`,
            2
          );

        job.tts_model =
          model;

        await updateJob(
          job.id,
          {
            tts_model:
              model
          }
        );

        break;

      } catch (error) {

        lastError =
          error;

        console.error(
          `TTS ${model} failed:`,
          error.message
        );

        /*
          If free quota is exhausted,
          immediately move to next
          legal model.
        */

        await sleep(500);
      }
    }

    if (!generated) {

      throw (
        lastError ||
        new Error(
          "ALL_TTS_MODELS_FAILED"
        )
      );
    }

    let wav =
      generated.audio;

    /*
      Gemini may return WAV or raw PCM.
    */

    if (
      generated.mimeType
        .includes("audio/wav")
    ) {

      wav =
        generated.audio;

    } else {

      wav =
        pcmToWav(
          generated.audio,
          generated.sampleRate ||
            24000,
          1
        );
    }

    await saveAudioChunk(
      job.id,
      i,
      wav
    );

    await updateJob(
      job.id,
      {
        tts_current_chunk:
          i + 1
      }
    );
  }

  const saved =
    await getAudioChunks(
      job.id
    );

  if (
    saved.length !== total
  ) {

    throw new Error(
      "AUDIO_CHUNKS_INCOMPLETE"
    );
  }

  return combineWavBuffers(
    saved.map(
      x => x.audio
    )
  );
}

/* =====================================================
   MAIN JOB
===================================================== */

const runningJobs =
  new Set();

async function runJob(
  jobIdValue
) {

  if (
    runningJobs.has(
      jobIdValue
    )
  ) {
    return;
  }

  runningJobs.add(
    jobIdValue
  );

  try {

    let job =
      await getJob(
        jobIdValue
      );

    if (!job) {
      throw new Error(
        "JOB_NOT_FOUND"
      );
    }

    await updateJob(
      job.id,
      {
        status: "running",
        error: null
      }
    );

    /*
      SCRIPT
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
          job.topic
        );

      await updateJob(
        job.id,
        {
          script:
            result.script,

          script_model:
            result.model
        }
      );

      job =
        await getJob(
          job.id
        );

      await progress(
        job,
        `Script generated — ${result.model}`,
        40
      );
    }

    /*
      QUALITY
    */

    const quality =
      checkScript(
        job.script
      );

    if (!quality.passed) {

      throw new Error(
        `QUALITY_FAILED_${quality.reason}`
      );
    }

    await updateJob(
      job.id,
      {
        word_count:
          quality.wordCount
      }
    );

    job =
      await getJob(
        job.id
      );

    await progress(
      job,
      `Quality check passed — ${quality.wordCount} words`,
      50
    );

    /*
      TTS
    */

    await progress(
      job,
      "Generating AI narration",
      55
    );

    const audio =
      await generateVoiceResumable(
        job
      );

    await progress(
      job,
      "Narration ready",
      82
    );

    /*
      SEND AUDIO
    */

    await progress(
      job,
      "Sending narration to Telegram",
      90
    );

    await sendAudioDocument(
      job.chat_id,
      audio,
      `${job.id}.wav`
    );

    /*
      COMPLETE
    */

    await updateJob(
      job.id,
      {
        status:
          "completed",

        stage:
          "Test completed successfully",

        progress:
          100,

        error:
          null
      }
    );

    await sendMessage(
      job.chat_id,
      [
        "✅ JOB COMPLETED",
        "",
        `Topic: ${job.topic}`,
        `Script: ${job.script_model}`,
        `TTS: ${job.tts_model || "Gemini TTS"}`,
        `Words: ${job.word_count}`,
        "",
        "🎧 Narration sent successfully."
      ].join("\n")
    );

  } catch (error) {

    console.error(
      `[${jobIdValue}] FINAL ERROR`,
      error
    );

    try {

      const job =
        await getJob(
          jobIdValue
        );

      if (job) {

        await updateJob(
          job.id,
          {
            status:
              "paused",

            error:
              error.message
          }
        );

        await sendMessage(
          job.chat_id,
          [
            "⏸️ JOB PAUSED SAFELY",
            "",
            `Topic: ${job.topic}`,
            "",
            `Reason: ${error.message}`,
            "",
            "💳 No paid fallback was used.",
            "",
            `Resume with:`,
            `/resume ${job.id}`
          ].join("\n")
        );
      }

    } catch (notifyError) {

      console.error(
        "Pause notification failed:",
        notifyError.message
      );
    }

  } finally {

    runningJobs.delete(
      jobIdValue
    );
  }
}

/* =====================================================
   RECOVERY
===================================================== */

async function recoverJobs() {

  const result =
    await db(
      `
      SELECT id
      FROM jobs
      WHERE status IN
        ('running', 'processing')
      ORDER BY created_at
      `
    );

  if (!result.rows.length) {

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

    await updateJob(
      row.id,
      {
        status: "queued"
      }
    );

    setTimeout(
      () => {
        runJob(
          row.id
        ).catch(
          console.error
        );
      },
      2000
    );
  }
}

/* =====================================================
   TELEGRAM COMMANDS
===================================================== */

async function handleMessage(
  message
) {

  if (!message?.chat?.id) {
    return;
  }

  const chatId =
    String(
      message.chat.id
    );

  const text =
    clean(
      message.text
    );

  if (!text) {
    return;
  }

  console.log(
    `Telegram: ${text}`
  );

  /* START */

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

  /* STATUS */

  if (
    text === "/status"
  ) {

    const result =
      await db(
        `
        SELECT
          COUNT(*) FILTER
            (WHERE status='running')
            AS running,

          COUNT(*) FILTER
            (WHERE status='paused')
            AS paused,

          COUNT(*) FILTER
            (WHERE status='completed')
            AS completed,

          COUNT(*) AS total

        FROM jobs
        WHERE chat_id = $1
        `,
        [chatId]
      );

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

  /* JOBS */

  if (
    text === "/jobs"
  ) {

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
        [chatId]
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

  /* RESUME */

  if (
    text.startsWith(
      "/resume "
    )
  ) {

    const id =
      text
        .slice(8)
        .trim();

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
      chatId
    ) {

      await sendMessage(
        chatId,
        "❌ This job does not belong to your account."
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

    await updateJob(
      id,
      {
        status:
          "queued",
        error:
          null
      }
    );

    await sendMessage(
      chatId,
      [
        "▶️ RESUME STARTED",
        "",
        `Job: ${id}`,
        `Current progress: ${job.progress}%`,
        "",
        "Already completed TTS chunks will NOT be regenerated."
      ].join("\n")
    );

    runJob(id)
      .catch(
        console.error
      );

    return;
  }

  /* CREATE */

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

    const id =
      jobId();

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
        (
          $1,
          $2,
          $3,
          'queued',
          'Job created',
          0
        )
      `,
      [
        id,
        chatId,
        topic
      ]
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

    runJob(id)
      .catch(
        console.error
      );

    return;
  }

  await sendMessage(
    chatId,
    [
      "Unknown command.",
      "",
      "Use /start."
    ].join("\n")
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
      ).catch(
        console.error
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

      const result =
        await db(
          "SELECT COUNT(*)::int AS jobs FROM jobs"
        );

      res.json({
        ok: true,
        uptime:
          process.uptime(),
        jobs:
          result.rows[0].jobs
      });

    } catch {

      res.status(500).json({
        ok: false,
        database:
          "error"
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
      url:
        webhookUrl,

      allowed_updates:
        ["message"],

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
