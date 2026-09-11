import express from "express";
import pg from "pg";

const { Pool } = pg;

const app = express();
app.use(express.json({ limit: "2mb" }));

const PORT = process.env.PORT || 10000;

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const WEBHOOK_URL = process.env.WEBHOOK_URL;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL
    ? { rejectUnauthorized: false }
    : false,
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
   DATABASE INITIALIZATION
========================= */

async function initDatabase() {
  await db(`
    CREATE TABLE IF NOT EXISTS jobs (
      id TEXT PRIMARY KEY,
      topic TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'queued',
      progress INTEGER DEFAULT 0,
      stage TEXT DEFAULT 'queued',
      script TEXT,
      error TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  // IMPORTANT: fixes the exact error from your screenshot
  await db(`
    ALTER TABLE jobs
      ADD COLUMN IF NOT EXISTS tts_total_chunks INTEGER DEFAULT 0,
      ADD COLUMN IF NOT EXISTS tts_completed_chunks INTEGER DEFAULT 0,
      ADD COLUMN IF NOT EXISTS tts_current_chunk INTEGER DEFAULT 0
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

  console.log("PostgreSQL database initialized");
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

  const text = await response.text();

  let data;
  try {
    data = JSON.parse(text);
  } catch {
    throw new Error(`Telegram invalid response: ${text}`);
  }

  if (!data.ok) {
    throw new Error(`Telegram API error: ${text}`);
  }

  return data.result;
}

async function sendMessage(chatId, text) {
  return telegram("sendMessage", {
    chat_id: chatId,
    text,
    disable_web_page_preview: true,
  });
}

async function sendDocument(chatId, buffer, filename, caption = "") {
  const form = new FormData();

  form.append("chat_id", String(chatId));
  form.append(
    "document",
    new Blob([buffer], { type: "audio/wav" }),
    filename
  );

  if (caption) form.append("caption", caption);

  const response = await fetch(
    `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendDocument`,
    {
      method: "POST",
      body: form,
    }
  );

  const data = await response.json();

  if (!data.ok) {
    throw new Error(`Telegram document error: ${JSON.stringify(data)}`);
  }

  return data.result;
}

/* =========================
   GEMINI
========================= */

async function geminiRequest(model, body, timeoutMs = 90000) {
  if (!GEMINI_API_KEY) {
    throw new Error("GEMINI_API_KEY missing");
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

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

    const text = await response.text();

    if (!response.ok) {
      const error = new Error(`Gemini ${response.status}: ${text}`);
      error.status = response.status;
      throw error;
    }

    return JSON.parse(text);
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

async function retryGemini(model, body, timeoutMs = 90000) {
  let lastError;

  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      return await geminiRequest(model, body, timeoutMs);
    } catch (error) {
      lastError = error;

      const status = error.status;

      // Retry only transient errors.
      if (status !== 429 && status !== 500 && status !== 502 && status !== 503) {
        throw error;
      }

      if (attempt < 2) {
        const wait = 3000 + Math.floor(Math.random() * 3000);
        await sleep(wait);
      }
    }
  }

  throw lastError;
}

/* =========================
   SCRIPT GENERATION
========================= */

async function generateScript(topic) {
  const prompt = `
Create an original YouTube narration script about:

${topic}

Requirements:
- Completely original wording.
- Engaging hook.
- Clear factual explanations.
- No copied article wording.
- No fake citations.
- Suitable for a faceless YouTube video.
- Around 600-800 words.
- Natural narration style.
- Do not include stage directions.
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
        45000
      );

      const text =
        result?.candidates?.[0]?.content?.parts
          ?.map((p) => p.text || "")
          .join("")
          .trim();

      if (text) {
        return {
          script: text,
          model,
        };
      }
    } catch (error) {
      console.log(`Script model failed: ${model}`, error.message);
      lastError = error;
    }
  }

  throw lastError || new Error("All script models failed");
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
    throw new Error("Script too short for quality gate");
  }

  return words.length;
}

/* =========================
   TTS CHUNKING
========================= */

function splitIntoChunks(text, maxWords = 140) {
  const sentences = text
    .replace(/\s+/g, " ")
    .match(/[^.!?]+[.!?]+|[^.!?]+$/g) || [text];

  const chunks = [];
  let current = [];

  for (const sentence of sentences) {
    const words = sentence.trim().split(/\s+/);

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
   TTS USING INTERACTIONS API
========================= */

async function generateTTSChunk(text, model) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 120000);

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
      const error = new Error(`TTS ${response.status}: ${raw}`);
      error.status = response.status;
      throw error;
    }

    const result = JSON.parse(raw);

    const audioData = result?.output_audio?.data;

    if (!audioData) {
      throw new Error("TTS returned no audio data");
    }

    return Buffer.from(audioData, "base64");
  } catch (error) {
    if (error.name === "AbortError") {
      const e = new Error("TTS_REQUEST_TIMEOUT_120000MS");
      e.code = "TIMEOUT";
      throw e;
    }

    throw error;
  } finally {
    clearTimeout(timer);
  }
}

/* =========================
   WAV HELPERS
========================= */

function readUInt32LE(buffer, offset) {
  return buffer.readUInt32LE(offset);
}

function isWav(buffer) {
  return (
    buffer.length >= 12 &&
    buffer.toString("ascii", 0, 4) === "RIFF" &&
    buffer.toString("ascii", 8, 12) === "WAVE"
  );
}

function pcmToWav(pcm, sampleRate = 24000, channels = 1, bits = 16) {
  const blockAlign = channels * bits / 8;
  const byteRate = sampleRate * blockAlign;

  const header = Buffer.alloc(44);

  header.write("RIFF", 0);
  header.writeUInt32LE(36 + pcm.length, 4);
  header.write("WAVE", 8);

  header.write("fmt ", 12);
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20);
  header.writeUInt16LE(channels, 22);
  header.writeUInt32LE(sampleRate, 24);
  header.writeUInt32LE(byteRate, 28);
  header.writeUInt16LE(blockAlign, 32);
  header.writeUInt16LE(bits, 34);

  header.write("data", 36);
  header.writeUInt32LE(pcm.length, 40);

  return Buffer.concat([header, pcm]);
}

function normalizeAudio(buffer) {
  if (isWav(buffer)) return buffer;
  return pcmToWav(buffer);
}

/* =========================
   CONCATENATE WAV FILES
========================= */

function concatWavBuffers(buffers) {
  const normalized = buffers.map(normalizeAudio);

  if (normalized.length === 1) {
    return normalized[0];
  }

  const first = normalized[0];

  const sampleRate = readUInt32LE(first, 24);
  const channels = first.readUInt16LE(22);
  const bits = first.readUInt16LE(34);

  const pcmParts = normalized.map((wav) => {
    let offset = 12;

    while (offset + 8 <= wav.length) {
      const id = wav.toString("ascii", offset, offset + 4);
      const size = wav.readUInt32LE(offset + 4);

      if (id === "data") {
        return wav.subarray(offset + 8, offset + 8 + size);
      }

      offset += 8 + size;
    }

    throw new Error("WAV data chunk not found");
  });

  const pcm = Buffer.concat(pcmParts);

  return pcmToWav(pcm, sampleRate, channels, bits);
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
  const entries = Object.entries(fields);

  if (!entries.length) return;

  const values = [];
  const sets = [];

  entries.forEach(([key, value], index) => {
    values.push(value);
    sets.push(`${key} = $${index + 1}`);
  });

  values.push(id);

  await db(
    `
      UPDATE jobs
      SET ${sets.join(", ")}, updated_at = NOW()
      WHERE id = $${values.length}
    `,
    values
  );
}

/* =========================
   GENERATE AUDIO FOR JOB
========================= */

async function processTTS(job, chatId) {
  const chunks = splitIntoChunks(job.script, 140);

  await updateJob(job.id, {
    tts_total_chunks: chunks.length,
    stage: "tts",
    progress: 55,
    error: null,
  });

  for (let i = 0; i < chunks.length; i++) {
    const existing = await db(
      `
      SELECT audio_data, status
      FROM job_audio_chunks
      WHERE job_id = $1 AND chunk_index = $2
      `,
      [job.id, i]
    );

    if (
      existing.rows[0] &&
      existing.rows[0].status === "completed" &&
      existing.rows[0].audio_data
    ) {
      console.log(`TTS chunk ${i + 1} already exists`);
      continue;
    }

    await updateJob(job.id, {
      tts_current_chunk: i,
    });

    let generated = false;
    let lastError;

    for (const model of TTS_MODELS) {
      try {
        console.log(`Generating TTS chunk ${i + 1}/${chunks.length} using ${model}`);

        const audio = await generateTTSChunk(chunks[i], model);

        await db(
          `
          INSERT INTO job_audio_chunks
            (job_id, chunk_index, audio_data, mime_type, status, model)
          VALUES
            ($1, $2, $3, $4, 'completed', $5)
          ON CONFLICT (job_id, chunk_index)
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
            i,
            audio,
            "audio/wav",
            model,
          ]
        );

        generated = true;
        break;
      } catch (error) {
        console.log(
          `TTS model failed ${model}:`,
          error.message
        );

        lastError = error;

        if (
          error.status !== 429 &&
          error.status !== 500 &&
          error.status !== 502 &&
          error.status !== 503 &&
          error.code !== "TIMEOUT"
        ) {
          break;
        }

        await sleep(3000);
      }
    }

    if (!generated) {
      await updateJob(job.id, {
        status: "paused",
        stage: "tts",
        error: lastError?.message || "TTS failed",
      });

      if (chatId) {
        await sendMessage(
          chatId,
          `⏸️ JOB PAUSED SAFELY\n\nTTS chunk ${i + 1}/${chunks.length} failed.\n\nNo paid fallback was used.\n\nResume with:\n/resume ${job.id}`
        );
      }

      return false;
    }

    const completed = await db(
      `
      SELECT COUNT(*)::int AS count
      FROM job_audio_chunks
      WHERE job_id = $1 AND status = 'completed'
      `,
      [job.id]
    );

    const completedCount = completed.rows[0].count;

    const progress = Math.min(
      85,
      55 + Math.floor((completedCount / chunks.length) * 30)
    );

    await updateJob(job.id, {
      tts_completed_chunks: completedCount,
      progress,
    });
  }

  const audioRows = await db(
    `
    SELECT chunk_index, audio_data
    FROM job_audio_chunks
    WHERE job_id = $1 AND status = 'completed'
    ORDER BY chunk_index ASC
    `,
    [job.id]
  );

  if (audioRows.rows.length !== chunks.length) {
    throw new Error("Not all TTS chunks completed");
  }

  const audio = concatWavBuffers(
    audioRows.rows.map((row) => row.audio_data)
  );

  await updateJob(job.id, {
    status: "completed",
    stage: "tts_complete",
    progress: 100,
    error: null,
  });

  if (chatId) {
    await sendMessage(
      chatId,
      `✅ TTS COMPLETED\n\n${chunks.length} audio chunks generated successfully.`
    );

    if (audio.length <= 49 * 1024 * 1024) {
      await sendDocument(
        chatId,
        audio,
        `${job.id}.wav`,
        `🎙️ AI narration completed\nJob: ${job.id}`
      );
    } else {
      await sendMessage(
        chatId,
        "✅ Narration completed, but the WAV file is too large for Telegram's file limit."
      );
    }
  }

  return true;
}

/* =========================
   CREATE JOB
========================= */

async function createJob(topic, chatId) {
  const id = `job_${Date.now()}_${Math.floor(Math.random() * 1000)}`;

  await db(
    `
    INSERT INTO jobs
      (id, topic, status, progress, stage)
    VALUES
      ($1, $2, 'queued', 0, 'queued')
    `,
    [id, topic]
  );

  await sendMessage(
    chatId,
    `🎬 JOB CREATED\n\nTopic: ${topic}\n\nJob ID: ${id}`
  );

  processJob(id, chatId).catch(async (error) => {
    console.error("Background job error:", error);

    await updateJob(id, {
      status: "paused",
      error: error.message,
    });

    try {
      await sendMessage(
        chatId,
        `⏸️ JOB PAUSED SAFELY\n\nReason: ${error.message}\n\nResume with:\n/resume ${id}`
      );
    } catch {}
  });

  return id;
}

/* =========================
   PROCESS JOB
========================= */

async function processJob(id, chatId) {
  let job = await getJob(id);

  if (!job) {
    throw new Error("Job not found");
  }

  await updateJob(id, {
    status: "running",
    stage: "creator",
    progress: 5,
  });

  await sendMessage(
    chatId,
    `🎬 Creator Agent started\nProgress: 5%`
  );

  if (!job.script) {
    await updateJob(id, {
      stage: "script",
      progress: 10,
    });

    await sendMessage(
      chatId,
      `📝 Generating original script\nProgress: 10%`
    );

    const result = await generateScript(job.topic);

    await updateJob(id, {
      script: result.script,
      progress: 40,
      stage: "script_complete",
    });

    const wordCount = qualityCheck(result.script);

    await updateJob(id, {
      progress: 50,
      stage: "quality_checked",
    });

    await sendMessage(
      chatId,
      `🎬 Script generated — ${result.model}\nProgress: 40%\n\n🎬 Quality check passed — ${wordCount} words\nProgress: 50%`
    );

    job = await getJob(id);
  }

  await updateJob(id, {
    stage: "tts",
    progress: 55,
    status: "running",
  });

  await sendMessage(
    chatId,
    `🎙️ Generating AI narration\nProgress: 55%`
  );

  await processTTS(job, chatId);
}

/* =========================
   RESUME
========================= */

async function resumeJob(id, chatId) {
  const job = await getJob(id);

  if (!job) {
    await sendMessage(chatId, "❌ Job not found.");
    return;
  }

  if (job.status === "completed") {
    await sendMessage(chatId, "✅ This job is already completed.");
    return;
  }

  await sendMessage(
    chatId,
    `▶️ RESUMING JOB\n\n${id}\n\nSaved progress will be reused.`
  );

  processJob(id, chatId).catch(async (error) => {
    await updateJob(id, {
      status: "paused",
      error: error.message,
    });

    await sendMessage(
      chatId,
      `⏸️ JOB PAUSED SAFELY\n\nReason: ${error.message}\n\nResume with:\n/resume ${id}`
    );
  });
}

/* =========================
   STATUS
========================= */

async function status(chatId) {
  const result = await db(`
    SELECT
      COUNT(*) FILTER (WHERE status = 'running')::int AS running,
      COUNT(*) FILTER (WHERE status = 'paused')::int AS paused,
      COUNT(*) FILTER (WHERE status = 'completed')::int AS completed,
      COUNT(*)::int AS total
    FROM jobs
  `);

  const s = result.rows[0];

  await sendMessage(
    chatId,
    `🤖 AI YouTube Autopilot\n\nBackend: ONLINE\n\nRunning: ${s.running}\nPaused: ${s.paused}\nCompleted: ${s.completed}\nTotal: ${s.total}\n\nPayment mode: APPROVAL ONLY`
  );
}

/* =========================
   TELEGRAM WEBHOOK
========================= */

app.post("/telegram/webhook", async (req, res) => {
  res.sendStatus(200);

  try {
    const message = req.body?.message;

    if (!message?.chat?.id || !message?.text) {
      return;
    }

    const chatId = message.chat.id;
    const text = message.text.trim();

    if (text === "/start") {
      await sendMessage(
        chatId,
        `🤖 AI YouTube Autopilot\n\nONLINE ✅\n\nCommands:\n/create <topic>\n/status\n/resume <job_id>`
      );
      return;
    }

    if (text === "/status") {
      await status(chatId);
      return;
    }

    if (text.startsWith("/create ")) {
      const topic = text.substring(8).trim();

      if (!topic) {
        await sendMessage(chatId, "Use: /create <topic>");
        return;
      }

      await createJob(topic, chatId);
      return;
    }

    if (text.startsWith("/resume ")) {
      const id = text.substring(8).trim();

      if (!id) {
        await sendMessage(chatId, "Use: /resume <job_id>");
        return;
      }

      await resumeJob(id, chatId);
      return;
    }

    await sendMessage(
      chatId,
      "Unknown command.\n\nUse /start, /create <topic>, /status or /resume <job_id>"
    );
  } catch (error) {
    console.error("Webhook processing error:", error);
  }
});

/* =========================
   HEALTH
========================= */

app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "AI YouTube Autopilot",
    status: "online",
  });
});

/* =========================
   STARTUP
========================= */

async function startup() {
  try {
    await initDatabase();

    if (WEBHOOK_URL && TELEGRAM_BOT_TOKEN) {
      console.log("Setting Telegram webhook:", WEBHOOK_URL);

      await telegram("setWebhook", {
        url: WEBHOOK_URL,
        secret_token: process.env.TELEGRAM_WEBHOOK_SECRET || undefined,
        allowed_updates: ["message"],
      });

      console.log("Telegram webhook configured");
    }

    // Recover jobs that were running during a restart.
    const recovery = await db(`
      UPDATE jobs
      SET status = 'queued',
          updated_at = NOW()
      WHERE status = 'running'
      RETURNING id
    `);

    console.log(
      `Recovered ${recovery.rows.length} interrupted job(s)`
    );

    app.listen(PORT, () => {
      console.log(`AI YouTube Autopilot listening on port ${PORT}`);
      console.log("STARTUP COMPLETE");
    });
  } catch (error) {
    console.error("STARTUP FAILED:", error);
    process.exit(1);
  }
}

startup();
