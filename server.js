import express from "express";
import pg from "pg";
import { spawn } from "node:child_process";
import { mkdtemp, rm, readFile, mkdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import ffmpegPath from "ffmpeg-static";

const { Pool } = pg;
const app = express();

app.use(express.json({ limit: "2mb" }));

/* TELEGRAM_RESUME_SINGLE_HANDLER_FINAL */

/*
  FINAL TELEGRAM UPDATE GATE

  - Every Telegram update_id is processed only once.
  - Duplicate webhook delivery is acknowledged but ignored.
  - Non-resume messages continue to the normal conversational manager.
  - Resume commands are handled exactly once.
*/
app.use(async (req, res, next) => {
  if (req.method !== "POST") {
    return next();
  }

  const update = req.body || {};
  const updateId = Number(update?.update_id);

  if (!Number.isSafeInteger(updateId)) {
    return next();
  }

  const message =
    update?.message ||
    update?.edited_message ||
    update?.channel_post;

  const chatId =
    message?.chat?.id;

  const text =
    String(message?.text || "").trim();

  try {
    /*
      ATOMIC TELEGRAM UPDATE CLAIM

      Same update_id can be delivered more than once by Telegram.
      Only the first request gets the row.
    */
    const claimed = await db(
      `
      INSERT INTO telegram_updates
        (update_id)
      VALUES
        ($1)
      ON CONFLICT
        (update_id)
      DO NOTHING
      RETURNING update_id
      `,
      [updateId]
    );

    if (!claimed.rows.length) {
      console.log(
        `Ignoring duplicate Telegram update_id=${updateId}`
      );

      return res.sendStatus(200);
    }

    if (!chatId || !text) {
      return next();
    }

    const match =
      text.match(
        /^\/resume(?:@\w+)?(?:\s+([A-Za-z0-9_-]+))?$/i
      );

    /*
      Not a resume command:
      let Telegram Manager V3 handle it.
    */
    if (!match) {
      return next();
    }

    const requestedJobId =
      match[1] || null;

    /*
      For plain /resume, add a short atomic cooldown.

      This prevents a burst of repeated /resume updates from
      walking through several old paused jobs.
    */
    if (!requestedJobId) {
      const guard = await db(
        `
        INSERT INTO telegram_resume_guard
          (chat_id, last_resume_at)
        VALUES
          ($1, NOW())
        ON CONFLICT
          (chat_id)
        DO UPDATE SET
          last_resume_at = NOW()
        WHERE telegram_resume_guard.last_resume_at
              < NOW() - INTERVAL '15 seconds'
        RETURNING chat_id
        `,
        [chatId]
      );

      if (!guard.rows.length) {
        console.log(
          `Ignoring rapid duplicate plain /resume chat=${chatId}`
        );

        return res.sendStatus(200);
      }
    }

    /*
      Telegram webhook gets its HTTP 200 immediately.
      All actual work continues in the background.
    */
    res.sendStatus(200);

    (async () => {
      try {
        let jobId = requestedJobId;

        if (!jobId) {
          const result = await db(
            `
            SELECT id
            FROM jobs
            WHERE chat_id = $1
              AND status = 'paused'
            ORDER BY updated_at DESC
            LIMIT 1
            `,
            [chatId]
          );

          if (!result.rows.length) {
            await sendMessage(
              chatId,
              "📭 Koi paused job nahi mili."
            ).catch(() => {});

            return;
          }

          jobId = result.rows[0].id;
        }

        await resumeJob(
          jobId,
          chatId
        );

      } catch (error) {
        console.error(
          "FINAL TELEGRAM RESUME ERROR:",
          error
        );

        await sendMessage(
          chatId,
          `⚠️ Resume process nahi ho saki.\n\n${String(error.message || error).slice(0, 500)}`
        ).catch(() => {});
      }
    })();

    return;
  } catch (error) {
    console.error(
      "TELEGRAM UPDATE GATE ERROR:",
      error
    );

    /*
      Do not break normal Telegram processing if the
      idempotency layer has a temporary DB problem.
    */
    return next();
  }
});

/* TELEGRAM_RESUME_SINGLE_HANDLER_FINAL_END */











/* TELEGRAM_MANAGER_V3_START */

function parseNaturalJobRequest(text) {
  const t = String(text || "").trim();

  const patterns = [
    /^(?:make|create|generate|banao|bana do|video banao|video bana do)\s+(?:a\s+)?(?:youtube\s+)?(?:video\s+)?(?:on|about|par|pe)\s+(.+)$/i,
    /^(?:youtube\s+)?video\s+(?:on|about|par|pe)\s+(.+)$/i,
    /^(.+?)\s+(?:par|pe|about|on)\s+(?:ek\s+)?video\s+(?:banao|bana do)$/i,
    /^(?:is topic par|iss topic par|is topic pe|iss topic pe)\s+video\s+(?:banao|bana do)\s*:?\s*(.+)$/i
  ];

  for (const p of patterns) {
    const m = t.match(p);

    if (m) {
      const topic = m[m.length - 1];

      if (topic) {
        return topic.trim().replace(/[.!?]+$/, "");
      }
    }
  }

  if (/^(?:make|create|generate|banao|bana do)\s+/i.test(t)) {
    const rest = t
      .replace(/^(?:make|create|generate|banao|bana do)\s+/i, "")
      .trim();

    if (
      rest.length >= 4 &&
      !/^(?:a\s+)?video\s*$/i.test(rest)
    ) {
      return rest
        .replace(/^(?:a\s+)?video\s+/i, "")
        .trim();
    }
  }

  return null;
}

async function telegramManagerGetJobs(chatId, limit = 10) {
  const result = await db(`
    SELECT
      id,
      topic,
      status,
      progress,
      stage,
      error,
      created_at,
      updated_at,
      tts_total_chunks,
      tts_completed_chunks
    FROM jobs
    WHERE chat_id = $1
    ORDER BY created_at DESC
    LIMIT $2
  `, [chatId, limit]);

  return result.rows;
}

async function telegramManagerTargetJob(chatId, text) {
  const jobs = await telegramManagerGetJobs(chatId, 10);

  if (!jobs.length) {
    return null;
  }

  const lower = String(text || "").toLowerCase();

  const idMatch = jobs.find(job =>
    lower.includes(String(job.id).toLowerCase())
  );

  if (idMatch) {
    return idMatch;
  }

  const topicMatches = jobs.filter(job => {
    const topic = String(job.topic || "").toLowerCase();
    return topic && lower.includes(topic);
  });

  if (topicMatches.length === 1) {
    return topicMatches[0];
  }

  return jobs[0];
}

function telegramManagerFormatJob(job) {
  if (!job) {
    return "📭 Koi job nahi mili.";
  }

  const progress = Number(job.progress || 0);

  const tts =
    job.tts_total_chunks
      ? `\n🎙️ TTS: ${job.tts_completed_chunks || 0}/${job.tts_total_chunks}`
      : "";

  const error =
    job.error
      ? `\n⚠️ Error: ${String(job.error).slice(0, 500)}`
      : "";

  return (
    `🎯 ${job.topic}\n` +
    `🆔 ${job.id}\n` +
    `📌 Status: ${job.status}\n` +
    `📈 Progress: ${progress}%\n` +
    `🔧 Stage: ${job.stage || "unknown"}` +
    tts +
    error
  );
}

async function telegramManagerV3(chatId, text) {
  const raw = String(text || "").trim();

  if (!raw) {
    return false;
  }

  /* =========================
     HELP
  ========================= */

  if (
    /\b(?:help|madad|kya kya kar sakte ho|kya kar sakte ho)\b/i
      .test(raw)
  ) {
    await sendMessage(
      chatId,
`🤖 AI YouTube Autopilot

Tum normal language me bol sakte ho:

🎬 "Black hole par ek video banao"
📊 "Mere latest jobs ka status batao"
📈 "Ye job kitna complete hua?"
▶️ "Isko resume karo"
🧠 "Mere channel ka analysis karo"
🧠 "Kya sahi hai aur kya improve karna chahiye?"
📰 "Aaj YouTube ki latest news kya hai?"
🕒 "Abhi kitne baje hain?"

Risky actions jaise publish, delete ya payment ke liye confirmation li jayegi.`
    );

    return true;
  }

  /* =========================
     GREETING
  ========================= */

  if (
    /^(?:hi|hello|hey|hii|namaste|salam)\b/i.test(raw)
  ) {
    await sendMessage(
      chatId,
      "👋 Hello! Main tumhara AI YouTube Manager hoon. Jo kaam chahiye normal language me bolo."
    );

    return true;
  }

  /* =========================
     TIME
  ========================= */

  if (
    /\b(?:kitne baje|time kya|abhi time|current time|what time)\b/i
      .test(raw)
  ) {
    const time =
      new Intl.DateTimeFormat("en-IN", {
        timeZone: "Asia/Kolkata",
        hour: "numeric",
        minute: "2-digit",
        second: "2-digit",
        hour12: true
      }).format(new Date());

    await sendMessage(
      chatId,
      `🕒 Abhi ${time} hai.\n🌍 Asia/Kolkata`
    );

    return true;
  }

  /* =========================
     DATE
  ========================= */

  if (
    /\b(?:aaj ki date|today date|today|date kya hai)\b/i
      .test(raw) &&
    !/news|update/i.test(raw)
  ) {
    const date =
      new Intl.DateTimeFormat("en-IN", {
        timeZone: "Asia/Kolkata",
        day: "2-digit",
        month: "long",
        year: "numeric"
      }).format(new Date());

    await sendMessage(
      chatId,
      `📅 Aaj ${date} hai.`
    );

    return true;
  }

  /* =========================
     JOB LIST
  ========================= */

  if (
    /\b(?:jobs?|kaam|videos?)\b/i.test(raw) &&
    /\b(?:status|progress|running|latest|recent|dikhao|batao|kitne)\b/i
      .test(raw)
  ) {
    const jobs =
      await telegramManagerGetJobs(chatId, 5);

    if (!jobs.length) {
      await sendMessage(
        chatId,
        "📭 Abhi tumhare chat se koi job nahi mili."
      );
    } else {
      const lines =
        jobs.map(
          (job, index) =>
            `${index + 1}. ${job.topic} — ${job.status} — ${job.progress}%`
        );

      await sendMessage(
        chatId,
        `📊 Latest jobs:\n\n${lines.join("\n")}`
      );
    }

    return true;
  }

  /* =========================
     SINGLE JOB STATUS
  ========================= */

  if (
    /\b(?:status batao|status kya hai|kitna hua|kahan tak|progress batao|latest video)\b/i
      .test(raw)
  ) {
    const job =
      await telegramManagerTargetJob(
        chatId,
        raw
      );

    await sendMessage(
      chatId,
      job
        ? telegramManagerFormatJob(job)
        : "📭 Is chat ke liye koi job nahi mili."
    );

    return true;
  }

  /* =========================
     RESUME
  ========================= */

  if (
    /\b(?:resume|chalu|continue|dobara chalao|start again)\b/i
      .test(raw) &&
    /\b(?:job|video|isko|isey|ise)\b/i.test(raw)
  ) {
    const job =
      await telegramManagerTargetJob(
        chatId,
        raw
      );

    if (!job) {
      await sendMessage(
        chatId,
        "📭 Resume karne ke liye koi recent job nahi mili."
      );

      return true;
    }

    if (job.status !== "paused") {
      await sendMessage(
        chatId,
        `ℹ️ Ye job paused nahi hai.\n\n${telegramManagerFormatJob(job)}`
      );

      return true;
    }

    await sendMessage(
      chatId,
      `▶️ ${job.topic} ko resume kar raha hoon.\nSaved work reuse hoga.`
    );

    resumeJob(job.id, chatId).catch(async error => {
      await updateJob(job.id, {
        status: "paused",
        error: error.message
      }).catch(() => {});

      await sendMessage(
        chatId,
        `⏸️ Resume ke dauran job safely pause ho gayi.\n⚠️ ${error.message.slice(0, 500)}`
      ).catch(() => {});
    });

    return true;
  }

  /* =========================
     NATURAL VIDEO CREATION
  ========================= */

  const topic =
    parseNaturalJobRequest(raw);

  if (topic) {
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
        ($1, $2, $3, 'queued', 0, 'queued')
      `,
      [id, topic, chatId]
    );

    await sendMessage(
      chatId,
`🎬 Job created

Topic: ${topic}
🆔 ${id}

📝 Script
↓
🎙️ Narration
↓
🎬 Video
↓
📝 Captions
↓
🖼️ Thumbnail`
    );

    processJob(
      id,
      chatId
    ).catch(async error => {
      await updateJob(id, {
        status: "paused",
        error: error.message
      }).catch(() => {});

      await sendMessage(
        chatId,
        `⏸️ Job safely paused because of an error.

🆔 ${id}
⚠️ ${error.message.slice(0, 500)}`
      ).catch(() => {});
    });

    return true;
  }

  /* =========================
     AI YOUTUBE MANAGER
  ========================= */

  if (
    /\b(?:analysis|analyze|analyse|kya sahi|kya galat|next 24|next kya|channel|youtube algorithm|algorithm|performance|perform|kaunsi video|best video|thumbnail|ctr|retention|views|improve|improvement)\b/i
      .test(raw)
  ) {
    const jobs =
      await telegramManagerGetJobs(
        chatId,
        10
      );

    const jobContext =
      jobs.map(job => ({
        id: job.id,
        topic: job.topic,
        status: job.status,
        progress: job.progress,
        stage: job.stage,
        error: job.error,
        created_at: job.created_at,
        updated_at: job.updated_at
      }));

    try {
      const result =
        await retryGemini(
          "gemini-3.5-flash",
          {
            contents: [
              {
                role: "user",
                parts: [
                  {
                    text:
`You are the user's practical YouTube Manager.

Answer in concise Hinglish.

Rules:
- Never claim secret YouTube algorithm knowledge.
- Never invent YouTube Analytics.
- Job status is NOT proof of video performance.
- If YouTube Analytics is not connected, clearly say that.
- Give useful next steps.
- Separate facts from suggestions.
- Never publish, delete, or spend money without explicit approval.

User question:
${raw}

Available job data:
${JSON.stringify(jobContext)}`
                  }
                ]
              }
            ],
            generationConfig: {
              temperature: 0.4,
              maxOutputTokens: 700
            }
          },
          30000,
          1
        );

      const answer =
        result?.candidates?.[0]?.content?.parts
          ?.map(part => part.text || "")
          .join("")
          .trim();

      if (answer) {
        await sendMessage(
          chatId,
          `🧠 YouTube Manager\n\n${answer}`
        );

        return true;
      }
    } catch (error) {
      console.log(
        "Telegram Manager V3 analysis error:",
        error.message
      );

      await sendMessage(
        chatId,
        "⚠️ Analysis abhi available nahi hua. Job status aur video creation phir bhi available hain."
      );

      return true;
    }
  }

  return false;
}

/* TELEGRAM_MANAGER_V3_END */






/* TELEGRAM_FAST_INTERCEPT_V1 */

/*
  This middleware runs before the older Telegram route.
  It guarantees that simple conversational messages reach
  conversationalManager first.
*/
app.use(async (req, res, next) => {
  try {
    if (req.method !== "POST") {
      return next();
    }

    const update = req.body;

    const message =
      update?.message ||
      update?.edited_message ||
      update?.channel_post;

    const chatId = message?.chat?.id;
    const text = message?.text;

    if (!chatId || !text) {
      return next();
    }

    /*
      conversationalManager handles:
      - time/date locally
      - news via fresh source
      - simple status via PostgreSQL
      - complex questions via Gemini
    */
    if (typeof telegramManagerV3 === "function") {
      const handled = await telegramManagerV3(
        chatId,
        String(text)
      );

      if (handled) {
        return res.sendStatus(200);
      }
    }

    return next();
  } catch (error) {
    console.log(
      "Telegram fast intercept error:",
      error.message
    );

    /*
      Never break the original Telegram route because
      of the conversational layer.
    */
    return next();
  }
});


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
    CREATE TABLE IF NOT EXISTS telegram_updates (
      update_id BIGINT PRIMARY KEY,
      received_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await db(`
    CREATE TABLE IF NOT EXISTS telegram_resume_guard (
      chat_id BIGINT PRIMARY KEY,
      last_resume_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await db(`
    DELETE FROM telegram_updates
    WHERE received_at < NOW() - INTERVAL '14 days'
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
     LEGACY AUDIO COLUMN FIX
  ========================= */

  await db(`
    DO $$
    BEGIN
      IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'job_audio_chunks'
          AND column_name = 'audio'
      ) THEN
        ALTER TABLE job_audio_chunks
        ALTER COLUMN audio DROP NOT NULL;
      END IF;
    END $$;
  `);

  /* =========================
     VIDEO MEDIA
  ========================= */

  await db(`
    CREATE TABLE IF NOT EXISTS job_media (
      job_id TEXT NOT NULL,
      kind TEXT NOT NULL,
      data BYTEA NOT NULL,
      mime_type TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      PRIMARY KEY (job_id, kind)
    )
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


const PROGRESS_DEDUPE_V1 = new Map();

async function sendMessage(chatId, text) {
  if (!chatId) return;

  const messageText = String(text || "");

  /*
    Progress updates are useful, but identical progress messages
    arriving repeatedly create Telegram spam.
    Keep a short per-chat dedupe window.
  */
  if (/Progress:\s*\d+%/i.test(messageText)) {
    const key = `${chatId}:${messageText}`;
    const now = Date.now();
    const previous = PROGRESS_DEDUPE_V1.get(key) || 0;

    if (now - previous < 5 * 60 * 1000) {
      console.log("Skipping duplicate Telegram progress message:", messageText);
      return;
    }

    PROGRESS_DEDUPE_V1.set(key, now);

    // Prevent unlimited in-memory growth.
    if (PROGRESS_DEDUPE_V1.size > 500) {
      for (const [k, t] of PROGRESS_DEDUPE_V1) {
        if (now - t > 10 * 60 * 1000) {
          PROGRESS_DEDUPE_V1.delete(k);
        }
      }
    }
  }

  return telegram("sendMessage", {
    chat_id: chatId,
    text: messageText,
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
  const basePrompt = `
Create a complete ORIGINAL YouTube narration script about:

${topic}

This is for a long-form faceless YouTube video.

MANDATORY REQUIREMENTS:
- Write 600 to 900 words.
- Do NOT write a short answer.
- Do NOT summarize.
- Do NOT explain what you are going to write.
- Output ONLY the finished narration.
- Strong opening hook in the first paragraph.
- Clear, interesting explanations.
- Natural spoken narration.
- Add useful detail, examples, comparisons or context where appropriate.
- Completely original wording.
- No copied article wording.
- No fake citations.
- No stage directions.
- No headings such as "Introduction", "Conclusion", or "Scene".
- The final response must be suitable to read aloud directly.

IMPORTANT:
If your first draft is too short, rewrite it before returning the answer.
Target approximately 750 words.
`;

  let lastError;

  for (const model of SCRIPT_MODELS) {
    for (let attempt = 1; attempt <= 2; attempt++) {
      try {
        const prompt =
          attempt === 1
            ? basePrompt
            : `${basePrompt}

RETRY INSTRUCTION:
Your previous generation was too short.
Generate a NEW complete script now.
It MUST contain at least 500 words and should be around 750 words.
Do not mention this retry instruction.`;

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
              maxOutputTokens: 4000,
            },
          },
          60000,
          2
        );

        const text = result?.candidates?.[0]?.content?.parts
          ?.map((part) => part.text || "")
          .join("")
          .trim();

        const wordCount = text
          ? text.split(/\s+/).filter(Boolean).length
          : 0;

        console.log(
          `Script attempt: model=${model}, attempt=${attempt}, words=${wordCount}`
        );

        if (text && wordCount >= 450) {
          return {
            script: text,
            model,
          };
        }

        console.log(
          `Script too short (${wordCount} words). Regenerating...`
        );
      } catch (error) {
        console.log(
          `Script model failed: ${model}, attempt=${attempt}: ${error.message}`
        );

        lastError = error;
      }
    }
  }

  throw lastError ||
    new Error(
      "Unable to generate a sufficiently long script after automatic regeneration"
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

  if (words.length < 450) {
    throw new Error(
      `Script too short for quality gate: ${words.length} words (minimum 450)`
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

const PIPER_VOICES_FINAL = [
  "en_US-lessac-medium",
  "en_US-lessac-low"
];

let GEMINI_TTS_QUOTA_BLOCKED_UNTIL = 0;

async function findPiperEspeakDataDir(dataDir) {
  const candidates = [
    process.env.ESPEAK_DATA_PATH,
    join(dataDir, "espeak-ng-data"),
    join(process.cwd(), "espeak-ng-data")
  ].filter(Boolean);

  for (const candidate of candidates) {
    try {
      const stat = await import("node:fs/promises").then(m => m.stat(candidate));
      if (stat.isDirectory()) return candidate;
    } catch {}
  }

  try {
    const discovered = await new Promise((resolve, reject) => {
      const child = spawn(
        "python3",
        [
          "-c",
          "import pathlib,piper_phonemize; print(pathlib.Path(piper_phonemize.__file__).resolve().parent / 'espeak-ng-data')"
        ],
        { stdio: ["ignore", "pipe", "pipe"] }
      );

      let stdout = "";
      let stderr = "";

      child.stdout.on("data", d => { stdout += d.toString(); });
      child.stderr.on("data", d => { stderr += d.toString(); });
      child.on("error", reject);

      child.on("close", code => {
        if (code === 0) resolve(stdout.trim());
        else reject(new Error(`Piper espeak probe failed: ${stderr.slice(-1000)}`));
      });
    });

    const candidate = String(discovered || "").trim();

    if (candidate) {
      const stat = await import("node:fs/promises").then(m => m.stat(candidate));
      if (stat.isDirectory()) return candidate;
    }
  } catch (error) {
    console.log("Piper espeak auto-detection failed:", error.message);
  }

  return null;
}

async function ensurePiperVoice(dataDir, voice) {
  await mkdir(dataDir, { recursive: true });

  const modelPath = join(dataDir, `${voice}.onnx`);
  const configPath = join(dataDir, `${voice}.onnx.json`);

  try {
    await readFile(modelPath);
    await readFile(configPath);

    return { modelPath, configPath };
  } catch {}

  console.log(`Piper voice missing; downloading ${voice}`);

  await new Promise((resolve, reject) => {
    const child = spawn(
      "python3",
      [
        "-m",
        "piper.download_voices",
        voice,
        "--data-dir",
        dataDir
      ],
      {
        stdio: ["ignore", "pipe", "pipe"]
      }
    );

    let stderr = "";

    child.stderr.on("data", d => {
      stderr += d.toString();
    });

    child.on("error", reject);

    child.on("close", code => {
      if (code === 0) {
        resolve();
      } else {
        reject(
          new Error(
            `Piper voice download failed (${voice}) exit=${code}: ${stderr.slice(-2000)}`
          )
        );
      }
    });
  });

  await readFile(modelPath);
  await readFile(configPath);

  return { modelPath, configPath };
}

async function runPiperVoice(
  text,
  voice,
  dataDir,
  outputPath
) {
  const { modelPath } =
    await ensurePiperVoice(
      dataDir,
      voice
    );

  const espeakDataDir =
    await findPiperEspeakDataDir(
      dataDir
    );

  const env = {
    ...process.env,
    ...(espeakDataDir
      ? {
          ESPEAK_DATA_PATH:
            espeakDataDir
        }
      : {})
  };

  /*
    Python module is tried first because the
    npm/pip installation is guaranteed to expose it.
    CLI is the secondary local runner.
  */
  const runners = [
    {
      name: "python-module",
      command: "python3",
      args: [
        "-m",
        "piper",
        "--model",
        modelPath,
        "--data-dir",
        dataDir,
        "--download-dir",
        dataDir,
        "--output_file",
        outputPath,
        "--sentence-silence",
        "0.05"
      ]
    },
    {
      name: "piper-command",
      command: "piper",
      args: [
        "--model",
        modelPath,
        "--data-dir",
        dataDir,
        "--download-dir",
        dataDir,
        "--output_file",
        outputPath,
        "--sentence-silence",
        "0.05"
      ]
    }
  ];

  let lastError;

  for (let attempt = 1; attempt <= 2; attempt++) {
    for (const runner of runners) {
      try {
        console.log(
          `Piper TTS attempt=${attempt} runner=${runner.name} voice=${voice}`
        );

        await new Promise((resolve, reject) => {
          const child = spawn(
            runner.command,
            runner.args,
            {
              env,
              stdio: [
                "pipe",
                "pipe",
                "pipe"
              ]
            }
          );

          let stderr = "";
          let stdout = "";

          child.stdout.on("data", d => {
            stdout += d.toString();
          });

          child.stderr.on("data", d => {
            stderr += d.toString();
          });

          child.on("error", reject);

          child.on("close", code => {
            if (code === 0) {
              resolve();
            } else {
              reject(
                new Error(
                  `Piper ${runner.name} exit=${code}: ${stderr.slice(-2500) || stdout.slice(-1000)}`
                )
              );
            }
          });

          child.stdin.end(
            String(text || "")
          );
        });

        const audio =
          await readFile(outputPath);

        if (
          !audio.length ||
          !isWav(audio)
        ) {
          throw new Error(
            `Piper ${runner.name} produced invalid/empty WAV`
          );
        }

        return {
          buffer: audio,
          mimeType: "audio/wav",
          model: `piper-${voice}`
        };

      } catch (error) {
        lastError = error;

        console.error(
          `Piper ${runner.name} failed:`,
          error.message
        );
      }
    }

    if (attempt < 2) {
      await sleep(1500);
    }
  }

  throw (
    lastError ||
    new Error(
      `Piper ${voice} failed`
    )
  );
}

async function generateLocalPiperTTS(text) {
  const workDir =
    await mkdtemp(
      join(
        tmpdir(),
        "piper-job-"
      )
    );

  const dataDir =
    process.env.PIPER_DATA_DIR ||
    join(
      process.cwd(),
      ".piper-voices"
    );

  const outputPath =
    join(
      workDir,
      "speech.wav"
    );

  try {
    await mkdir(
      dataDir,
      { recursive: true }
    );

    let lastError;

    for (
      const voice of PIPER_VOICES_FINAL
    ) {
      try {
        return await runPiperVoice(
          text,
          voice,
          dataDir,
          outputPath
        );
      } catch (error) {
        lastError = error;

        console.error(
          `Local Piper voice ${voice} failed:`,
          error.message
        );
      }
    }

    throw new Error(
      `Local Piper TTS failed for all voices: ${lastError?.message || "unknown error"}`
    );

  } finally {
    await rm(
      workDir,
      {
        recursive: true,
        force: true
      }
    ).catch(() => {});
  }
}

function parseRetrySeconds(message) {
  const match =
    String(message || "")
      .match(
        /retry in\s+([0-9]+(?:\.[0-9]+)?)s/i
      );

  if (!match) {
    return 60;
  }

  return Math.min(
    300,
    Math.max(
      15,
      Math.ceil(
        Number(match[1])
      )
    )
  );
}

async function generateTTSWithRetry(text) {
  /*
    FINAL TTS PROVIDER CHAIN

    1. Piper = free/local primary.
    2. Gemini = backup only.
    3. Gemini 429 = quota block, no hammering.
    4. No paid provider is auto-selected.
    5. If both free providers fail, pause safely.
  */

  let piperError;

  try {
    const result =
      await generateLocalPiperTTS(
        text
      );

    console.log(
      "TTS provider success: Piper",
      result.model
    );

    return result;

  } catch (error) {
    piperError = error;

    console.error(
      "TTS provider failed: Piper:",
      error.message
    );
  }

  if (
    Date.now() <
    GEMINI_TTS_QUOTA_BLOCKED_UNTIL
  ) {
    throw new Error(
      `TTS fallback exhausted. Piper failed: ${piperError?.message || "unknown"}. Gemini TTS is temporarily quota-blocked.`
    );
  }

  let geminiError;

  for (
    const model of TTS_MODELS
  ) {
    try {
      console.log(
        `TTS provider backup: Gemini model=${model}`
      );

      const result =
        await generateTTSChunk(
          text,
          model
        );

      console.log(
        `TTS provider success: Gemini model=${model}`
      );

      return {
        ...result,
        model
      };

    } catch (error) {
      geminiError = error;

      console.error(
        `Gemini TTS failed model=${model}:`,
        error.message
      );

      if (
        error.status === 429
      ) {
        const seconds =
          parseRetrySeconds(
            error.message
          );

        GEMINI_TTS_QUOTA_BLOCKED_UNTIL =
          Date.now() +
          seconds * 1000;

        break;
      }

      if (
        error.status === 400 ||
        error.status === 401 ||
        error.status === 403
      ) {
        break;
      }

      if (
        error.status === 500 ||
        error.status === 502 ||
        error.status === 503 ||
        error.code === "TIMEOUT"
      ) {
        await sleep(3000);
      }
    }
  }

  const finalError =
    new Error(
      `All free TTS providers failed. Piper: ${piperError?.message || "unavailable"} | Gemini: ${geminiError?.message || "unavailable"}`
    );

  finalError.code =
    "TTS_ALL_FREE_PROVIDERS_FAILED";

  throw finalError;
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
    320
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

Technical reason:
${String(error.message).slice(0, 800)}

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
    status: "running",
    stage: "tts_complete",
    progress: 70,
    tts_completed_chunks:
      chunks.length,
    error: null,
  });

  return audio;
}

/* =========================
   VIDEO GENERATION
========================= */

async function runFFmpeg(args, timeoutMs = 300000) {
  return new Promise((resolve, reject) => {
    const child = spawn(ffmpegPath, args, {
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stderr = "";

    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      reject(new Error(`FFMPEG_TIMEOUT_${timeoutMs}MS`));
    }, timeoutMs);

    child.stderr.on("data", d => {
      stderr += d.toString();
    });

    child.on("error", err => {
      clearTimeout(timer);
      reject(err);
    });

    child.on("close", code => {
      clearTimeout(timer);

      if (code === 0) {
        resolve({ stderr });
      } else {
        reject(
          new Error(
            `FFmpeg failed with code ${code}: ${stderr.slice(-3000)}`
          )
        );
      }
    });
  });
}

function wavDurationSeconds(wav) {
  const format = getWavFormat(wav);
  const pcm = getWavPcm(wav);

  if (!format.sampleRate || !format.channels || !format.bits) {
    return 1;
  }

  const bytesPerSecond =
    format.sampleRate *
    format.channels *
    (format.bits / 8);

  if (!bytesPerSecond) return 1;

  return Math.max(
    1,
    pcm.length / bytesPerSecond
  );
}

function createSrt(script, duration) {
  const words = String(script || "")
    .trim()
    .split(/\s+/)
    .filter(Boolean);

  if (!words.length) return "";

  const groups = [];

  for (let i = 0; i < words.length; i += 10) {
    groups.push(
      words.slice(i, i + 10).join(" ")
    );
  }

  const step =
    duration / groups.length;

  function stamp(seconds) {
    const totalMs =
      Math.max(0, Math.floor(seconds * 1000));

    const h =
      Math.floor(totalMs / 3600000);

    const m =
      Math.floor(
        (totalMs % 3600000) / 60000
      );

    const sec =
      Math.floor(
        (totalMs % 60000) / 1000
      );

    const ms =
      totalMs % 1000;

    return (
      String(h).padStart(2, "0") +
      ":" +
      String(m).padStart(2, "0") +
      ":" +
      String(sec).padStart(2, "0") +
      "," +
      String(ms).padStart(3, "0")
    );
  }

  return groups.map((text, i) => {
    const start = i * step;
    const end =
      Math.min(
        duration,
        (i + 1) * step
      );

    return (
      `${i + 1}\n` +
      `${stamp(start)} --> ${stamp(end)}\n` +
      `${text}\n`
    );
  }).join("\n");
}

function ffmpegFilterPath(path) {
  return String(path)
    .replace(/\\/g, "\\\\")
    .replace(/:/g, "\\:")
    .replace(/'/g, "\\'");
}

async function buildVideoPackage(job, audioBuffer) {
  const workDir = await mkdtemp(
    join(
      tmpdir(),
      `autopilot-video-${job.id}-`
    )
  );

  const audioFile =
    join(workDir, "narration.wav");

  const srtFile =
    join(workDir, "captions.srt");

  const videoFile =
    join(workDir, "video.mp4");

  const thumbFile =
    join(workDir, "thumbnail.jpg");

  const titleFile =
    join(workDir, "title.txt");

  try {
    const { writeFile } =
      await import("node:fs/promises");

    await writeFile(
      audioFile,
      audioBuffer
    );

    const duration =
      wavDurationSeconds(audioBuffer);

    const srt =
      createSrt(
        job.script,
        duration
      );

    await writeFile(
      srtFile,
      srt,
      "utf8"
    );

    await writeFile(
      titleFile,
      String(
        job.topic ||
        "Amazing Facts"
      ).slice(0, 100),
      "utf8"
    );

    const subtitlePath =
      ffmpegFilterPath(srtFile);

    await runFFmpeg([
      "-y",

      "-f",
      "lavfi",

      "-i",
      "color=c=0x101820:s=1280x720:r=30",

      "-i",
      audioFile,

      "-vf",
      `subtitles='${subtitlePath}':force_style='FontName=DejaVu Sans,FontSize=20,PrimaryColour=&H00FFFFFF,OutlineColour=&H00000000,Outline=2,Alignment=2,MarginV=55'`,

      "-c:v",
      "libx264",

      "-preset",
      "veryfast",

      "-pix_fmt",
      "yuv420p",

      "-c:a",
      "aac",

      "-b:a",
      "128k",

      "-shortest",

      videoFile
    ]);

    await runFFmpeg([
      "-y",

      "-f",
      "lavfi",

      "-i",
      "color=c=0x101820:s=1280x720",

      "-frames:v",
      "1",

      "-vf",
      `drawtext=fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf:textfile='${ffmpegFilterPath(titleFile)}':fontcolor=white:fontsize=52:x=(w-text_w)/2:y=(h-text_h)/2:box=1:boxcolor=black@0.45:boxborderw=24`,

      thumbFile
    ]);

    return {
      video:
        await readFile(videoFile),

      thumbnail:
        await readFile(thumbFile),

      captions:
        Buffer.from(srt, "utf8")
    };

  } finally {
    await rm(
      workDir,
      {
        recursive: true,
        force: true
      }
    ).catch(() => {});
  }
}

async function sendVideo(
  chatId,
  buffer,
  filename,
  caption = ""
) {
  if (!chatId) return;

  const form = new FormData();

  form.append(
    "chat_id",
    String(chatId)
  );

  form.append(
    "video",
    new Blob(
      [buffer],
      { type: "video/mp4" }
    ),
    filename
  );

  if (caption) {
    form.append(
      "caption",
      caption
    );
  }

  const response =
    await fetch(
      `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendVideo`,
      {
        method: "POST",
        body: form
      }
    );

  const data =
    await response.json();

  if (!data.ok) {
    throw new Error(
      `Telegram video error: ${JSON.stringify(data)}`
    );
  }

  return data.result;
}

/* =========================
   FULL JOB
========================= */

async function processJob(
  id,
  chatId = null
) {
  /* =========================
     DUPLICATE JOB PROTECTION
  ========================= */

  // Atomically claim queued/paused jobs.
  // This is safe with PostgreSQL connection pooling.
  const claim = await db(
    `UPDATE jobs
     SET status = 'running',
         updated_at = NOW()
     WHERE id = $1
       AND status = 'queued'
     RETURNING id`,
    [id]
  );

  if (!claim.rows.length) {
    console.log(`Job ${id} is already running or completed; duplicate worker skipped.`);
    return false;
  }

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

  /* =========================
     SCRIPT
  ========================= */

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

    job =
      await getJob(id);
  }

  /* =========================
     TTS
  ========================= */

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

  const audio =
    await processTTS(
      job,
      targetChat
    );

  if (!audio) {
    return;
  }

  /* =========================
     VIDEO
  ========================= */

  await updateJob(id, {
    stage: "video",
    progress: 75,
    status: "running",
    error: null,
  });

  await sendMessage(
    targetChat,
    "🎬 Generating video + captions\nProgress: 75%"
  );

  const existingMedia =
    await db(
      `
      SELECT
        kind,
        data,
        mime_type
      FROM job_media
      WHERE job_id = $1
        AND kind IN (
          'video',
          'thumbnail',
          'captions'
        )
      `,
      [id]
    );

  const mediaMap =
    new Map(
      existingMedia.rows.map(
        row => [row.kind, row]
      )
    );

  let media;

  /*
    If video outputs were already persisted before a crash,
    NEVER render them again.
  */
  if (
    mediaMap.has("video") &&
    mediaMap.has("thumbnail") &&
    mediaMap.has("captions")
  ) {
    console.log(
      `Reusing persisted media for job ${id}`
    );

    media = {
      video:
        mediaMap.get("video").data,

      thumbnail:
        mediaMap.get("thumbnail").data,

      captions:
        mediaMap.get("captions").data
    };

  } else {
    media =
      await buildVideoPackage(
        job,
        audio
      );
  }

  /* =========================
     SAVE MEDIA
  ========================= */

  await db(
    `
    INSERT INTO job_media
      (
        job_id,
        kind,
        data,
        mime_type
      )
    VALUES
      (
        $1,
        'video',
        $2,
        'video/mp4'
      )
    ON CONFLICT
      (job_id, kind)
    DO UPDATE SET
      data = EXCLUDED.data,
      mime_type = EXCLUDED.mime_type,
      updated_at = NOW()
    `,
    [
      id,
      media.video
    ]
  );

  await db(
    `
    INSERT INTO job_media
      (
        job_id,
        kind,
        data,
        mime_type
      )
    VALUES
      (
        $1,
        'thumbnail',
        $2,
        'image/jpeg'
      )
    ON CONFLICT
      (job_id, kind)
    DO UPDATE SET
      data = EXCLUDED.data,
      mime_type = EXCLUDED.mime_type,
      updated_at = NOW()
    `,
    [
      id,
      media.thumbnail
    ]
  );

  await db(
    `
    INSERT INTO job_media
      (
        job_id,
        kind,
        data,
        mime_type
      )
    VALUES
      (
        $1,
        'captions',
        $2,
        'application/x-subrip'
      )
    ON CONFLICT
      (job_id, kind)
    DO UPDATE SET
      data = EXCLUDED.data,
      mime_type = EXCLUDED.mime_type,
      updated_at = NOW()
    `,
    [
      id,
      media.captions
    ]
  );

  await updateJob(id, {
    stage: "video_complete",
    progress: 90,
    status: "running",
    error: null,
  });

  /* =========================
     TELEGRAM OUTPUT
  ========================= */

  if (
    media.video.length <=
    49 * 1024 * 1024
  ) {
    await sendVideo(
      targetChat,
      media.video,
      `${id}.mp4`,
      "🎬 Video generated with captions"
    );
  } else {
    await sendDocument(
      targetChat,
      media.video,
      `${id}.mp4`,
      "video/mp4",
      "🎬 Video generated"
    );
  }

  await sendDocument(
    targetChat,
    media.thumbnail,
    `${id}-thumbnail.jpg`,
    "image/jpeg",
    "🖼️ Thumbnail generated"
  );

  await sendDocument(
    targetChat,
    media.captions,
    `${id}.srt`,
    "application/x-subrip",
    "📝 Captions generated"
  );

  await updateJob(id, {
    stage: "completed",
    progress: 100,
    status: "completed",
    error: null,
  });

  await sendMessage(
    targetChat,
    `✅ VIDEO PIPELINE COMPLETED

Job: ${id}

🎙️ Narration: READY
🎬 Video: READY
📝 Captions: READY
🖼️ Thumbnail: READY

Next stage: YouTube upload.`
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

async function resumeJob(id, chatId) {
  const job = await getJob(id);

  if (!job) {
    if (chatId) {
      await sendMessage(
        chatId,
        "❌ Job not found."
      ).catch(() => {});
    }

    return false;
  }

  const targetChat =
    chatId || job.chat_id;

  /*
    NEVER restart a running job.
  */
  if (job.status === "running") {
    await sendMessage(
      targetChat,
      `ℹ️ Job already running.\n\n🆔 ${id}\n📈 Progress: ${job.progress || 0}%\n🔧 Stage: ${job.stage || "unknown"}`
    ).catch(() => {});

    return false;
  }

  /*
    NEVER restart a completed job.
  */
  if (job.status === "completed") {
    await sendMessage(
      targetChat,
      `✅ Job already completed.\n\n🆔 ${id}`
    ).catch(() => {});

    return false;
  }

  /*
    Queued means another worker/startup recovery already owns it.
  */
  if (job.status === "queued") {
    await sendMessage(
      targetChat,
      `⏳ Job already queued.\n\n🆔 ${id}\n📈 Progress: ${job.progress || 0}%`
    ).catch(() => {});

    return false;
  }

  if (job.status !== "paused") {
    await sendMessage(
      targetChat,
      `⚠️ Job cannot be resumed from status: ${job.status}\n\n🆔 ${id}`
    ).catch(() => {});

    return false;
  }

  /*
    ONLY legal resume transition:
      paused -> queued

    Two simultaneous resume requests cannot both claim it.
  */
  const claimed = await db(
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
    [id, targetChat]
  );

  if (!claimed.rows.length) {
    const latest =
      await getJob(id);

    await sendMessage(
      targetChat,
      latest?.status === "running"
        ? `ℹ️ Resume already started.\n\n🆔 ${id}`
        : `ℹ️ Job state changed to ${latest?.status || "unknown"}.\n\n🆔 ${id}`
    ).catch(() => {});

    return false;
  }

  await sendMessage(
    targetChat,
    `▶️ RESUMING JOB\n\n🆔 ${id}\n\nSaved script and completed TTS chunks will be reused.`
  ).catch(() => {});

  /*
    processJob now claims ONLY queued jobs.
    Therefore exactly one worker can take ownership.
  */
  processJob(
    id,
    targetChat
  ).catch(async error => {
    console.error(
      `Resume worker failed for ${id}:`,
      error
    );

    await updateJob(
      id,
      {
        status: "paused",
        error: error.message
      }
    ).catch(() => {});

    await sendMessage(
      targetChat,
      `⏸️ JOB PAUSED SAFELY\n\n🆔 ${id}\n\n⚠️ ${String(error.message).slice(0, 1000)}\n\nResume: /resume ${id}`
    ).catch(() => {});
  });

  return true;
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


/* CONVERSATIONAL_YOUTUBE_MANAGER_V2 */

/*
  FAST CONVERSATIONAL MANAGER

  Fast path:
  - time/date questions: local calculation, no Gemini
  - simple job/status questions: PostgreSQL only
  - news: fresh Google News RSS, no unnecessary AI delay
  - complex analysis: Gemini
*/

function fastTimeAnswer(text) {
  const q = String(text || "").toLowerCase().trim();

  const asksTime =
    /\b(time|samay|kitne baje|baje|waqt)\b/.test(q);

  const asksDate =
    /\b(date|today|aaj|kal|tomorrow|day|din)\b/.test(q);

  if (!asksTime && !asksDate) return null;

  let zone = "Asia/Kolkata";

  if (/\busa\b|\bunited states\b|\bnew york\b|\best\b/.test(q)) {
    zone = "America/New_York";
  } else if (/\blos angeles\b|\bcalifornia\b|\bpst\b/.test(q)) {
    zone = "America/Los_Angeles";
  } else if (/\blondon\b|\buk\b|\bgmt\b/.test(q)) {
    zone = "Europe/London";
  } else if (/\bdubai\b|\buae\b/.test(q)) {
    zone = "Asia/Dubai";
  }

  const now = new Date();

  const parts = new Intl.DateTimeFormat("en-IN", {
    timeZone: zone,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    day: "2-digit",
    month: "long",
    year: "numeric",
    weekday: "long",
    hour12: true
  }).formatToParts(now);

  const get = (type) =>
    parts.find(p => p.type === type)?.value || "";

  const time = `${get("hour")}:${get("minute")}:${get("second")} ${get("dayPeriod")}`;
  const date = `${get("weekday")}, ${get("day")} ${get("month")} ${get("year")}`;

  if (asksTime && asksDate) {
    return `🕒 ${time}\n📅 ${date}\n🌍 ${zone}`;
  }

  if (asksTime) {
    return `🕒 Abhi ${time} hai.\n🌍 ${zone}`;
  }

  return `📅 Aaj ${date} hai.`;
}

function isNewsQuestion(text) {
  return /\b(news|khabar|khabrein|latest|breaking|headlines|aaj ki khabar|latest news|current news)\b/i
    .test(String(text || ""));
}

async function getFastNews(chatId, text) {
  try {
    const query = String(text || "")
      .replace(/\b(latest|news|khabar|khabrein|breaking|headlines|aaj ki)\b/gi, " ")
      .replace(/\s+/g, " ")
      .trim();

    const rssUrl =
      "https://news.google.com/rss/search?q=" +
      encodeURIComponent(query || "India") +
      "&hl=en-IN&gl=IN&ceid=IN:en";

    const response = await fetch(rssUrl, {
      headers: { "User-Agent": "AI-YouTube-Autopilot/2.0" }
    });

    if (!response.ok) {
      throw new Error(`News RSS ${response.status}`);
    }

    const xml = await response.text();

    const items = [...xml.matchAll(
      /<item>([\s\S]*?)<\/item>/gi
    )].slice(0, 5);

    if (!items.length) {
      await sendMessage(
        chatId,
        "Abhi fresh news result nahi mila. Main galat khabar guess nahi karunga."
      );
      return true;
    }

    const clean = (v) =>
      v
        .replace(/<!\[CDATA\[|\]\]>/g, "")
        .replace(/<[^>]+>/g, "")
        .replace(/&amp;/g, "&")
        .replace(/&quot;/g, '"')
        .replace(/&#39;/g, "'")
        .replace(/&lt;/g, "<")
        .replace(/&gt;/g, ">")
        .trim();

    const headlines = items.map((m, i) => {
      const block = m[1];
      const title = clean(
        (block.match(/<title>([\s\S]*?)<\/title>/i) || [,""])[1]
      );
      const source = clean(
        (block.match(/<source[^>]*>([\s\S]*?)<\/source>/i) || [,""])[1]
      );

      return `${i + 1}. ${title}${source ? ` — ${source}` : ""}`;
    });

    await sendMessage(
      chatId,
      `📰 Fresh news:\n\n${headlines.join("\n")}\n\n_Source: Google News RSS; headlines are current-source results, not invented summaries._`
    );

    return true;
  } catch (error) {
    console.log("Fast news error:", error.message);

    await sendMessage(
      chatId,
      "Fresh news source abhi respond nahi kar raha. Main bina verify kiye news nahi bataunga."
    );

    return true;
  }
}

async function conversationalManager(chatId, text) {
  const q = String(text || "").trim();
  if (!q) return false;

  /*
    1. TIME / DATE = instant
  */
  const timeAnswer = fastTimeAnswer(q);

  if (timeAnswer) {
    await sendMessage(chatId, timeAnswer);
    return true;
  }

  /*
    2. NEWS = fresh source first
  */
  if (isNewsQuestion(q)) {
    return getFastNews(chatId, q);
  }

  /*
    3. SIMPLE AUTOMATION STATUS = database only
       No Gemini delay.
  */
  const lower = q.toLowerCase();

  const simpleStatus =
    /^(status|job status|mera status|mere jobs|video status|kya chal raha|abhi kya chal raha|progress|kitna hua)/i
      .test(q);

  if (simpleStatus) {
    try {
      const result = await db(`
        SELECT id, topic, status, progress, stage,
               tts_total_chunks, tts_completed_chunks
        FROM jobs
        WHERE chat_id = $1
        ORDER BY created_at DESC
        LIMIT 5
      `, [chatId]);

      if (!result.rows.length) {
        await sendMessage(chatId, "Abhi aapke chat se koi recent job nahi mili.");
        return true;
      }

      const lines = result.rows.map((j, i) =>
        `${i + 1}. ${j.topic}\n   ${j.status} • ${j.progress || 0}% • ${j.stage || "queued"}`
      );

      await sendMessage(
        chatId,
        `📊 Current status:\n\n${lines.join("\n\n")}`
      );

      return true;
    } catch (error) {
      console.log("Fast status error:", error.message);
    }
  }

  /*
    4. Only complex questions go to Gemini.
  */
  const asksAnalysis =
    /channel|video|performance|analytics|views|watch time|ctr|retention|subscriber|traffic|algorithm|thumbnail|title|script|kya sahi|kya galat|next|aage|fayda|benefit|check karo/i
      .test(lower);

  if (!asksAnalysis) return false;

  let latest = [];

  try {
    const result = await db(`
      SELECT id, topic, status, progress, stage,
             tts_total_chunks, tts_completed_chunks,
             created_at, updated_at
      FROM jobs
      WHERE chat_id = $1
      ORDER BY created_at DESC
      LIMIT 10
    `, [chatId]);

    latest = result.rows;
  } catch (error) {
    console.log("Conversational manager DB read:", error.message);
  }

  const jobSummary = latest.length
    ? latest.map((j, i) =>
        `${i + 1}. ${j.topic} — ${j.status}, ${j.progress}% (${j.stage})`
      ).join("\n")
    : "No recent automation jobs.";

  const prompt = `
You are the YouTube Manager for an AI YouTube automation system.

USER:
${q}

RECENT JOBS:
${jobSummary}

RULES:
- Reply naturally in Hindi/Hinglish.
- Be concise and useful.
- Do not claim secret YouTube algorithm knowledge.
- Never invent analytics numbers.
- If YouTube Analytics data is not connected, say so.
- Give practical next steps.
- Never publish, delete, spend money or change important settings without explicit approval.
- Never claim an action completed unless it actually completed.
`;

  try {
    const result = await retryGemini(
      SCRIPT_MODELS[0],
      {
        contents: [
          {
            role: "user",
            parts: [{ text: prompt }]
          }
        ],
        generationConfig: {
          temperature: 0.3,
          maxOutputTokens: 700
        }
      },
      30000,
      1
    );

    const answer = result?.candidates?.[0]?.content?.parts
      ?.map(part => part.text || "")
      .join("")
      .trim();

    if (answer) {
      await sendMessage(chatId, answer);
      return true;
    }
  } catch (error) {
    console.log("Fast conversational analysis error:", error.message);

    await sendMessage(
      chatId,
      "Analysis service abhi busy hai. Main bina verify kiye answer nahi dunga."
    );

    return true;
  }

  return false;
}

/* =========================
   PIPER RUNTIME PREFLIGHT
========================= */

async function verifyPiperRuntime() {
  const dataDir =
    process.env.PIPER_DATA_DIR ||
    join(
      process.cwd(),
      ".piper-voices"
    );

  try {
    await mkdir(
      dataDir,
      { recursive: true }
    );

    /*
      Verify the Python module itself is callable.
      This catches the exact Render PATH problem before
      a real job reaches TTS.
    */
    await new Promise((resolve, reject) => {
      const child = spawn(
        "python3",
        [
          "-m",
          "piper",
          "--help"
        ],
        {
          stdio: ["ignore", "ignore", "pipe"]
        }
      );

      let stderr = "";

      child.stderr.on(
        "data",
        chunk => {
          stderr += chunk.toString();
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
            resolve();
          } else {
            reject(
              new Error(
                `python3 -m piper failed, exit=${code}: ${stderr.slice(-1200)}`
              )
            );
          }
        }
      );
    });

    /*
      Verify at least the primary voice files exist.
      postinstall normally creates them.
    */
    await readFile(
      join(
        dataDir,
        "en_US-lessac-medium.onnx"
      )
    );

    await readFile(
      join(
        dataDir,
        "en_US-lessac-medium.onnx.json"
      )
    );

    console.log(
      "PIPER PREFLIGHT: READY"
    );

    return true;

  } catch (error) {
    console.error(
      "PIPER PREFLIGHT: NOT READY:",
      error.message
    );

    return false;
  }
}

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

    /*
      ROOT FIX: never re-queue a freshly running job on every
      Render restart. Only recover jobs that have been genuinely
      stale for at least 30 minutes.
    */
    const recovery =
      await db(`
        UPDATE jobs
        SET
          status = 'queued',
          updated_at = NOW()
        WHERE status = 'running'
          AND updated_at < NOW() - INTERVAL '30 minutes'
        RETURNING id
      `);

    console.log(
      `Recovered ${recovery.rows.length} genuinely stale job(s) to queued state`
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

        /*
          FINAL QUEUE WORKER

          Never start 10 jobs simultaneously.

          One worker processes one queued job at a time.
          PostgreSQL still provides the atomic job claim inside
          processJob(), so duplicate workers cannot own one job.

          This is especially important for:
          - Gemini quotas
          - Render free CPU/RAM
          - Piper CPU usage
          - FFmpeg CPU usage
        */
        setTimeout(async () => {
          let workerRunning = true;

          while (workerRunning) {
            try {
              const queued =
                await db(`
                  SELECT
                    id,
                    chat_id
                  FROM jobs
                  WHERE status = 'queued'
                  ORDER BY created_at ASC
                  LIMIT 1
                `);

              if (!queued.rows.length) {
                workerRunning = false;
                break;
              }

              const row =
                queued.rows[0];

              try {
                await processJob(
                  row.id,
                  row.chat_id
                );
              } catch (error) {
                console.error(
                  `Queue worker failed for ${row.id}:`,
                  error
                );

                await updateJob(
                  row.id,
                  {
                    status: "paused",
                    error: error.message
                  }
                ).catch(() => {});

                await sendMessage(
                  row.chat_id,
                  `⏸️ JOB PAUSED SAFELY

Reason:
${String(error.message).slice(0, 1200)}

Resume with:
/resume ${row.id}`
                ).catch(() => {});
              }

              /*
                Small yield between jobs.
              */
              await sleep(500);

            } catch (error) {
              console.error(
                "Queued-job worker database error:",
                error
              );

              /*
                Do not spin forever if PostgreSQL is temporarily
                unavailable.
              */
              await sleep(5000);
            }
          }

          console.log(
            "QUEUE WORKER: idle — no queued jobs"
          );
        }, 1000);

        /*
          Non-fatal Piper preflight after server startup.
        */
        verifyPiperRuntime().catch(
          error => console.error(
            "Piper preflight error:",
            error
          )
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
