import express from "express";

const app = express();
app.use(express.json({ limit: "2mb" }));

const PORT = process.env.PORT || 10000;

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;

const BASE_URL =
  process.env.RENDER_EXTERNAL_URL ||
  process.env.BASE_URL ||
  "https://ai-youtube-autopilot-a24y.onrender.com";

const TELEGRAM_API =
  `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}`;

const GEMINI_API =
  "https://generativelanguage.googleapis.com/v1beta";

const jobs = new Map();

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
   TIMEOUT
===================================================== */

async function fetchWithTimeout(url, options = {}, timeoutMs = 45000) {
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
      `Telegram ${method}: ${data.description || response.statusText}`
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
      `Telegram sendAudio: ${data.description || response.statusText}`
    );
  }

  return data.result;
}

/* =====================================================
   GEMINI STANDARD TEXT API
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

function isTransient(error) {
  const message = String(error?.message || "");

  return (
    error?.status === 408 ||
    error?.status === 429 ||
    error?.status >= 500 ||
    message.includes("REQUEST_TIMEOUT") ||
    message.includes("fetch failed") ||
    message.includes("ECONNRESET") ||
    message.includes("503") ||
    message.includes("429")
  );
}

async function retryRequest(fn, label, attempts = 3) {
  let lastError;

  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      console.log(
        `${label} attempt ${attempt}/${attempts}`
      );

      return await fn();

    } catch (error) {
      lastError = error;

      console.error(
        `${label} failed attempt ${attempt}: ${error.message}`
      );

      if (!isTransient(error)) {
        throw error;
      }

      if (attempt < attempts) {
        // Exponential backoff + random jitter
        const base =
          Math.min(16000, 1500 * (2 ** (attempt - 1)));

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

/*
  Current stable models.
  Flash-Lite is deliberately included as a
  high-availability fallback.
*/

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
- No introduction like "welcome back"
- Do not mention being an AI
- Return ONLY the narration

Make every sentence useful and engaging.
      `.trim();

      const script = await retryRequest(
        () => geminiText(model, prompt),
        `SCRIPT ${model}`,
        2
      );

      if (script.length < 1200) {
        throw new Error(
          "SCRIPT_TOO_SHORT"
        );
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

      console.error(
        `Script model unavailable: ${model} -> ${error.message}`
      );

      // Immediately move to next model.
      await sleep(500);
    }
  }

  throw lastError ||
    new Error("ALL_SCRIPT_MODELS_FAILED");
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
    throw new Error(
      "TTS_AUDIO_MISSING"
    );
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
   VOICE GENERATION WITH FALLBACK
===================================================== */

async function generateVoice(script) {
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

      console.error(
        `TTS model failed: ${model} -> ${error.message}`
      );

      // Move automatically to next TTS model.
      await sleep(1000);
    }
  }

  throw lastError ||
    new Error("ALL_TTS_MODELS_FAILED");
}

/* =====================================================
   JOB PROGRESS
===================================================== */

async function progress(
  job,
  stage,
  percent
) {
  job.stage = stage;
  job.progress = percent;

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
   MAIN JOB
===================================================== */

async function runJob(job) {

  try {

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

    job.script =
      result.script;

    job.scriptModel =
      result.model;

    await progress(
      job,
      `Script generated — ${result.model}`,
      40
    );

    const quality =
      checkScript(
        job.script
      );

    if (!quality.passed) {
      throw new Error(
        `QUALITY_FAILED_${quality.reason}`
      );
    }

    job.wordCount =
      quality.wordCount;

    await progress(
      job,
      `Quality check passed — ${quality.wordCount} words`,
      50
    );

    await progress(
      job,
      "Generating AI narration",
      55
    );

    const voice =
      await generateVoice(
        job.script
      );

    job.ttsModel =
      voice.model;

    await progress(
      job,
      `Narration ready — ${voice.model}`,
      80
    );

    await sendAudio(
      job.chatId,
      voice.audio,
      `${job.id}.wav`
    );

    job.status =
      "completed";

    job.progress =
      100;

    job.stage =
      "Test completed successfully";

    await sendMessage(
      job.chatId,
      [
        "✅ JOB COMPLETED",
        "",
        `Topic: ${job.topic}`,
        `Script: ${job.scriptModel}`,
        `TTS: ${job.ttsModel}`,
        `Words: ${job.wordCount}`,
        "",
        "🎧 Audio sent successfully."
      ].join("\n")
    );

  } catch (error) {

    console.error(
      `[${job.id}] FINAL ERROR`,
      error
    );

    job.status =
      "paused";

    job.error =
      error.message;

    try {
      await sendMessage(
        job.chatId,
        [
          "⏸️ JOB PAUSED SAFELY",
          "",
          `Topic: ${job.topic}`,
          "",
          `Reason: ${error.message}`,
          "",
          "No paid fallback was used.",
          "The job state is preserved."
        ].join("\n")
      );
    } catch {}
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

  if (text === "/start") {

    await sendMessage(
      chatId,
      [
        "🤖 AI YouTube Autopilot",
        "",
        "/create <topic>",
        "/status",
        "/jobs",
        "",
        "Example:",
        "/create 5 surprising facts about space"
      ].join("\n")
    );

    return;
  }

  if (text === "/status") {

    const active =
      [...jobs.values()]
        .filter(
          j => j.status === "running"
        ).length;

    await sendMessage(
      chatId,
      [
        "📊 SYSTEM STATUS",
        "",
        "Backend: ONLINE",
        `Active jobs: ${active}`,
        `Total jobs: ${jobs.size}`,
        "Payment mode: APPROVAL ONLY"
      ].join("\n")
    );

    return;
  }

  if (text === "/jobs") {

    const list =
      [...jobs.values()]
        .filter(
          j => j.chatId === chatId
        )
        .slice(-10);

    if (!list.length) {
      await sendMessage(
        chatId,
        "No jobs found."
      );

      return;
    }

    await sendMessage(
      chatId,
      list.map(j =>
        [
          `ID: ${j.id}`,
          `Topic: ${j.topic}`,
          `Status: ${j.status}`,
          `Progress: ${j.progress}%`
        ].join("\n")
      ).join("\n\n")
    );

    return;
  }

  if (
    text.startsWith("/create ")
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

    const job = {
      id: jobId(),
      chatId,
      topic,
      status: "running",
      progress: 0,
      stage: "Job created",
      createdAt:
        new Date().toISOString()
    };

    jobs.set(
      job.id,
      job
    );

    await sendMessage(
      chatId,
      [
        "🎬 JOB CREATED",
        "",
        `ID: ${job.id}`,
        `Topic: ${topic}`,
        "",
        "Starting Creator Agent..."
      ].join("\n")
    );

    // Background execution.
    runJob(job)
      .catch(error => {
        console.error(
          "Background job error:",
          error
        );
      });

    return;
  }

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

    // Respond immediately.
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
      status:
        "online"
    });
  }
);

app.get(
  "/health",
  (req, res) => {
    res.json({
      ok: true,
      uptime:
        process.uptime(),
      jobs:
        jobs.size
    });
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

app.listen(
  PORT,
  async () => {

    console.log(
      `AI YouTube Autopilot listening on port ${PORT}`
    );

    try {

      await setupWebhook();

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
