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

const GEMINI_URL =
  "https://generativelanguage.googleapis.com/v1beta/interactions";

const jobs = new Map();

/* =========================
   BASIC HELPERS
========================= */

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function makeJobId() {
  return `job_${Date.now()}_${Math.floor(Math.random() * 1000)}`;
}

function safeText(value) {
  return String(value ?? "").trim();
}

/* =========================
   TELEGRAM
========================= */

async function telegramRequest(method, body = {}) {
  const response = await fetch(`${TELEGRAM_API}/${method}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  });

  const data = await response.json();

  if (!response.ok || !data.ok) {
    throw new Error(
      `Telegram ${method} failed: ${data.description || response.statusText}`
    );
  }

  return data.result;
}

async function sendTelegramMessage(chatId, text) {
  return telegramRequest("sendMessage", {
    chat_id: chatId,
    text
  });
}

async function sendTelegramAudio(chatId, audioBuffer, filename = "narration.wav") {
  const form = new FormData();

  form.append("chat_id", String(chatId));

  form.append(
    "audio",
    new Blob([audioBuffer], { type: "audio/wav" }),
    filename
  );

  const response = await fetch(
    `${TELEGRAM_API}/sendAudio`,
    {
      method: "POST",
      body: form
    }
  );

  const data = await response.json();

  if (!response.ok || !data.ok) {
    throw new Error(
      `Telegram sendAudio failed: ${data.description || response.statusText}`
    );
  }

  return data.result;
}

/* =========================
   GEMINI
========================= */

async function geminiInteraction(payload) {
  const response = await fetch(GEMINI_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-goog-api-key": GEMINI_API_KEY
    },
    body: JSON.stringify(payload)
  });

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
      data?.message ||
      raw ||
      response.statusText;

    const error = new Error(
      `Gemini ${response.status}: ${message}`
    );

    error.status = response.status;
    throw error;
  }

  return data;
}

/* =========================
   TEXT EXTRACTION
========================= */

function extractTextFromGemini(data) {
  if (!data) return "";

  if (typeof data.output_text === "string") {
    return data.output_text.trim();
  }

  if (typeof data.text === "string") {
    return data.text.trim();
  }

  const pieces = [];

  if (Array.isArray(data.steps)) {
    for (const step of data.steps) {
      if (Array.isArray(step.content)) {
        for (const item of step.content) {
          if (typeof item.text === "string") {
            pieces.push(item.text);
          }
        }
      }
    }
  }

  if (pieces.length) {
    return pieces.join("\n").trim();
  }

  if (Array.isArray(data.output)) {
    for (const item of data.output) {
      if (typeof item.text === "string") {
        pieces.push(item.text);
      }

      if (Array.isArray(item.content)) {
        for (const content of item.content) {
          if (typeof content.text === "string") {
            pieces.push(content.text);
          }
        }
      }
    }
  }

  return pieces.join("\n").trim();
}

/* =========================
   SCRIPT GENERATION
========================= */

const SCRIPT_MODELS = [
  "gemini-3.8-flash",
  "gemini-3.7-flash",
  "gemini-3.6-flash",
  "gemini-3.5-flash",
  "gemini-2.5-flash"
];

async function generateScript(topic) {
  let lastError = null;

  for (const model of SCRIPT_MODELS) {
    try {
      console.log(`Trying Gemini script model: ${model}`);

      const result = await geminiInteraction({
        model,
        input: `
Create an original YouTube narration script about:

${topic}

Requirements:
- Completely original wording
- No copied article text
- Strong hook in the beginning
- Interesting storytelling
- Clear factual explanations
- Suitable for a faceless YouTube video
- No fake citations
- No unnecessary headings
- Natural spoken English
- Approximately 600-800 words

Return ONLY the narration script.
        `.trim()
      });

      const script = extractTextFromGemini(result);

      if (!script || script.length < 300) {
        throw new Error("Gemini returned empty or very short script");
      }

      console.log(
        `Script generated successfully with ${model}: ${script.length} chars`
      );

      return {
        script,
        model
      };
    } catch (error) {
      lastError = error;

      console.error(
        `Script model failed: ${model} -> ${error.message}`
      );

      await sleep(1000);
    }
  }

  throw lastError || new Error("All script models failed");
}

/* =========================
   QUALITY CHECK
========================= */

function qualityCheck(script) {
  const words = script
    .split(/\s+/)
    .filter(Boolean);

  const wordCount = words.length;

  if (wordCount < 250) {
    return {
      passed: false,
      reason: "Script is too short",
      wordCount
    };
  }

  if (wordCount > 2500) {
    return {
      passed: false,
      reason: "Script is unusually long",
      wordCount
    };
  }

  return {
    passed: true,
    reason: "Quality check passed",
    wordCount
  };
}

/* =========================
   AUDIO / TTS
========================= */

const TTS_MODELS = [
  "gemini-3.1-flash-tts-preview",
  "gemini-2.5-flash-preview-tts"
];

function extractAudioBase64(data) {
  if (!data) return null;

  if (data.output_audio?.data) {
    return data.output_audio.data;
  }

  if (data.outputAudio?.data) {
    return data.outputAudio.data;
  }

  if (data.audio?.data) {
    return data.audio.data;
  }

  if (Array.isArray(data.steps)) {
    for (const step of data.steps) {
      if (Array.isArray(step.content)) {
        for (const item of step.content) {
          if (
            item.type === "audio" &&
            typeof item.data === "string"
          ) {
            return item.data;
          }
        }
      }
    }
  }

  return null;
}

function pcmToWav(pcmBuffer, sampleRate = 24000, channels = 1) {
  const bitsPerSample = 16;

  const byteRate =
    sampleRate *
    channels *
    bitsPerSample / 8;

  const blockAlign =
    channels *
    bitsPerSample / 8;

  const buffer = Buffer.alloc(44 + pcmBuffer.length);

  buffer.write("RIFF", 0);
  buffer.writeUInt32LE(36 + pcmBuffer.length, 4);
  buffer.write("WAVE", 8);

  buffer.write("fmt ", 12);
  buffer.writeUInt32LE(16, 16);
  buffer.writeUInt16LE(1, 20);
  buffer.writeUInt16LE(channels, 22);
  buffer.writeUInt32LE(sampleRate, 24);
  buffer.writeUInt32LE(byteRate, 28);
  buffer.writeUInt16LE(blockAlign, 32);
  buffer.writeUInt16LE(bitsPerSample, 34);

  buffer.write("data", 36);
  buffer.writeUInt32LE(pcmBuffer.length, 40);

  pcmBuffer.copy(buffer, 44);

  return buffer;
}

async function generateVoice(script) {
  let lastError = null;

  for (const model of TTS_MODELS) {
    for (let attempt = 1; attempt <= 2; attempt++) {
      try {
        console.log(
          `TTS model=${model} attempt=${attempt}`
        );

        const result = await geminiInteraction({
          model,
          input: `
Synthesize the following YouTube narration as a natural professional narrator.

Voice:
- Clear
- Engaging
- Confident
- Natural American English
- Medium pace
- Suitable for a faceless YouTube documentary/facts video

IMPORTANT:
Speak ONLY the narration between the markers.

--- BEGIN NARRATION ---
${script}
--- END NARRATION ---
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
        });

        const base64Audio = extractAudioBase64(result);

        if (!base64Audio) {
          throw new Error(
            "TTS response did not contain audio data"
          );
        }

        const pcm = Buffer.from(base64Audio, "base64");

        if (!pcm.length) {
          throw new Error("TTS returned empty audio");
        }

        const wav = pcmToWav(pcm);

        console.log(
          `TTS succeeded with ${model}, bytes=${wav.length}`
        );

        return {
          audio: wav,
          model
        };

      } catch (error) {
        lastError = error;

        console.error(
          `TTS failed: ${model} attempt ${attempt} -> ${error.message}`
        );

        if (attempt < 2) {
          await sleep(2000);
        }
      }
    }

    console.log(
      `Switching TTS fallback after ${model}`
    );

    await sleep(1000);
  }

  throw lastError || new Error("All TTS models failed");
}

/* =========================
   JOB STATUS
========================= */

async function updateJob(job, patch) {
  Object.assign(job, patch);

  console.log(
    `[${job.id}] ${job.stage} ${job.progress}%`
  );

  if (job.chatId) {
    try {
      await sendTelegramMessage(
        job.chatId,
        `🎬 ${job.stage}\nProgress: ${job.progress}%`
      );
    } catch (error) {
      console.error(
        "Telegram progress update failed:",
        error.message
      );
    }
  }
}

/* =========================
   CREATE JOB
========================= */

async function runCreateJob(job) {
  try {
    await updateJob(job, {
      stage: "Creator Agent started",
      progress: 5,
      status: "running"
    });

    await updateJob(job, {
      stage: "Generating original script",
      progress: 10
    });

    const scriptResult =
      await generateScript(job.topic);

    job.script = scriptResult.script;
    job.scriptModel = scriptResult.model;

    await updateJob(job, {
      stage: `Script generated (${scriptResult.model})`,
      progress: 40
    });

    const quality =
      qualityCheck(job.script);

    if (!quality.passed) {
      throw new Error(
        `Quality check failed: ${quality.reason}`
      );
    }

    job.wordCount = quality.wordCount;

    await updateJob(job, {
      stage: `Quality check passed — ${quality.wordCount} words`,
      progress: 50
    });

    await updateJob(job, {
      stage: "Generating AI narration",
      progress: 55
    });

    const voiceResult =
      await generateVoice(job.script);

    job.ttsModel = voiceResult.model;

    await updateJob(job, {
      stage: `AI narration ready (${voiceResult.model})`,
      progress: 80
    });

    await sendTelegramAudio(
      job.chatId,
      voiceResult.audio,
      `${job.id}.wav`
    );

    await updateJob(job, {
      stage: "Test completed successfully",
      progress: 100,
      status: "completed"
    });

    await sendTelegramMessage(
      job.chatId,
      [
        "✅ JOB COMPLETED",
        "",
        `Topic: ${job.topic}`,
        `Script model: ${job.scriptModel}`,
        `TTS model: ${job.ttsModel}`,
        `Words: ${job.wordCount}`,
        "",
        "🎧 Narration audio भेज दिया गया है."
      ].join("\n")
    );

  } catch (error) {
    console.error(
      `[${job.id}] JOB FAILED:`,
      error
    );

    job.status = "paused";
    job.stage = "Paused safely";
    job.progress = Math.max(job.progress || 0, 55);
    job.error = error.message;

    try {
      await sendTelegramMessage(
        job.chatId,
        [
          "⏸️ JOB PAUSED",
          "",
          `Topic: ${job.topic}`,
          "",
          `Reason: ${error.message}`,
          "",
          "No paid fallback was used.",
          "Job state preserved."
        ].join("\n")
      );
    } catch {}
  }
}

/* =========================
   TELEGRAM COMMANDS
========================= */

async function handleTelegramMessage(message) {
  if (!message?.chat?.id) return;

  const chatId = message.chat.id;
  const text = safeText(message.text);

  if (!text) return;

  console.log(
    `Telegram message from ${chatId}: ${text}`
  );

  if (text === "/start") {
    await sendTelegramMessage(
      chatId,
      [
        "🤖 AI YouTube Autopilot",
        "",
        "Commands:",
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
    const activeJobs = [...jobs.values()]
      .filter(j => j.status === "running");

    await sendTelegramMessage(
      chatId,
      [
        "📊 SYSTEM STATUS",
        "",
        `Backend: ONLINE`,
        `Active jobs: ${activeJobs.length}`,
        `Total jobs: ${jobs.size}`,
        "Mode: FREE-FIRST"
      ].join("\n")
    );

    return;
  }

  if (text === "/jobs") {
    const userJobs = [...jobs.values()]
      .filter(j => j.chatId === chatId)
      .slice(-10);

    if (!userJobs.length) {
      await sendTelegramMessage(
        chatId,
        "No jobs yet."
      );
      return;
    }

    const lines = userJobs.map(j =>
      `${j.id}\n${j.topic}\n${j.status} — ${j.progress}%`
    );

    await sendTelegramMessage(
      chatId,
      lines.join("\n\n")
    );

    return;
  }

  if (text.startsWith("/create ")) {
    const topic =
      text.slice("/create ".length).trim();

    if (!topic) {
      await sendTelegramMessage(
        chatId,
        "Topic missing.\n\nExample:\n/create 5 surprising facts about space"
      );
      return;
    }

    const job = {
      id: makeJobId(),
      chatId,
      topic,
      status: "queued",
      stage: "Job created",
      progress: 0,
      createdAt: new Date().toISOString()
    };

    jobs.set(job.id, job);

    await sendTelegramMessage(
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

    // Run in background so webhook returns immediately.
    runCreateJob(job).catch(error => {
      console.error(
        "Background job error:",
        error
      );
    });

    return;
  }

  await sendTelegramMessage(
    chatId,
    [
      "Unknown command.",
      "",
      "Use /start to see available commands."
    ].join("\n")
  );
}

/* =========================
   WEBHOOK
========================= */

app.post("/telegram/webhook", (req, res) => {
  // IMPORTANT:
  // Respond immediately so Telegram does not keep retrying.
  res.sendStatus(200);

  const update = req.body;

  if (update?.message) {
    handleTelegramMessage(update.message)
      .catch(error => {
        console.error(
          "Webhook message handling error:",
          error
        );
      });
  }
});

/* =========================
   HEALTH
========================= */

app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "AI YouTube Autopilot",
    mode: "webhook",
    status: "online"
  });
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    uptime: process.uptime(),
    jobs: jobs.size
  });
});

/* =========================
   WEBHOOK SETUP
========================= */

async function configureTelegramWebhook() {
  if (!TELEGRAM_BOT_TOKEN) {
    throw new Error(
      "TELEGRAM_BOT_TOKEN environment variable is missing"
    );
  }

  if (!GEMINI_API_KEY) {
    throw new Error(
      "GEMINI_API_KEY environment variable is missing"
    );
  }

  const webhookUrl =
    `${BASE_URL.replace(/\/$/, "")}/telegram/webhook`;

  console.log(
    `Configuring Telegram webhook: ${webhookUrl}`
  );

  const result = await telegramRequest(
    "setWebhook",
    {
      url: webhookUrl,
      allowed_updates: ["message"],
      drop_pending_updates: false
    }
  );

  console.log(
    "Telegram webhook configured:",
    result
  );

  const info =
    await telegramRequest("getWebhookInfo");

  console.log(
    "Telegram webhook info:",
    JSON.stringify(info, null, 2)
  );
}

/* =========================
   START SERVER
========================= */

app.listen(PORT, async () => {
  console.log(
    `AI YouTube Autopilot listening on port ${PORT}`
  );

  try {
    await configureTelegramWebhook();
  } catch (error) {
    console.error(
      "Webhook setup failed:",
      error.message
    );
  }
});
