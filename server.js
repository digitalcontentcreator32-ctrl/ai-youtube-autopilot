import express from "express";

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 10000;

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "";
const GEMINI_API_KEY = process.env.GEMINI_API_KEY || "";

const GEMINI_MODELS = [
  "gemini-3.8-flash",
  "gemini-3.7-flash",
  "gemini-3.6-flash",
  "gemini-3.5-flash-lite"
];

const TTS_MODEL = "gemini-2.5-flash-preview-tts";
const TTS_VOICE = "Kore";

const jobs = new Map();

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/* ---------------- TELEGRAM ---------------- */

async function telegram(method, body = {}) {
  if (!TELEGRAM_BOT_TOKEN) {
    throw new Error("TELEGRAM_BOT_TOKEN is not configured.");
  }

  const response = await fetch(
    `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/${method}`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify(body)
    }
  );

  return response.json();
}

async function sendTelegram(chatId, text) {
  return telegram("sendMessage", {
    chat_id: chatId,
    text
  });
}

/* ---------------- GEMINI SCRIPT ---------------- */

function isTemporaryGeminiError(status, message) {
  const text = String(message || "").toLowerCase();

  return (
    status === 429 ||
    status === 503 ||
    text.includes("high demand") ||
    text.includes("temporarily unavailable") ||
    text.includes("overloaded") ||
    text.includes("resource exhausted") ||
    text.includes("try again later") ||
    text.includes("unavailable")
  );
}

function extractGeminiText(data) {
  if (data?.output_text) {
    return data.output_text;
  }

  for (const step of data?.steps || []) {
    if (step?.type === "model_output") {
      if (typeof step.content === "string") {
        return step.content;
      }

      if (Array.isArray(step.content)) {
        for (const item of step.content) {
          if (typeof item?.text === "string") {
            return item.text;
          }
        }
      }
    }
  }

  return "";
}

async function generateWithModel(model, command) {
  const prompt = `
You are the Creator Agent of an AI YouTube Autopilot.

Create an ORIGINAL YouTube video package for:

${command}

Return exactly:

TITLE:
HOOK:
DESCRIPTION:
SCRIPT:

The SCRIPT must be narration-ready.

Rules:
- Original writing only.
- Do not copy another creator.
- No copyrighted lyrics.
- Avoid fake facts.
- Keep facts scientifically responsible.
- Make it engaging for a faceless YouTube channel.
- Use natural spoken English.
- Do not put visual instructions inside the narration.
- Target approximately 500-800 spoken words.
`;

  const response = await fetch(
    "https://generativelanguage.googleapis.com/v1beta/interactions",
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-goog-api-key": GEMINI_API_KEY
      },
      body: JSON.stringify({
        model,
        input: prompt
      })
    }
  );

  const raw = await response.text();

  let data;

  try {
    data = JSON.parse(raw);
  } catch {
    data = { raw };
  }

  if (!response.ok) {
    const message =
      data?.error?.message ||
      data?.message ||
      data?.raw ||
      `Gemini HTTP ${response.status}`;

    const error = new Error(message);
    error.status = response.status;
    error.temporary = isTemporaryGeminiError(
      response.status,
      message
    );

    throw error;
  }

  const text = extractGeminiText(data);

  if (!text) {
    throw new Error("Gemini returned an empty response.");
  }

  return text;
}

async function generateScript(command) {
  if (!GEMINI_API_KEY) {
    throw new Error("GEMINI_API_KEY is not configured.");
  }

  let lastError = null;

  for (const model of GEMINI_MODELS) {
    try {
      console.log(`Trying Gemini model: ${model}`);

      const text = await generateWithModel(
        model,
        command
      );

      return {
        model,
        text
      };

    } catch (error) {
      lastError = error;

      console.log(
        `${model} failed: ${error.message}`
      );

      if (!error.temporary) {
        throw error;
      }

      await sleep(1500);
    }
  }

  throw new Error(
    `All Gemini models failed. Last error: ${
      lastError?.message || "Unknown error"
    }`
  );
}

/* ---------------- QUALITY CHECK ---------------- */

function extractSection(text, section, nextSections = []) {
  const startRegex = new RegExp(
    `\\b${section}:\\s*`,
    "i"
  );

  const start = text.search(startRegex);

  if (start === -1) {
    return "";
  }

  const afterStart =
    text.slice(start).replace(startRegex, "");

  let end = afterStart.length;

  for (const next of nextSections) {
    const regex = new RegExp(
      `\\b${next}:\\s*`,
      "i"
    );

    const index = afterStart.search(regex);

    if (index !== -1 && index < end) {
      end = index;
    }
  }

  return afterStart
    .slice(0, end)
    .trim();
}

function qualityCheck(text) {
  const title = extractSection(
    text,
    "TITLE",
    ["HOOK", "DESCRIPTION", "SCRIPT"]
  );

  const hook = extractSection(
    text,
    "HOOK",
    ["DESCRIPTION", "SCRIPT"]
  );

  const description = extractSection(
    text,
    "DESCRIPTION",
    ["SCRIPT"]
  );

  const script = extractSection(
    text,
    "SCRIPT",
    []
  );

  const errors = [];

  if (!title) {
    errors.push("Missing title");
  }

  if (!hook) {
    errors.push("Missing hook");
  }

  if (!script) {
    errors.push("Missing script");
  }

  const wordCount = script
    ? script.split(/\s+/).filter(Boolean).length
    : 0;

  if (wordCount < 150) {
    errors.push("Script is too short");
  }

  if (wordCount > 2500) {
    errors.push("Script is unusually long");
  }

  const lower = script.toLowerCase();

  const unwanted = [
    "[visual cue:",
    "copyrighted lyrics",
    "i am an ai",
    "as an ai language model"
  ];

  for (const phrase of unwanted) {
    if (lower.includes(phrase)) {
      errors.push(`Unwanted content: ${phrase}`);
    }
  }

  return {
    passed: errors.length === 0,
    errors,
    title,
    hook,
    description,
    script,
    wordCount
  };
}

/* ---------------- WAV CREATOR ---------------- */

function createWav(pcmBuffer, sampleRate = 24000) {
  const channels = 1;
  const bitsPerSample = 16;

  const byteRate =
    sampleRate *
    channels *
    (bitsPerSample / 8);

  const blockAlign =
    channels * (bitsPerSample / 8);

  const buffer = Buffer.alloc(
    44 + pcmBuffer.length
  );

  buffer.write("RIFF", 0);
  buffer.writeUInt32LE(
    36 + pcmBuffer.length,
    4
  );

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
  buffer.writeUInt32LE(
    pcmBuffer.length,
    40
  );

  pcmBuffer.copy(buffer, 44);

  return buffer;
}

/* ---------------- AI VOICE ---------------- */

async function generateVoice(text) {
  if (!GEMINI_API_KEY) {
    throw new Error(
      "GEMINI_API_KEY is not configured."
    );
  }

  const prompt = `
Read the following YouTube narration as a professional,
clear, natural American-English documentary narrator.

Style:
- confident
- energetic
- cinematic
- natural pauses
- easy to understand
- not overly dramatic

Narration:

${text}
`;

  const response = await fetch(
    "https://generativelanguage.googleapis.com/v1beta/interactions",
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-goog-api-key": GEMINI_API_KEY
      },
      body: JSON.stringify({
        model: TTS_MODEL,
        input: prompt,
        response_format: {
          type: "audio"
        },
        generation_config: {
          speech_config: [
            {
              voice: TTS_VOICE
            }
          ]
        }
      })
    }
  );

  const raw = await response.text();

  let data;

  try {
    data = JSON.parse(raw);
  } catch {
    data = { raw };
  }

  if (!response.ok) {
    const message =
      data?.error?.message ||
      data?.message ||
      data?.raw ||
      `TTS HTTP ${response.status}`;

    throw new Error(message);
  }

  const audioBase64 =
    data?.output_audio?.data;

  if (!audioBase64) {
    throw new Error(
      "TTS returned no audio data."
    );
  }

  const pcm = Buffer.from(
    audioBase64,
    "base64"
  );

  return createWav(pcm);
}

/* ---------------- TELEGRAM AUDIO ---------------- */

async function sendAudio(
  chatId,
  audioBuffer,
  caption
) {
  const form = new FormData();

  form.append(
    "chat_id",
    String(chatId)
  );

  form.append(
    "caption",
    caption
  );

  form.append(
    "audio",
    new Blob(
      [audioBuffer],
      { type: "audio/wav" }
    ),
    "narration.wav"
  );

  const response = await fetch(
    `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendAudio`,
    {
      method: "POST",
      body: form
    }
  );

  return response.json();
}

/* ---------------- JOB ---------------- */

function createJob(command, chatId) {
  const id =
    `job_${Date.now()}_${jobs.size + 1}`;

  const job = {
    id,
    command,
    chatId,
    status: "queued",
    progress: 0,
    createdAt:
      new Date().toISOString(),
    updatedAt:
      new Date().toISOString()
  };

  jobs.set(id, job);

  return job;
}

async function processJob(job) {
  try {

    /* SCRIPT */

    job.status = "running";
    job.progress = 5;

    await sendTelegram(
      job.chatId,
      `🤖 Creator Agent started\n\nJob: ${job.id}\nProgress: 5%\n\nGenerating original script...`
    );

    const generated =
      await generateScript(
        job.command
      );

    job.progress = 40;

    await sendTelegram(
      job.chatId,
      `🧠 Script generated\n\nModel: ${generated.model}\nProgress: 40%\n\nRunning quality check...`
    );

    /* QUALITY */

    const checked =
      qualityCheck(generated.text);

    if (!checked.passed) {
      throw new Error(
        `Quality check failed:\n${checked.errors.join("\n")}`
      );
    }

    job.progress = 55;

    await sendTelegram(
      job.chatId,
      `✅ Quality check passed\n\nWords: ${checked.wordCount}\nProgress: 55%\n\n🎙️ Generating AI narration...`
    );

    /* VOICE */

    const audio =
      await generateVoice(
        checked.script
      );

    job.progress = 80;

    await sendTelegram(
      job.chatId,
      `🎙️ AI voice generated\n\nVoice: ${TTS_VOICE}\nModel: ${TTS_MODEL}\nProgress: 80%\n\nSending test audio...`
    );

    /* SEND AUDIO */

    await sendAudio(
      job.chatId,
      audio,
      `🎙️ AI YouTube Autopilot\n\n${checked.title}`
    );

    job.status = "completed";
    job.progress = 100;
    job.updatedAt =
      new Date().toISOString();

    await sendTelegram(
      job.chatId,
      `✅ PIPELINE TEST COMPLETED\n\nJob: ${job.id}\n\nScript ✅\nQuality Check ✅\nAI Voice ✅\n\nNext stage: Visuals + Captions + Video Rendering.`
    );

  } catch (error) {

    job.status = "paused";
    job.error = error.message;
    job.updatedAt =
      new Date().toISOString();

    await sendTelegram(
      job.chatId,
      `⚠️ Job paused\n\nJob: ${job.id}\n\nReason:\n${error.message}\n\nNo paid service was charged.`
    );
  }
}

/* ---------------- TELEGRAM COMMANDS ---------------- */

async function handleTelegramMessage(message) {
  const chatId =
    message?.chat?.id;

  const text =
    message?.text?.trim();

  if (!chatId || !text) {
    return;
  }

  if (text === "/start") {
    await sendTelegram(
      chatId,
      `🤖 AI YouTube Autopilot\n\nCommands:\n\n/create <topic>\n/status\n/jobs`
    );
    return;
  }

  if (text === "/status") {
    await sendTelegram(
      chatId,
      `📊 SYSTEM STATUS\n\nTelegram: ${
        TELEGRAM_BOT_TOKEN
          ? "✅"
          : "❌"
      }\nGemini: ${
        GEMINI_API_KEY
          ? "✅"
          : "❌"
      }\n\nScript Agent: ✅\nQuality Check: ✅\nAI Voice: ${
        GEMINI_API_KEY
          ? "✅"
          : "❌"
      }\n\nTTS Model: ${TTS_MODEL}`
    );
    return;
  }

  if (text === "/jobs") {

    if (jobs.size === 0) {
      await sendTelegram(
        chatId,
        "📭 No jobs found."
      );
      return;
    }

    let output = "📋 JOBS\n\n";

    for (const job of jobs.values()) {
      output +=
        `${job.id}\nStatus: ${job.status}\nProgress: ${job.progress}%\n\n`;
    }

    await sendTelegram(
      chatId,
      output
    );

    return;
  }

  if (text.startsWith("/create ")) {

    const command =
      text.slice(8).trim();

    if (!command) {
      await sendTelegram(
        chatId,
        "Example:\n/create 5 surprising facts about space"
      );
      return;
    }

    const job =
      createJob(
        command,
        chatId
      );

    await sendTelegram(
      chatId,
      `📝 Job created\n\nJob: ${job.id}\nStatus: queued`
    );

    processJob(job);

    return;
  }

  await sendTelegram(
    chatId,
    "Unknown command.\n\nUse /start"
  );
}

/* ---------------- TELEGRAM POLLING ---------------- */

let telegramOffset = 0;
let pollingRunning = false;

async function telegramPolling() {

  if (
    pollingRunning ||
    !TELEGRAM_BOT_TOKEN
  ) {
    return;
  }

  pollingRunning = true;

  console.log(
    "Telegram polling enabled"
  );

  while (true) {

    try {

      const result =
        await telegram(
          "getUpdates",
          {
            offset:
              telegramOffset,
            timeout: 25,
            allowed_updates:
              ["message"]
          }
        );

      if (
        result?.ok &&
        Array.isArray(
          result.result
        )
      ) {

        for (
          const update
          of result.result
        ) {

          telegramOffset =
            update.update_id + 1;

          try {
            await handleTelegramMessage(
              update.message
            );
          } catch (error) {
            console.error(
              "Telegram message error:",
              error.message
            );
          }
        }
      }

    } catch (error) {

      console.error(
        "Telegram polling error:",
        error.message
      );

      await sleep(5000);
    }
  }
}

/* ---------------- SERVER ---------------- */

app.get("/", (req, res) => {
  res.json({
    name:
      "AI YouTube Autopilot",
    status: "online",
    telegram:
      Boolean(
        TELEGRAM_BOT_TOKEN
      ),
    gemini:
      Boolean(
        GEMINI_API_KEY
      ),
    tts: TTS_MODEL
  });
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    telegram:
      Boolean(
        TELEGRAM_BOT_TOKEN
      ),
    gemini:
      Boolean(
        GEMINI_API_KEY
      ),
    jobs: jobs.size
  });
});

app.get("/api/jobs", (req, res) => {
  res.json({
    jobs:
      Array.from(
        jobs.values()
      )
  });
});

app.listen(
  PORT,
  () => {

    console.log(
      `AI YouTube Autopilot listening on ${PORT}`
    );

    console.log(
      `Gemini configured: ${Boolean(
        GEMINI_API_KEY
      )}`
    );

    console.log(
      `Telegram configured: ${Boolean(
        TELEGRAM_BOT_TOKEN
      )}`
    );

    telegramPolling();
  }
);
