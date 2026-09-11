import express from "express";

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 10000;

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;

const TTS_MODEL = "gemini-3.1-flash-tts-preview";
const TTS_VOICE = "Kore";

const SCRIPT_MODELS = [
  "gemini-3.8-flash",
  "gemini-3.7-flash",
  "gemini-3.6-flash",
  "gemini-3.5-flash-lite"
];

const jobs = new Map();

let telegramOffset = 0;
let telegramPolling = false;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function jobId() {
  return `job_${Date.now()}_${jobs.size + 1}`;
}

function updateJob(id, data) {
  const job = jobs.get(id);
  if (!job) return;

  Object.assign(job, data);

  if (job.chatId) {
    sendTelegram(
      job.chatId,
      `📊 *${job.stage || "Working"}*\nProgress: ${job.progress || 0}%`
    ).catch(() => {});
  }
}

async function telegramRequest(method, body = {}) {
  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/${method}`;

  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  });

  const data = await response.json();

  if (!data.ok) {
    throw new Error(data.description || "Telegram API error");
  }

  return data.result;
}

async function sendTelegram(chatId, text) {
  return telegramRequest("sendMessage", {
    chat_id: chatId,
    text,
    parse_mode: "Markdown"
  });
}

async function sendAudio(chatId, audioBuffer, filename = "narration.wav") {
  const form = new FormData();

  form.append("chat_id", String(chatId));
  form.append(
    "audio",
    new Blob([audioBuffer], { type: "audio/wav" }),
    filename
  );

  const response = await fetch(
    `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendAudio`,
    {
      method: "POST",
      body: form
    }
  );

  const data = await response.json();

  if (!data.ok) {
    throw new Error(data.description || "Telegram audio upload failed");
  }

  return data.result;
}

/* ---------------- GEMINI ---------------- */

async function geminiRequest(model, input, options = {}) {
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
        input,
        ...options
      })
    }
  );

  const text = await response.text();

  let data;

  try {
    data = JSON.parse(text);
  } catch {
    throw new Error(`Gemini returned invalid response: ${text.slice(0, 500)}`);
  }

  if (!response.ok) {
    const errorMessage =
      data?.error?.message ||
      data?.message ||
      `Gemini HTTP ${response.status}`;

    const error = new Error(errorMessage);
    error.status = response.status;

    throw error;
  }

  return data;
}

/* ---------------- SCRIPT ---------------- */

async function generateScript(topic) {
  let lastError = null;

  const prompt = `
You are an expert YouTube script writer.

Create a completely original faceless YouTube video script.

TOPIC:
${topic}

Requirements:
- English
- 500-800 spoken words
- Strong first 10-second hook
- Interesting storytelling
- Easy for AI voice narration
- No copyrighted lyrics
- No copied article text
- No visual directions inside the narration
- No "AI generated" disclaimer
- No fake citations
- Avoid repetitive filler
- Give useful and entertaining information

Return exactly this structure:

TITLE:
<short clickable title>

HOOK:
<opening hook>

DESCRIPTION:
<YouTube description>

SCRIPT:
<complete narration>
`;

  for (const model of SCRIPT_MODELS) {
    try {
      const data = await geminiRequest(model, prompt);

      const output =
        data?.outputs?.[0]?.text ||
        data?.output_text ||
        data?.text ||
        "";

      if (!output.trim()) {
        throw new Error("Gemini returned empty script");
      }

      return {
        model,
        text: output
      };
    } catch (error) {
      lastError = error;

      console.log(
        `Script model failed: ${model} -> ${error.message}`
      );

      if (![429, 500, 502, 503, 504].includes(error.status)) {
        break;
      }

      await sleep(1500);
    }
  }

  throw lastError || new Error("All script models failed");
}

/* ---------------- QUALITY CHECK ---------------- */

function qualityCheck(text) {
  const title = text.match(/TITLE:\s*([\s\S]*?)(?=\nHOOK:|$)/i);
  const hook = text.match(/HOOK:\s*([\s\S]*?)(?=\nDESCRIPTION:|$)/i);
  const description = text.match(
    /DESCRIPTION:\s*([\s\S]*?)(?=\nSCRIPT:|$)/i
  );
  const script = text.match(/SCRIPT:\s*([\s\S]*)$/i);

  if (!title || !hook || !script) {
    return {
      passed: false,
      reason: "Required sections are missing"
    };
  }

  const scriptText = script[1].trim();

  const words = scriptText
    .split(/\s+/)
    .filter(Boolean).length;

  if (words < 150) {
    return {
      passed: false,
      reason: `Script too short: ${words} words`
    };
  }

  if (words > 2500) {
    return {
      passed: false,
      reason: `Script too long: ${words} words`
    };
  }

  const blockedPatterns = [
    "visual cue:",
    "[visual]",
    "copyrighted lyrics",
    "ai disclaimer"
  ];

  const lower = scriptText.toLowerCase();

  for (const pattern of blockedPatterns) {
    if (lower.includes(pattern)) {
      return {
        passed: false,
        reason: `Blocked pattern detected: ${pattern}`
      };
    }
  }

  return {
    passed: true,
    title: title[1].trim(),
    hook: hook[1].trim(),
    description: description
      ? description[1].trim()
      : "",
    script: scriptText,
    words
  };
}

/* ---------------- WAV ---------------- */

function createWav(pcmBuffer, sampleRate = 24000) {
  const channels = 1;
  const bitsPerSample = 16;

  const byteRate =
    sampleRate * channels * (bitsPerSample / 8);

  const blockAlign =
    channels * (bitsPerSample / 8);

  const header = Buffer.alloc(44);

  header.write("RIFF", 0);
  header.writeUInt32LE(36 + pcmBuffer.length, 4);
  header.write("WAVE", 8);

  header.write("fmt ", 12);
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20);
  header.writeUInt16LE(channels, 22);
  header.writeUInt32LE(sampleRate, 24);
  header.writeUInt32LE(byteRate, 28);
  header.writeUInt16LE(blockAlign, 32);
  header.writeUInt16LE(bitsPerSample, 34);

  header.write("data", 36);
  header.writeUInt32LE(pcmBuffer.length, 40);

  return Buffer.concat([header, pcmBuffer]);
}

/* ---------------- TTS ---------------- */

async function generateVoice(text) {
  let lastError = null;

  const prompt = `
Read the following YouTube narration naturally.

Voice style:
- Clear
- Professional
- Energetic
- Natural pacing
- Suitable for a YouTube documentary/facts video
- Do not say anything before or after the narration

Narration:

${text}
`;

  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      console.log(`TTS attempt ${attempt}/3`);

      const data = await geminiRequest(
        TTS_MODEL,
        prompt,
        {
          response_format: {
            type: "audio"
          },
          generation_config: {
            speech_config: [
              {
                voice: TTS_VOICE,
                language: "en-US"
              }
            ]
          }
        }
      );

      const audioBase64 =
        data?.output_audio?.data;

      if (!audioBase64) {
        throw new Error(
          "Gemini TTS returned no audio data"
        );
      }

      const pcm = Buffer.from(
        audioBase64,
        "base64"
      );

      if (!pcm.length) {
        throw new Error("Generated audio is empty");
      }

      return createWav(pcm, 24000);

    } catch (error) {
      lastError = error;

      console.log(
        `TTS failed attempt ${attempt}: ${error.message}`
      );

      if (attempt < 3) {
        await sleep(1500 * attempt);
      }
    }
  }

  throw lastError || new Error("TTS failed");
}

/* ---------------- JOB PROCESSOR ---------------- */

async function processJob(id) {
  const job = jobs.get(id);

  if (!job) return;

  try {
    updateJob(id, {
      stage: "Creator Agent started",
      progress: 5,
      status: "running"
    });

    await sendTelegram(
      job.chatId,
      `🚀 *Creator Agent started*\nJob: ${id}`
    );

    /* SCRIPT */

    updateJob(id, {
      stage: "Generating original script",
      progress: 10
    });

    const generated = await generateScript(job.topic);

    job.model = generated.model;
    job.rawScript = generated.text;

    updateJob(id, {
      stage: "Script generated",
      progress: 40
    });

    await sendTelegram(
      job.chatId,
      `📝 Script generated\nModel: ${generated.model}`
    );

    /* QUALITY */

    updateJob(id, {
      stage: "Quality check",
      progress: 45
    });

    const quality = qualityCheck(generated.text);

    if (!quality.passed) {
      throw new Error(
        `Quality check failed: ${quality.reason}`
      );
    }

    job.title = quality.title;
    job.description = quality.description;
    job.script = quality.script;
    job.words = quality.words;

    updateJob(id, {
      stage: "Quality check passed",
      progress: 50
    });

    await sendTelegram(
      job.chatId,
      `✅ Quality check passed\nWords: ${quality.words}`
    );

    /* TTS */

    updateJob(id, {
      stage: "Generating AI narration",
      progress: 55
    });

    await sendTelegram(
      job.chatId,
      `🎙️ Generating AI narration...`
    );

    const audio = await generateVoice(
      quality.script
    );

    job.audioSize = audio.length;

    updateJob(id, {
      stage: "AI narration generated",
      progress: 80
    });

    await sendTelegram(
      job.chatId,
      `🎧 AI narration generated\nSending audio...`
    );

    /* TELEGRAM AUDIO */

    await sendAudio(
      job.chatId,
      audio,
      `${id}.wav`
    );

    /* COMPLETE */

    updateJob(id, {
      stage: "Completed",
      progress: 100,
      status: "completed",
      completedAt: new Date().toISOString()
    });

    await sendTelegram(
      job.chatId,
      `🎉 *Job completed!*

Title:
${job.title}

Words:
${job.words}

Model:
${job.model}

Status:
✅ Script
✅ Quality check
✅ AI narration
✅ Telegram audio`
    );

  } catch (error) {
    console.error("JOB ERROR:", error);

    const job = jobs.get(id);

    if (job) {
      job.status = "paused";
      job.stage = "Paused";
      job.error = error.message;
      job.pausedAt = new Date().toISOString();
    }

    await sendTelegram(
      job.chatId,
      `⚠️ *Job paused*

Reason:
${error.message}

💰 No paid service was charged.

Job:
${id}`
    ).catch(() => {});
  }
}

/* ---------------- TELEGRAM COMMANDS ---------------- */

async function handleTelegramMessage(message) {
  const chatId = message?.chat?.id;
  const text = message?.text?.trim();

  if (!chatId || !text) return;

  console.log(
    `Telegram message from ${chatId}: ${text}`
  );

  if (text === "/start") {
    await sendTelegram(
      chatId,
      `🤖 *AI YouTube Autopilot*

Commands:

/create <topic>
/status
/jobs

Example:

/create 5 surprising facts about space`
    );

    return;
  }

  if (text === "/status") {
    await sendTelegram(
      chatId,
      `🟢 *System Online*

Creator Agent: Ready
Gemini: Connected
Telegram: Connected
TTS: Ready
Mode: Free-first`
    );

    return;
  }

  if (text === "/jobs") {
    const userJobs = [...jobs.values()]
      .filter(j => String(j.chatId) === String(chatId))
      .slice(-10);

    if (!userJobs.length) {
      await sendTelegram(
        chatId,
        "📭 No jobs found."
      );

      return;
    }

    const list = userJobs
      .map(
        j =>
          `• ${j.id}\n  ${j.status} — ${j.progress}%\n  ${j.topic}`
      )
      .join("\n\n");

    await sendTelegram(
      chatId,
      `📋 *Recent Jobs*\n\n${list}`
    );

    return;
  }

  if (text.startsWith("/create ")) {
    const topic = text
      .replace("/create ", "")
      .trim();

    if (!topic) {
      await sendTelegram(
        chatId,
        "❌ Topic missing.\n\nExample:\n/create facts about space"
      );

      return;
    }

    const id = jobId();

    jobs.set(id, {
      id,
      chatId,
      topic,
      status: "queued",
      stage: "Job created",
      progress: 0,
      createdAt: new Date().toISOString()
    });

    await sendTelegram(
      chatId,
      `🆕 *Job created*

Job:
${id}

Topic:
${topic}

Starting Creator Agent...`
    );

    processJob(id).catch(console.error);

    return;
  }

  await sendTelegram(
    chatId,
    `❓ Unknown command.

Use:
/start
/status
/jobs
/create <topic>`
  );
}

/* ---------------- TELEGRAM POLLING ---------------- */

async function telegramPoll() {
  if (telegramPolling) return;

  telegramPolling = true;

  console.log("Telegram polling enabled");

  while (true) {
    try {
      const updates = await telegramRequest(
        "getUpdates",
        {
          offset: telegramOffset,
          timeout: 25,
          allowed_updates: ["message"]
        }
      );

      for (const update of updates) {
        telegramOffset = update.update_id + 1;

        try {
          await handleTelegramMessage(
            update.message
          );
        } catch (error) {
          console.error(
            "Telegram message error:",
            error
          );
        }
      }

    } catch (error) {
      console.error(
        "Telegram polling error:",
        error.message
      );

      await sleep(3000);
    }
  }
}

/* ---------------- HEALTH ---------------- */

app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "AI YouTube Autopilot",
    status: "online",
    time: new Date().toISOString()
  });
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    telegram: !!TELEGRAM_BOT_TOKEN,
    gemini: !!GEMINI_API_KEY,
    ttsModel: TTS_MODEL,
    jobs: jobs.size
  });
});

/* ---------------- START ---------------- */

app.listen(PORT, () => {
  console.log(
    `AI YouTube Autopilot listening on port ${PORT}`
  );

  if (!TELEGRAM_BOT_TOKEN) {
    console.error(
      "TELEGRAM_BOT_TOKEN is missing"
    );
  }

  if (!GEMINI_API_KEY) {
    console.error(
      "GEMINI_API_KEY is missing"
    );
  }

  if (TELEGRAM_BOT_TOKEN) {
    telegramPoll().catch(console.error);
  }
});
