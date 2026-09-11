import express from "express";

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 10000;

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;

const SCRIPT_MODELS = [
  "gemini-3.8-flash",
  "gemini-3.7-flash",
  "gemini-3.6-flash",
  "gemini-3.5-flash-lite"
];

const TTS_MODEL = "gemini-3.1-flash-tts-preview";
const TTS_VOICE = "Kore";

const jobs = new Map();

let telegramOffset = 0;
let telegramStarted = false;

/* =========================
   BASIC HELPERS
========================= */

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function createJobId() {
  return `job_${Date.now()}_${jobs.size + 1}`;
}

/* =========================
   TELEGRAM
========================= */

async function telegramRequest(method, body = {}) {
  if (!TELEGRAM_BOT_TOKEN) {
    throw new Error("TELEGRAM_BOT_TOKEN is missing");
  }

  const url =
    `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/${method}`;

  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  });

  const data = await response.json();

  if (!data.ok) {
    throw new Error(
      data.description || "Telegram API error"
    );
  }

  return data.result;
}

async function sendTelegram(chatId, text) {
  return telegramRequest("sendMessage", {
    chat_id: chatId,
    text
  });
}

async function sendAudio(chatId, buffer, filename) {
  const form = new FormData();

  form.append("chat_id", String(chatId));

  form.append(
    "audio",
    new Blob([buffer], {
      type: "audio/wav"
    }),
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
    throw new Error(
      data.description || "Telegram audio upload failed"
    );
  }

  return data.result;
}

/* =========================
   GEMINI REQUEST
========================= */

async function geminiRequest(model, input, extra = {}) {
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
        ...extra
      })
    }
  );

  const raw = await response.text();

  let data;

  try {
    data = JSON.parse(raw);
  } catch {
    throw new Error(
      `Gemini returned invalid JSON: ${raw.slice(0, 500)}`
    );
  }

  if (!response.ok) {
    const message =
      data?.error?.message ||
      data?.message ||
      `Gemini HTTP ${response.status}`;

    const error = new Error(message);
    error.status = response.status;

    throw error;
  }

  if (data?.errors?.length) {
    throw new Error(
      data.errors.map(x => x.message).join("; ")
    );
  }

  return data;
}

/* =========================
   EXTRACT GEMINI TEXT
========================= */

function extractGeminiText(data) {

  /* New/direct output_text */
  if (
    typeof data?.output_text === "string" &&
    data.output_text.trim()
  ) {
    return data.output_text.trim();
  }

  /* Interactions API steps */
  if (Array.isArray(data?.steps)) {

    const pieces = [];

    for (const step of data.steps) {

      if (step?.type !== "model_output") {
        continue;
      }

      if (!Array.isArray(step?.content)) {
        continue;
      }

      for (const item of step.content) {

        if (
          item?.type === "text" &&
          typeof item?.text === "string" &&
          item.text.trim()
        ) {
          pieces.push(item.text.trim());
        }
      }
    }

    if (pieces.length) {
      return pieces.join("\n\n").trim();
    }
  }

  /* Compatibility */
  if (Array.isArray(data?.outputs)) {

    const pieces = [];

    for (const output of data.outputs) {

      if (
        typeof output?.text === "string" &&
        output.text.trim()
      ) {
        pieces.push(output.text.trim());
      }

      if (Array.isArray(output?.content)) {

        for (const item of output.content) {

          if (
            item?.type === "text" &&
            typeof item?.text === "string" &&
            item.text.trim()
          ) {
            pieces.push(item.text.trim());
          }
        }
      }
    }

    if (pieces.length) {
      return pieces.join("\n\n").trim();
    }
  }

  /* Older compatibility */
  if (
    typeof data?.text === "string" &&
    data.text.trim()
  ) {
    return data.text.trim();
  }

  return "";
}

/* =========================
   SCRIPT GENERATION
========================= */

async function generateScript(topic) {

  const prompt = `
You are the Creator Agent for a professional faceless YouTube channel.

Create a completely original YouTube video script.

TOPIC:
${topic}

Requirements:

- English language
- 500 to 800 spoken words
- Strong hook in the first 10 seconds
- Interesting storytelling
- Natural spoken English
- Suitable for AI voice narration
- Original wording
- No copied article text
- No copyrighted lyrics
- No fake quotations
- No visual instructions
- No "scene:" instructions
- No "visual:" instructions
- No AI disclaimer
- No unnecessary filler
- Make the information useful and entertaining

Return EXACTLY this format:

TITLE:
<clickable YouTube title>

HOOK:
<strong opening hook>

DESCRIPTION:
<YouTube description>

SCRIPT:
<complete spoken narration>
`;

  let lastError = null;

  for (const model of SCRIPT_MODELS) {

    try {

      console.log(
        `Trying Gemini script model: ${model}`
      );

      const data = await geminiRequest(
        model,
        prompt
      );

      const scriptText =
        extractGeminiText(data);

      if (!scriptText) {

        console.log(
          `Model ${model} returned no readable text.`
        );

        console.log(
          "Gemini response keys:",
          Object.keys(data || {})
        );

        if (Array.isArray(data?.steps)) {
          console.log(
            "Gemini steps:",
            JSON.stringify(
              data.steps,
              null,
              2
            ).slice(0, 5000)
          );
        }

        throw new Error(
          "Gemini returned empty script"
        );
      }

      return {
        model,
        text: scriptText
      };

    } catch (error) {

      lastError = error;

      console.log(
        `Script model failed: ${model}`
      );

      console.log(error.message);

      const retryable =
        [429, 500, 502, 503, 504]
          .includes(error.status);

      if (!retryable) {
        continue;
      }

      await sleep(1500);
    }
  }

  throw (
    lastError ||
    new Error("All Gemini script models failed")
  );
}

/* =========================
   QUALITY CHECK
========================= */

function qualityCheck(text) {

  const titleMatch =
    text.match(
      /TITLE:\s*([\s\S]*?)(?=\nHOOK:|$)/i
    );

  const hookMatch =
    text.match(
      /HOOK:\s*([\s\S]*?)(?=\nDESCRIPTION:|$)/i
    );

  const descriptionMatch =
    text.match(
      /DESCRIPTION:\s*([\s\S]*?)(?=\nSCRIPT:|$)/i
    );

  const scriptMatch =
    text.match(
      /SCRIPT:\s*([\s\S]*)$/i
    );

  if (!titleMatch) {
    return {
      passed: false,
      reason: "TITLE missing"
    };
  }

  if (!hookMatch) {
    return {
      passed: false,
      reason: "HOOK missing"
    };
  }

  if (!scriptMatch) {
    return {
      passed: false,
      reason: "SCRIPT missing"
    };
  }

  const title =
    titleMatch[1].trim();

  const hook =
    hookMatch[1].trim();

  const description =
    descriptionMatch
      ? descriptionMatch[1].trim()
      : "";

  const script =
    scriptMatch[1].trim();

  const words =
    script
      .split(/\s+/)
      .filter(Boolean)
      .length;

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

  const blocked = [
    "visual cue:",
    "[visual]",
    "scene:",
    "[scene]",
    "copyrighted lyrics",
    "ai disclaimer"
  ];

  const lower =
    script.toLowerCase();

  for (const word of blocked) {

    if (lower.includes(word)) {

      return {
        passed: false,
        reason:
          `Blocked text detected: ${word}`
      };
    }
  }

  return {
    passed: true,
    title,
    hook,
    description,
    script,
    words
  };
}

/* =========================
   WAV CREATOR
========================= */

function createWav(
  pcm,
  sampleRate = 24000
) {

  const channels = 1;
  const bitsPerSample = 16;

  const blockAlign =
    channels * bitsPerSample / 8;

  const byteRate =
    sampleRate * blockAlign;

  const header =
    Buffer.alloc(44);

  header.write("RIFF", 0);

  header.writeUInt32LE(
    36 + pcm.length,
    4
  );

  header.write("WAVE", 8);

  header.write("fmt ", 12);

  header.writeUInt32LE(
    16,
    16
  );

  header.writeUInt16LE(
    1,
    20
  );

  header.writeUInt16LE(
    channels,
    22
  );

  header.writeUInt32LE(
    sampleRate,
    24
  );

  header.writeUInt32LE(
    byteRate,
    28
  );

  header.writeUInt16LE(
    blockAlign,
    32
  );

  header.writeUInt16LE(
    bitsPerSample,
    34
  );

  header.write("data", 36);

  header.writeUInt32LE(
    pcm.length,
    40
  );

  return Buffer.concat([
    header,
    pcm
  ]);
}

/* =========================
   TTS
========================= */

async function generateVoice(text) {

  const prompt = `
Read the following YouTube narration.

Voice:
- Natural
- Clear
- Professional
- Energetic
- Documentary style
- Good pacing
- American English

Do not add an introduction.
Do not add an ending.
Only speak the narration.

NARRATION:

${text}
`;

  let lastError = null;

  for (let attempt = 1; attempt <= 3; attempt++) {

    try {

      console.log(
        `TTS attempt ${attempt}/3`
      );

      const data =
        await geminiRequest(
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

      const audioData =
        data?.output_audio?.data;

      if (!audioData) {

        console.log(
          "TTS response:",
          JSON.stringify(
            data,
            null,
            2
          ).slice(0, 5000)
        );

        throw new Error(
          "Gemini TTS returned no audio data"
        );
      }

      const pcm =
        Buffer.from(
          audioData,
          "base64"
        );

      if (!pcm.length) {
        throw new Error(
          "TTS audio is empty"
        );
      }

      return createWav(
        pcm,
        24000
      );

    } catch (error) {

      lastError = error;

      console.log(
        `TTS attempt ${attempt} failed:`,
        error.message
      );

      if (attempt < 3) {
        await sleep(
          2000 * attempt
        );
      }
    }
  }

  throw (
    lastError ||
    new Error("TTS generation failed")
  );
}

/* =========================
   JOB
========================= */

async function processJob(id) {

  const job =
    jobs.get(id);

  if (!job) {
    return;
  }

  try {

    job.status = "running";
    job.stage = "Creator Agent started";
    job.progress = 5;

    await sendTelegram(
      job.chatId,
      `🚀 Creator Agent started

Job: ${id}

Progress: 5%`
    );

    /* SCRIPT */

    job.stage =
      "Generating original script";

    job.progress = 10;

    await sendTelegram(
      job.chatId,
      `📊 Generating original script

Progress: 10%`
    );

    const result =
      await generateScript(
        job.topic
      );

    job.model =
      result.model;

    job.rawScript =
      result.text;

    job.stage =
      "Script generated";

    job.progress = 40;

    await sendTelegram(
      job.chatId,
      `📝 Script generated

Model: ${result.model}

Progress: 40%`
    );

    /* QUALITY */

    job.stage =
      "Running quality check";

    job.progress = 45;

    const quality =
      qualityCheck(
        result.text
      );

    if (!quality.passed) {
      throw new Error(
        `Quality check failed: ${quality.reason}`
      );
    }

    job.title =
      quality.title;

    job.description =
      quality.description;

    job.script =
      quality.script;

    job.words =
      quality.words;

    job.stage =
      "Quality check passed";

    job.progress = 50;

    await sendTelegram(
      job.chatId,
      `✅ Quality check passed

Words: ${quality.words}

Progress: 50%`
    );

    /* TTS */

    job.stage =
      "Generating AI narration";

    job.progress = 55;

    await sendTelegram(
      job.chatId,
      `🎙️ Generating AI narration...

Progress: 55%`
    );

    const audio =
      await generateVoice(
        quality.script
      );

    job.audioBytes =
      audio.length;

    job.stage =
      "AI narration generated";

    job.progress = 80;

    await sendTelegram(
      job.chatId,
      `🎧 AI narration generated

Sending audio...
Progress: 80%`
    );

    /* SEND AUDIO */

    await sendAudio(
      job.chatId,
      audio,
      `${id}.wav`
    );

    /* COMPLETE */

    job.status =
      "completed";

    job.stage =
      "Completed";

    job.progress = 100;

    job.completedAt =
      new Date().toISOString();

    await sendTelegram(
      job.chatId,
      `🎉 JOB COMPLETED

Title:
${job.title}

Words:
${job.words}

Model:
${job.model}

✅ Script
✅ Quality check
✅ AI narration
✅ Audio sent`
    );

  } catch (error) {

    console.error(
      "JOB ERROR:",
      error
    );

    job.status =
      "paused";

    job.stage =
      "Paused";

    job.error =
      error.message;

    job.pausedAt =
      new Date().toISOString();

    await sendTelegram(
      job.chatId,
      `⚠️ JOB PAUSED

Reason:
${error.message}

💰 No paid service was charged.

Job:
${id}`
    ).catch(() => {});
  }
}

/* =========================
   TELEGRAM COMMANDS
========================= */

async function handleMessage(message) {

  const chatId =
    message?.chat?.id;

  const text =
    message?.text?.trim();

  if (!chatId || !text) {
    return;
  }

  /* START */

  if (text === "/start") {

    await sendTelegram(
      chatId,
      `🤖 AI YouTube Autopilot

Commands:

/create <topic>
/status
/jobs

Example:

/create 5 surprising facts about space`
    );

    return;
  }

  /* STATUS */

  if (text === "/status") {

    await sendTelegram(
      chatId,
      `🟢 SYSTEM ONLINE

Creator Agent: Ready
Gemini: Connected
TTS: Ready
Telegram: Connected
Mode: Free-first

Jobs: ${jobs.size}`
    );

    return;
  }

  /* JOBS */

  if (text === "/jobs") {

    const userJobs =
      [...jobs.values()]
        .filter(
          j =>
            String(j.chatId) ===
            String(chatId)
        )
        .slice(-10);

    if (!userJobs.length) {

      await sendTelegram(
        chatId,
        "📭 No jobs found."
      );

      return;
    }

    const list =
      userJobs
        .map(
          j =>
            `• ${j.id}
Status: ${j.status}
Progress: ${j.progress}%
Topic: ${j.topic}`
        )
        .join("\n\n");

    await sendTelegram(
      chatId,
      `📋 RECENT JOBS

${list}`
    );

    return;
  }

  /* CREATE */

  if (
    text.startsWith("/create ")
  ) {

    const topic =
      text
        .replace(
          "/create ",
          ""
        )
        .trim();

    if (!topic) {

      await sendTelegram(
        chatId,
        `❌ Topic missing.

Example:

/create facts about space`
      );

      return;
    }

    const id =
      createJobId();

    jobs.set(
      id,
      {
        id,
        chatId,
        topic,
        status: "queued",
        stage: "Job created",
        progress: 0,
        createdAt:
          new Date().toISOString()
      }
    );

    await sendTelegram(
      chatId,
      `🆕 JOB CREATED

Job:
${id}

Topic:
${topic}

Starting Creator Agent...`
    );

    processJob(id)
      .catch(error =>
        console.error(
          "Background job error:",
          error
        )
      );

    return;
  }

  /* UNKNOWN */

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

/* =========================
   TELEGRAM POLLING
========================= */

async function startTelegramPolling() {

  if (telegramStarted) {
    return;
  }

  telegramStarted = true;

  console.log(
    "Telegram polling enabled"
  );

  while (true) {

    try {

      const updates =
        await telegramRequest(
          "getUpdates",
          {
            offset:
              telegramOffset,
            timeout: 25,
            allowed_updates: [
              "message"
            ]
          }
        );

      for (
        const update of updates
      ) {

        telegramOffset =
          update.update_id + 1;

        try {

          await handleMessage(
            update.message
          );

        } catch (error) {

          console.error(
            "Message error:",
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

/* =========================
   HEALTH
========================= */

app.get(
  "/",
  (req, res) => {

    res.json({
      ok: true,
      service:
        "AI YouTube Autopilot",
      status: "online",
      ttsModel: TTS_MODEL,
      jobs: jobs.size,
      time:
        new Date().toISOString()
    });
  }
);

app.get(
  "/health",
  (req, res) => {

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
      ttsModel:
        TTS_MODEL,
      jobs:
        jobs.size
    });
  }
);

/* =========================
   START SERVER
========================= */

app.listen(
  PORT,
  () => {

    console.log(
      `AI YouTube Autopilot listening on ${PORT}`
    );

    console.log(
      `TTS model: ${TTS_MODEL}`
    );

    if (!TELEGRAM_BOT_TOKEN) {
      console.error(
        "❌ TELEGRAM_BOT_TOKEN missing"
      );
    }

    if (!GEMINI_API_KEY) {
      console.error(
        "❌ GEMINI_API_KEY missing"
      );
    }

    if (
      TELEGRAM_BOT_TOKEN &&
      GEMINI_API_KEY
    ) {
      startTelegramPolling()
        .catch(console.error);
    }
  }
);
