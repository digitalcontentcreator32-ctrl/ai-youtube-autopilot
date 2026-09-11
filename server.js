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

const jobs = new Map();

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

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

  const steps = data?.steps || [];

  for (const step of steps) {
    if (step?.type === "model_output") {
      const content = step?.content;

      if (typeof content === "string") {
        return content;
      }

      if (Array.isArray(content)) {
        for (const item of content) {
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
You are the Creator Agent of an AI YouTube Autopilot system.

Create an ORIGINAL YouTube video package from this user request:

${command}

Return these sections:

TITLE:
DESCRIPTION:
SCRIPT:
HOOK:
KEYWORDS:

Rules:
- Original content only.
- Do not copy another creator's script.
- Make the script engaging and natural.
- Avoid unsupported fake facts.
- If facts are uncertain, clearly avoid presenting them as certain.
- Suitable for a faceless YouTube channel.
- Do not include copyrighted lyrics or copied text.
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

  const rawText = await response.text();

  let data;

  try {
    data = JSON.parse(rawText);
  } catch {
    data = { raw: rawText };
  }

  if (!response.ok) {
    const errorMessage =
      data?.error?.message ||
      data?.message ||
      data?.raw ||
      `Gemini HTTP ${response.status}`;

    const error = new Error(errorMessage);
    error.status = response.status;
    error.temporary = isTemporaryGeminiError(
      response.status,
      errorMessage
    );

    throw error;
  }

  if (data?.errors?.length) {
    const errorMessage = data.errors
      .map(x => x?.message || JSON.stringify(x))
      .join("; ");

    const error = new Error(errorMessage);
    error.status = 500;
    error.temporary = isTemporaryGeminiError(500, errorMessage);

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

      const result = await generateWithModel(model, command);

      return {
        model,
        text: result
      };
    } catch (error) {
      lastError = error;

      console.log(
        `Model ${model} failed: ${error.message}`
      );

      if (!error.temporary) {
        throw error;
      }

      // Small delay before trying the next model.
      await sleep(1500);
    }
  }

  throw new Error(
    `All Gemini fallback models failed. Last error: ${
      lastError?.message || "Unknown error"
    }`
  );
}

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

function createJob(command, chatId) {
  const id = `job_${Date.now()}_${jobs.size + 1}`;

  const job = {
    id,
    command,
    chatId,
    status: "queued",
    progress: 0,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    result: null,
    error: null
  };

  jobs.set(id, job);

  return job;
}

async function processJob(job) {
  try {
    job.status = "running";
    job.progress = 5;
    job.updatedAt = new Date().toISOString();

    await sendTelegram(
      job.chatId,
      `🤖 Creator Agent started\n\nJob: ${job.id}\nProgress: 5%\n\nTrying Gemini automatically...`
    );

    const result = await generateScript(job.command);

    job.status = "completed";
    job.progress = 100;
    job.result = result;
    job.updatedAt = new Date().toISOString();

    const preview =
      result.text.length > 3500
        ? result.text.slice(0, 3500) + "\n\n...[preview truncated]"
        : result.text;

    await sendTelegram(
      job.chatId,
      `✅ Creator Agent completed\n\nJob: ${job.id}\nModel: ${result.model}\nProgress: 100%\n\n${preview}`
    );

  } catch (error) {
    job.status = "paused";
    job.error = error.message;
    job.updatedAt = new Date().toISOString();

    await sendTelegram(
      job.chatId,
      `⚠️ Job paused\n\nJob: ${job.id}\nReason:\n${error.message}\n\nNo paid service was charged.`
    );
  }
}

async function handleTelegramMessage(message) {
  const chatId = message?.chat?.id;
  const text = message?.text?.trim();

  if (!chatId || !text) {
    return;
  }

  if (text === "/start") {
    await sendTelegram(
      chatId,
      `🤖 AI YouTube Autopilot\n\nCommands:\n\n/create <topic> - Create a video script\n/status - System status\n/jobs - Show jobs\n/stop - Stop bot polling`
    );
    return;
  }

  if (text === "/status") {
    await sendTelegram(
      chatId,
      `📊 System Status\n\nTelegram: ✅ Connected\nGemini: ${
        GEMINI_API_KEY ? "✅ Connected" : "❌ Not configured"
      }\n\nFallback models:\n3.8 Flash → 3.7 Flash → 3.6 Flash → 3.5 Flash-Lite\n\nJobs: ${jobs.size}`
    );
    return;
  }

  if (text === "/jobs") {
    if (jobs.size === 0) {
      await sendTelegram(chatId, "📭 No jobs found.");
      return;
    }

    let output = "📋 Jobs\n\n";

    for (const job of jobs.values()) {
      output += `${job.id}\nStatus: ${job.status}\nProgress: ${job.progress}%\n\n`;
    }

    await sendTelegram(chatId, output);
    return;
  }

  if (text === "/stop") {
    await sendTelegram(
      chatId,
      "🛑 Current bot process cannot permanently stop Render polling from Telegram. Use Render to stop/restart the service."
    );
    return;
  }

  if (text.startsWith("/create ")) {
    const command = text.slice(8).trim();

    if (!command) {
      await sendTelegram(
        chatId,
        "Example:\n/create 5 surprising facts about space"
      );
      return;
    }

    const job = createJob(command, chatId);

    await sendTelegram(
      chatId,
      `📝 Job created\n\nJob: ${job.id}\nStatus: queued`
    );

    processJob(job);
    return;
  }

  await sendTelegram(
    chatId,
    "Unknown command.\n\nUse /start to see available commands."
  );
}

let telegramOffset = 0;
let pollingRunning = false;

async function telegramPolling() {
  if (pollingRunning || !TELEGRAM_BOT_TOKEN) {
    return;
  }

  pollingRunning = true;

  console.log("Telegram polling enabled");

  while (true) {
    try {
      const result = await telegram("getUpdates", {
        offset: telegramOffset,
        timeout: 25,
        allowed_updates: ["message"]
      });

      if (result?.ok && Array.isArray(result.result)) {
        for (const update of result.result) {
          telegramOffset = update.update_id + 1;

          try {
            await handleTelegramMessage(update.message);
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

app.get("/", (req, res) => {
  res.json({
    name: "AI YouTube Autopilot",
    status: "online",
    telegram: Boolean(TELEGRAM_BOT_TOKEN),
    gemini: Boolean(GEMINI_API_KEY),
    models: GEMINI_MODELS
  });
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    telegram: Boolean(TELEGRAM_BOT_TOKEN),
    gemini: Boolean(GEMINI_API_KEY),
    jobs: jobs.size
  });
});

app.get("/api/jobs", (req, res) => {
  res.json({
    jobs: Array.from(jobs.values())
  });
});

app.post("/api/command", async (req, res) => {
  const command = req.body?.command;

  if (!command) {
    return res.status(400).json({
      error: "command is required"
    });
  }

  const job = {
    id: `api_${Date.now()}`,
    command,
    chatId: null,
    status: "queued",
    progress: 0,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };

  jobs.set(job.id, job);

  processJob(job);

  res.json({
    ok: true,
    job
  });
});

app.listen(PORT, () => {
  console.log(
    `AI YouTube Autopilot listening on ${PORT}`
  );

  console.log(
    `Gemini configured: ${Boolean(GEMINI_API_KEY)}`
  );

  console.log(
    `Telegram configured: ${Boolean(TELEGRAM_BOT_TOKEN)}`
  );

  telegramPolling();
});
