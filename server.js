import express from "express";

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 10000;
const TOKEN = process.env.TELEGRAM_BOT_TOKEN || "";
const GEMINI_API_KEY = process.env.GEMINI_API_KEY || "";

const jobs = new Map();
let jobCounter = 1;
let telegramOffset = 0;

function newId() {
  return "job_" + Date.now() + "_" + jobCounter++;
}

function createJob(command, chatId) {
  const job = {
    id: newId(),
    command,
    chatId,
    status: "queued",
    progress: 0,
    stage: "Waiting",
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    result: null,
    error: null
  };

  jobs.set(job.id, job);
  return job;
}

async function telegram(method, body) {
  if (!TOKEN) throw new Error("TELEGRAM_BOT_TOKEN is not configured");

  const response = await fetch(
    `https://api.telegram.org/bot${TOKEN}/${method}`,
    {
      method: "POST",
      headers: {
        "content-type": "application/json"
      },
      body: JSON.stringify(body)
    }
  );

  return response.json();
}

async function sendMessage(chatId, text) {
  if (!chatId) return;

  try {
    await telegram("sendMessage", {
      chat_id: chatId,
      text
    });
  } catch (error) {
    console.error("Telegram send error:", error.message);
  }
}

function updateJob(job, status, progress, stage) {
  job.status = status;
  job.progress = progress;
  job.stage = stage;
  job.updatedAt = new Date().toISOString();
}

function extractGeminiText(data) {
  if (data?.output_text) {
    return data.output_text;
  }

  const steps = data?.steps || [];

  for (let i = steps.length - 1; i >= 0; i--) {
    const step = steps[i];

    if (step?.type === "model_output" && Array.isArray(step.content)) {
      const text = step.content
        .filter(x => x?.type === "text")
        .map(x => x.text || "")
        .join("\n");

      if (text.trim()) return text;
    }
  }

  return "";
}

async function generateScript(command) {
  if (!GEMINI_API_KEY) {
    throw new Error("GEMINI_API_KEY is not configured");
  }

  const prompt = `
You are the Creator Agent for an automated YouTube channel.

User request:
${command}

Create an ORIGINAL YouTube video package.

Return ONLY valid JSON with these fields:
{
  "title": "engaging YouTube title",
  "hook": "strong opening hook",
  "script": "complete narration script",
  "description": "YouTube description",
  "tags": ["tag1", "tag2", "tag3"],
  "shorts_hook": "shorts opening hook",
  "visual_plan": [
    "scene 1 visual",
    "scene 2 visual",
    "scene 3 visual"
  ]
}

Rules:
- Do not copy existing videos.
- Avoid fabricated facts.
- Make the script original and useful.
- Keep the narration natural.
- Do not include copyrighted lyrics or copied text.
- If the user asks for a Short, make the script suitable for a short-form video.
- If the user asks for a long video, make the script suitable for a long-form video.
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
        model: "gemini-3.8-flash",
        input: prompt,
        response_format: {
          type: "text",
          mime_type: "application/json",
          schema: {
            type: "object",
            properties: {
              title: { type: "string" },
              hook: { type: "string" },
              script: { type: "string" },
              description: { type: "string" },
              tags: {
                type: "array",
                items: { type: "string" }
              },
              shorts_hook: { type: "string" },
              visual_plan: {
                type: "array",
                items: { type: "string" }
              }
            },
            required: [
              "title",
              "hook",
              "script",
              "description",
              "tags",
              "shorts_hook",
              "visual_plan"
            ]
          }
        }
      })
    }
  );

  const data = await response.json();

  if (!response.ok) {
    console.error("Gemini error:", JSON.stringify(data));
    throw new Error(
      data?.error?.message ||
      `Gemini request failed with HTTP ${response.status}`
    );
  }

  const text = extractGeminiText(data);

  if (!text) {
    throw new Error("Gemini returned no text");
  }

  try {
    return JSON.parse(text);
  } catch {
    return {
      title: "AI Generated Video",
      hook: "",
      script: text,
      description: "",
      tags: [],
      shorts_hook: "",
      visual_plan: []
    };
  }
}

async function processJob(job) {
  try {
    updateJob(job, "running", 5, "Starting Creator Agent");

    await sendMessage(
      job.chatId,
      `🤖 Creator Agent started\nJob: ${job.id}\nProgress: 5%`
    );

    updateJob(job, "running", 20, "Sending request to Gemini");

    const result = await generateScript(job.command);

    updateJob(job, "running", 70, "AI script created");

    job.result = result;

    await sendMessage(
      job.chatId,
      `🧠 Gemini script generated\nJob: ${job.id}\nProgress: 70%\n\nTitle: ${result.title}`
    );

    updateJob(job, "completed", 100, "Script Ready");

    await sendMessage(
      job.chatId,
      `✅ Creator Agent finished\nJob: ${job.id}\nProgress: 100%\n\n🎬 TITLE\n${result.title}\n\n🪝 HOOK\n${result.hook}\n\n📝 SCRIPT\n${result.script.slice(0, 3500)}`
    );

  } catch (error) {
    console.error("Job error:", error);

    updateJob(job, "paused", job.progress, "Gemini/API Error");
    job.error = error.message;

    await sendMessage(
      job.chatId,
      `⚠️ Job paused\nJob: ${job.id}\n\nReason:\n${error.message}\n\nNo paid service was charged.`
    );
  }
}

function startQueuedJobs() {
  setInterval(async () => {
    const job = [...jobs.values()].find(
      j => j.status === "queued"
    );

    if (job) {
      await processJob(job);
    }
  }, 2000);
}

async function handleTelegramUpdate(update) {
  const message = update?.message;

  if (!message?.text) return;

  const chatId = message.chat.id;
  const text = message.text.trim();

  if (text === "/start") {
    await sendMessage(
      chatId,
      `🚀 AI YouTube Autopilot

ONLINE ✅

Commands:
/status
/create <your video request>
/jobs
/stop

FREE-FIRST MODE: ON`
    );
    return;
  }

  if (text === "/status") {
    const allJobs = [...jobs.values()];
    const running = allJobs.filter(j => j.status === "running").length;
    const queued = allJobs.filter(j => j.status === "queued").length;

    await sendMessage(
      chatId,
      `📊 SYSTEM STATUS

Backend: ONLINE ✅
Telegram: CONNECTED ✅
Gemini: ${GEMINI_API_KEY ? "CONFIGURED ✅" : "NOT CONFIGURED ❌"}

Running: ${running}
Queued: ${queued}
Total jobs: ${allJobs.length}

Mode: FREE-FIRST`
    );
    return;
  }

  if (text === "/jobs") {
    const userJobs = [...jobs.values()]
      .filter(j => String(j.chatId) === String(chatId))
      .slice(-10);

    if (!userJobs.length) {
      await sendMessage(chatId, "No jobs found.");
      return;
    }

    const lines = userJobs.map(
      j =>
        `${j.id}\n${j.status.toUpperCase()} | ${j.progress}% | ${j.stage}`
    );

    await sendMessage(
      chatId,
      `📋 YOUR JOBS\n\n${lines.join("\n\n")}`
    );
    return;
  }

  if (text === "/stop") {
    let stopped = 0;

    for (const job of jobs.values()) {
      if (
        String(job.chatId) === String(chatId) &&
        (job.status === "queued" || job.status === "running")
      ) {
        job.status = "paused";
        job.stage = "Stopped by user";
        job.updatedAt = new Date().toISOString();
        stopped++;
      }
    }

    await sendMessage(
      chatId,
      stopped
        ? `🛑 ${stopped} job(s) paused.`
        : "No running or queued jobs."
    );

    return;
  }

  const command = text.startsWith("/create ")
    ? text.slice(8).trim()
    : text;

  if (!command) {
    await sendMessage(
      chatId,
      "Use:\n/create Make a 30-second YouTube Short about space"
    );
    return;
  }

  const job = createJob(command, chatId);

  await sendMessage(
    chatId,
    `📥 Request saved\n\nJob: ${job.id}\nStatus: QUEUED\nProgress: 0%`
  );
}

async function pollTelegram() {
  if (!TOKEN) {
    console.log("Telegram polling disabled: token missing");
    return;
  }

  try {
    const response = await telegram("getUpdates", {
      offset: telegramOffset,
      timeout: 20
    });

    if (response?.ok && Array.isArray(response.result)) {
      for (const update of response.result) {
        telegramOffset = update.update_id + 1;

        try {
          await handleTelegramUpdate(update);
        } catch (error) {
          console.error("Telegram update error:", error.message);
        }
      }
    }
  } catch (error) {
    console.error("Telegram polling error:", error.message);
  }

  setTimeout(pollTelegram, 1000);
}

app.get("/", (req, res) => {
  res.json({
    name: "AI YouTube Autopilot",
    status: "online",
    version: "3.0-gemini"
  });
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    telegram: Boolean(TOKEN),
    gemini: Boolean(GEMINI_API_KEY),
    jobs: jobs.size
  });
});

app.get("/api/jobs", (req, res) => {
  res.json([...jobs.values()]);
});

app.post("/api/command", async (req, res) => {
  const command = String(req.body?.command || "").trim();

  if (!command) {
    return res.status(400).json({
      ok: false,
      error: "command is required"
    });
  }

  const job = createJob(command, req.body?.chatId || null);

  res.json({
    ok: true,
    job
  });
});

app.listen(PORT, () => {
  console.log(`AI YouTube Autopilot listening on ${PORT}`);
  console.log(`Telegram polling: ${TOKEN ? "enabled" : "disabled"}`);
  console.log(`Gemini: ${GEMINI_API_KEY ? "configured" : "missing"}`);

  startQueuedJobs();
  pollTelegram();
});
