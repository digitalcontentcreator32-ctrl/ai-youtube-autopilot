import express from "express";

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 10000;
const TOKEN = process.env.TELEGRAM_BOT_TOKEN || "";

const jobs = new Map();
let jobCounter = 1;

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
    updatedAt: new Date().toISOString()
  };

  jobs.set(job.id, job);
  return job;
}

async function telegram(method, body) {
  if (!TOKEN) {
    throw new Error("TELEGRAM_BOT_TOKEN is not configured");
  }

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

/*
  CREATOR AGENT PIPELINE

  Current version creates the automation pipeline/state.
  Real AI providers will be connected in the next stage.
*/
async function processJob(job) {
  if (job.status !== "queued") return;

  updateJob(job, "running", 5, "Starting Creator Agent");

  await sendMessage(
    job.chatId,
    `🚀 Creator Agent started\nJob: ${job.id}\nProgress: 5%`
  );

  await new Promise(resolve => setTimeout(resolve, 1500));

  if (job.status === "paused") return;

  updateJob(job, "running", 20, "Researching topic");

  await new Promise(resolve => setTimeout(resolve, 1500));

  if (job.status === "paused") return;

  updateJob(job, "running", 40, "Creating original script");

  await new Promise(resolve => setTimeout(resolve, 1500));

  if (job.status === "paused") return;

  updateJob(job, "running", 60, "Preparing voice and visuals");

  await new Promise(resolve => setTimeout(resolve, 1500));

  if (job.status === "paused") return;

  updateJob(job, "running", 80, "Preparing video");

  await new Promise(resolve => setTimeout(resolve, 1500));

  if (job.status === "paused") return;

  updateJob(job, "completed", 100, "Ready");

  await sendMessage(
    job.chatId,
    `✅ Creator Agent finished\n\nJob: ${job.id}\nStatus: COMPLETED\nProgress: 100%\n\n⚠️ AI video providers are not connected yet.`
  );

  console.log(`Job completed: ${job.id}`);
}

function startQueuedJobs() {
  setInterval(() => {
    for (const job of jobs.values()) {
      if (job.status === "queued") {
        processJob(job);
      }
    }
  }, 2000);
}

app.get("/", (_req, res) => {
  res.json({
    ok: true,
    service: "AI YouTube Autopilot",
    version: "2.0"
  });
});

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "AI YouTube Autopilot",
    telegramConfigured: Boolean(TOKEN),
    jobs: jobs.size
  });
});

app.get("/api/jobs", (_req, res) => {
  res.json({
    jobs: [...jobs.values()]
  });
});

app.post("/api/command", (req, res) => {
  const command = String(req.body?.command || "").trim();

  if (!command) {
    return res.status(400).json({
      ok: false,
      error: "command is required"
    });
  }

  const job = createJob(
    command,
    req.body?.chatId || null
  );

  res.json({
    ok: true,
    job
  });
});

async function handleTelegramUpdate(update) {
  const message = update?.message;

  if (!message?.chat?.id || !message?.text) {
    return;
  }

  const chatId = message.chat.id;
  const text = message.text.trim();

  if (text === "/start") {
    await sendMessage(
      chatId,
      `AI YouTube Autopilot connected! 🤖

/status - system status
/create <request> - create Creator Agent job
/jobs - show jobs
/stop - emergency stop`
    );
    return;
  }

  if (text === "/status") {
    const running = [...jobs.values()]
      .filter(job => job.status === "running").length;

    const queued = [...jobs.values()]
      .filter(job => job.status === "queued").length;

    await sendMessage(
      chatId,
      `System: ONLINE
Telegram: CONNECTED
Running: ${running}
Queued: ${queued}
Total jobs: ${jobs.size}
Mode: FREE-FIRST`
    );
    return;
  }

  if (text === "/jobs") {
    const userJobs = [...jobs.values()]
      .filter(job => job.chatId === chatId);

    if (userJobs.length === 0) {
      await sendMessage(chatId, "No jobs found.");
      return;
    }

    const lines = userJobs.map(job =>
      `${job.id}\n${job.status.toUpperCase()} | ${job.progress}% | ${job.stage}`
    );

    await sendMessage(
      chatId,
      `Your jobs:\n\n${lines.join("\n\n")}`
    );
    return;
  }

  if (text === "/stop") {
    let stopped = 0;

    for (const job of jobs.values()) {
      if (
        job.chatId === chatId &&
        (job.status === "queued" ||
         job.status === "running")
      ) {
        job.status = "paused";
        job.stage = "Paused by emergency stop";
        job.updatedAt = new Date().toISOString();
        stopped++;
      }
    }

    await sendMessage(
      chatId,
      `🛑 Emergency stop applied.\nPaused jobs: ${stopped}`
    );

    return;
  }

  const command = text.startsWith("/create ")
    ? text.slice(8).trim()
    : text.startsWith("/")
      ? ""
      : text;

  if (command) {
    const job = createJob(command, chatId);

    await sendMessage(
      chatId,
      `Request saved ✅

Job: ${job.id}
Status: QUEUED
Progress: 0%

Creator Agent will start processing.`
    );
  }
}

async function pollTelegram() {
  if (!TOKEN) {
    console.log(
      "TELEGRAM_BOT_TOKEN is not configured yet."
    );
    return;
  }

  let offset = 0;

  console.log("Telegram polling enabled.");

  while (true) {
    try {
      const result = await telegram(
        "getUpdates",
        {
          timeout: 25,
          offset,
          allowed_updates: ["message"]
        }
      );

      if (result.ok) {
        for (const update of result.result || []) {
          offset = update.update_id + 1;

          try {
            await handleTelegramUpdate(update);
          } catch (error) {
            console.error(
              "Update error:",
              error.message
            );
          }
        }
      }
    } catch (error) {
      console.error(
        "Telegram error:",
        error.message
      );

      await new Promise(resolve =>
        setTimeout(resolve, 5000)
      );
    }
  }
}

app.listen(PORT, () => {
  console.log(
    `AI YouTube Autopilot listening on ${PORT}`
  );

  startQueuedJobs();
  pollTelegram();
});
