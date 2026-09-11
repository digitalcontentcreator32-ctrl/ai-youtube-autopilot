import express from "express";

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 10000;
const TOKEN = process.env.TELEGRAM_BOT_TOKEN || "";

const jobs = new Map();

function newId() {
  return "job_" + Date.now();
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
  return telegram("sendMessage", {
    chat_id: chatId,
    text: text
  });
}

function createJob(command, chatId) {
  const job = {
    id: newId(),
    command: command,
    chatId: chatId,
    status: "queued",
    progress: 0,
    createdAt: new Date().toISOString()
  };

  jobs.set(job.id, job);
  return job;
}

app.get("/", (_req, res) => {
  res.json({
    ok: true,
    service: "AI YouTube Autopilot",
    version: "1.0"
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
      error: "command is required"
    });
  }

  const job = createJob(
    command,
    req.body?.chatId || null
  );

  res.json({
    ok: true,
    job: job
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
      "AI YouTube Autopilot connected!\n\n/status - system status\n/create <request> - create Creator Agent job\n/jobs - show jobs\n/stop - emergency stop"
    );
    return;
  }

  if (text === "/status") {
    await sendMessage(
      chatId,
      `System: ONLINE\nTelegram: CONNECTED\nJobs: ${jobs.size}\nMode: FREE-FIRST`
    );
    return;
  }

  if (text === "/jobs") {
    const userJobs = [...jobs.values()]
      .filter(job => job.chatId === chatId);

    if (userJobs.length === 0) {
      await sendMessage(
        chatId,
        "No jobs found."
      );
      return;
    }

    const lines = userJobs.map(
      job =>
        `${job.id} - ${job.status.toUpperCase()} - ${job.progress}%`
    );

    await sendMessage(
      chatId,
      `Your jobs:\n\n${lines.join("\n")}`
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
        stopped++;
      }
    }

    await sendMessage(
      chatId,
      `Emergency stop applied.\nPaused jobs: ${stopped}`
    );

    return;
  }

  const command = text.startsWith("/create ")
    ? text.slice(8).trim()
    : text.startsWith("/")
      ? ""
      : text;

  if (command) {
    const job = createJob(
      command,
      chatId
    );

    await sendMessage(
      chatId,
      `Request saved ✅\n\nJob: ${job.id}\nStatus: QUEUED\nProgress: 0%`
    );

    return;
  }

  await sendMessage(
    chatId,
    "Unknown command.\n\nUse /status, /create <request>, /jobs or /stop."
  );
}

async function pollTelegram() {
  if (!TOKEN) {
    console.log(
      "TELEGRAM_BOT_TOKEN is not configured yet."
    );
    return;
  }

  let offset = 0;

  console.log(
    "Telegram polling enabled."
  );

  while (true) {
    try {
      const result = await telegram(
        "getUpdates",
        {
          timeout: 25,
          offset: offset,
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
              "Update handling error:",
              error.message
            );
          }
        }
      } else {
        console.error(
          "Telegram API error:",
          result.description
        );
      }
    } catch (error) {
      console.error(
        "Telegram connection error:",
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

  pollTelegram();
});
