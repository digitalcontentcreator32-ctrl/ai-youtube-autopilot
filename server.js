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
    text
  });
}

function createJob(command, chatId) {
  const job = {
    id: newId(),
    command,
    chatId,
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
      "AI YouTube Autopilot connected!\n\n/status - system status\\n/create <request> - create Creator Agent job\\n/stop - emergency stop"
    );
    return;
  }

  if (text === "/status") {
    await sendMessage(
      chatId,
      `System: ONLINE\\nTelegram: CONNECTED\\nJobs: ${jobs.size}\\nMode: FREE-FIRST`
    );
    return;
  }

  if (text === "/stop") {
    for (const job of jobs.values()) {
      if (
        job.chatId === chatId &&
        (job.status === "queued" ||
         job.status === "running")
      ) {
        job.status = "paused";
      }
    }

    await sendMessage(
      chatId,
      "Emergency stop applied. Jobs are paused safely."
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
      `Request saved ✅\nJob: ${job.id}\nStatus: QUEUED`
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
          await handleTelegramUpdate(update);
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

  pollTelegram();
});
