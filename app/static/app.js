const KEY_SLOT = "soup_api_key";
let apiKey = localStorage.getItem(KEY_SLOT) || "";
let activeRunId = null;
let streamGeneration = 0;
let cursor = 0;
let eventCount = 0;
let visibleEvents = [];
let noticeTimer = null;

const $ = (id) => document.getElementById(id);
const show = (element) => element.classList.remove("hidden");
const hide = (element) => element.classList.add("hidden");

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[char]);
}

async function request(path, options = {}) {
  const headers = new Headers(options.headers || {});
  if (apiKey) headers.set("X-API-Key", apiKey);
  if (options.body && !headers.has("Content-Type")) headers.set("Content-Type", "application/json");
  const response = await fetch(path, { ...options, headers, cache: "no-store" });
  const data = response.status === 204 ? null : await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data?.detail || `Ошибка запроса (${response.status})`);
  return data;
}

function setConnection(online, label = online ? "Подключено" : "Не подключено") {
  const el = $("connection");
  el.classList.toggle("online", online);
  el.lastElementChild.textContent = label;
}

function toast(message) {
  const el = $("notice");
  el.textContent = message;
  show(el);
  clearTimeout(noticeTimer);
  noticeTimer = setTimeout(() => hide(el), 3400);
}

function showError(id, message) {
  const el = $(id);
  el.textContent = message;
  show(el);
}

function formatDate(value) {
  return new Intl.DateTimeFormat("ru-RU", { dateStyle: "medium", timeStyle: "short" }).format(new Date(value));
}

async function boot() {
  if (!apiKey) {
    hide($("app"));
    show($("setup"));
    setConnection(false);
    return;
  }
  try {
    const project = await request("/api/project");
    $("project-title").textContent = project.name;
    hide($("setup"));
    show($("app"));
    setConnection(true);
    await loadRuns();
  } catch (error) {
    localStorage.removeItem(KEY_SLOT);
    apiKey = "";
    hide($("app"));
    show($("setup"));
    showError("setup-error", `${error.message}. Создайте новый проект или вставьте действующий ключ.`);
    setConnection(false);
  }
}

async function loadRuns() {
  const result = await request("/api/runs");
  const list = $("runs-list");
  list.replaceChildren();
  hide($("runs-error"));
  if (!result.runs.length) {
    show($("empty-runs"));
    return;
  }
  hide($("empty-runs"));
  for (const run of result.runs) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "run-card";
    button.innerHTML = `<span><strong>${escapeHtml(run.name)}</strong><small>Создан ${escapeHtml(formatDate(run.created_at))}</small></span><span class="run-count">${run.event_count} событий　→</span>`;
    button.addEventListener("click", () => openRun(run.id));
    list.append(button);
  }
}

function setStreamStatus(online, label) {
  const el = $("stream-status");
  el.classList.toggle("offline", !online);
  el.lastChild.textContent = ` ${label}`;
}

function renderEvents(events) {
  visibleEvents = [...events].slice(-500);
  const body = $("metrics-body");
  body.replaceChildren();
  for (const event of [...visibleEvents].sort((a, b) => b.server_seq - a.server_seq)) {
    const row = document.createElement("tr");
    const clientTime = event.client_time ? formatDate(event.client_time) : "—";
    row.innerHTML = `<td class="seq">${event.server_seq}</td><td>${escapeHtml(event.metric_name)}</td><td class="val">${Number(event.value).toLocaleString("ru-RU", { maximumFractionDigits: 6 })}</td><td>${escapeHtml(clientTime)}</td><td>${escapeHtml(formatDate(event.received_at))}</td>`;
    body.append(row);
  }
  $("event-count").textContent = String(eventCount);
  $("cursor-value").textContent = String(cursor);
  if (events.length) {
    hide($("empty-metrics"));
    show($("metrics-wrap"));
  } else {
    show($("empty-metrics"));
    hide($("metrics-wrap"));
  }
}

async function openRun(runId) {
  streamGeneration += 1;
  activeRunId = runId;
  const snapshot = await request(`/api/runs/${runId}`);
  cursor = snapshot.cursor;
  eventCount = snapshot.cursor;
  $("run-title").textContent = snapshot.name;
  renderEvents(snapshot.events);
  $("curl-example").textContent = `curl -X POST http://localhost:8000/api/runs/${runId}/metrics \\
  -H 'Content-Type: application/json' \\
  -H 'X-API-Key: YOUR_API_KEY' \\
  -d '{"batch_id":"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11","events":[{"event_id":"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12","metric_name":"accuracy","value":0.91,"client_time":"${new Date().toISOString()}"}]}'`;
  hide($("runs-view"));
  show($("detail-view"));
  setStreamStatus(true, "Подключение…");
  const generation = streamGeneration;
  consumeStream(runId, generation).catch((error) => {
    if (generation === streamGeneration) setStreamStatus(false, error.message);
  });
}

function applyEvent(event) {
  if (!event || event.server_seq <= cursor) return;
  cursor = event.server_seq;
  eventCount += 1;
  renderEvents([...visibleEvents, event]);
}

async function consumeStream(runId, generation) {
  let retryMs = 400;
  while (generation === streamGeneration && activeRunId === runId) {
    try {
      const response = await fetch(`/api/runs/${runId}/events?after=${cursor}`, {
        headers: { "X-API-Key": apiKey, "Accept": "text/event-stream" },
        cache: "no-store",
      });
      if (response.status === 401 || response.status === 404) throw new Error("Доступ к потоку закрыт");
      if (!response.ok || !response.body) throw new Error(`Поток недоступен (${response.status})`);
      setStreamStatus(true, "В реальном времени");
      retryMs = 400;
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      while (generation === streamGeneration && activeRunId === runId) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true }).replace(/\r\n/g, "\n");
        let boundary;
        while ((boundary = buffer.indexOf("\n\n")) >= 0) {
          const block = buffer.slice(0, boundary);
          buffer = buffer.slice(boundary + 2);
          const eventName = block.match(/^event:\s*(.+)$/m)?.[1];
          const dataLine = block.match(/^data:\s*(.+)$/m)?.[1];
          if (eventName === "auth-error") throw new Error("Ключ отозван или доступ закрыт");
          if (eventName === "metric" && dataLine) applyEvent(JSON.parse(dataLine));
        }
      }
    } catch (error) {
      if (generation !== streamGeneration) return;
      setStreamStatus(false, "Связь прервана — переподключаемся");
      if (error.message.includes("Доступ") || error.message.includes("отозван")) {
        showError("detail-error", error.message);
        return;
      }
    }
    await new Promise((resolve) => setTimeout(resolve, retryMs));
    retryMs = Math.min(retryMs * 1.7, 4000);
  }
}

$("project-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  hide($("setup-error"));
  try {
    const result = await request("/api/projects", {
      method: "POST",
      body: JSON.stringify({ name: $("project-name").value.trim() }),
    });
    apiKey = result.api_key;
    localStorage.setItem(KEY_SLOT, apiKey);
    toast("Проект создан. API-ключ доступен в боковой панели.");
    await boot();
  } catch (error) {
    showError("setup-error", error.message);
  }
});

$("key-login-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  apiKey = $("existing-key").value.trim();
  localStorage.setItem(KEY_SLOT, apiKey);
  await boot();
});

$("new-run").addEventListener("click", () => {
  show($("run-form"));
  $("run-name").focus();
});
$("cancel-run").addEventListener("click", () => hide($("run-form")));
$("run-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    const result = await request("/api/runs", { method: "POST", body: JSON.stringify({ name: $("run-name").value.trim() }) });
    hide($("run-form"));
    $("run-name").value = "";
    await loadRuns();
    await openRun(result.id);
  } catch (error) {
    showError("runs-error", error.message);
  }
});

$("back-runs").addEventListener("click", async () => {
  streamGeneration += 1;
  activeRunId = null;
  hide($("detail-view"));
  show($("runs-view"));
  await loadRuns().catch((error) => showError("runs-error", error.message));
});

$("send-demo").addEventListener("click", async () => {
  if (!activeRunId) return;
  const eventId = crypto.randomUUID();
  const batchId = crypto.randomUUID();
  const names = ["accuracy", "loss", "latency_ms"];
  const metricName = names[Math.floor(Math.random() * names.length)];
  const value = metricName === "accuracy" ? Number((0.8 + Math.random() * 0.19).toFixed(4)) :
    metricName === "loss" ? Number((Math.random() * 0.7).toFixed(4)) : Math.round(20 + Math.random() * 180);
  try {
    await request(`/api/runs/${activeRunId}/metrics`, {
      method: "POST",
      body: JSON.stringify({ batch_id: batchId, events: [{ event_id: eventId, metric_name: metricName, value, client_time: new Date().toISOString() }] }),
    });
  } catch (error) {
    showError("detail-error", error.message);
  }
});

$("show-key").addEventListener("click", () => {
  $("key-value").value = apiKey;
  const panel = $("key-panel");
  panel.classList.toggle("hidden");
});
$("copy-key").addEventListener("click", async () => {
  await navigator.clipboard.writeText(apiKey);
  toast("API-ключ скопирован");
});
$("rotate-key").addEventListener("click", async () => {
  if (!confirm("Старый ключ сразу перестанет работать. Продолжить?")) return;
  try {
    const result = await request("/api/keys/rotate", { method: "POST" });
    apiKey = result.api_key;
    localStorage.setItem(KEY_SLOT, apiKey);
    $("key-value").value = apiKey;
    show($("key-panel"));
    if (activeRunId) {
      streamGeneration += 1;
      consumeStream(activeRunId, streamGeneration).catch(() => setStreamStatus(false, "Не подключено"));
    }
    toast("Ключ обновлён. Сохраните новый ключ в программе отправки.");
  } catch (error) {
    toast(error.message);
  }
});
$("forget-key").addEventListener("click", () => {
  streamGeneration += 1;
  localStorage.removeItem(KEY_SLOT);
  apiKey = "";
  activeRunId = null;
  hide($("app"));
  show($("setup"));
  setConnection(false);
});
$("copy-example").addEventListener("click", async () => {
  await navigator.clipboard.writeText($("curl-example").textContent);
  toast("Пример скопирован. Подставьте API-ключ.");
});

boot();
