/* DAG generator web UI.
 *
 * Plain DOM, no framework and no build step - the page has one job and a
 * handful of states, which is less code than any runtime would cost.
 *
 * Every string that comes back from the server is inserted with `textContent`
 * or through `highlightYaml`, which escapes before it adds markup. Generated
 * YAML, reasoning and validation errors all contain characters that would be
 * read as HTML, so nothing from a response is ever assigned to `innerHTML`.
 */

"use strict";

const SUGGESTIONS = [
  "Import the users table into Iceberg",
  "Load transactions hourly, append instead of overwrite",
  "Ingest the accounts table into Iceberg, daily at 2am",
];

const state = {
  sessionId: null,
  ready: false,
  busy: false,
  panels: {},
  panel: "tables",
};

const el = {
  app: document.getElementById("app"),
  scrim: document.getElementById("scrim"),
  sidebar: document.getElementById("sidebar"),
  sidebarBtn: document.getElementById("sidebar-btn"),
  thread: document.getElementById("thread"),
  threadInner: document.getElementById("thread-inner"),
  composer: document.getElementById("composer"),
  input: document.getElementById("input"),
  sendBtn: document.getElementById("send-btn"),
  resetBtn: document.getElementById("reset-btn"),
  panelBody: document.getElementById("panel-body"),
  providerName: document.getElementById("provider-name"),
  statusDot: document.getElementById("status-dot"),
  toasts: document.getElementById("toasts"),
};

/* ── Small DOM helpers ──────────────────────────────────────────────────── */

function node(tag, className, text) {
  const n = document.createElement(tag);
  if (className) n.className = className;
  if (text !== undefined && text !== null) n.textContent = text;
  return n;
}

function icon(id, size = 14) {
  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("width", String(size));
  svg.setAttribute("height", String(size));
  svg.setAttribute("viewBox", "0 0 16 16");
  svg.setAttribute("aria-hidden", "true");
  const use = document.createElementNS("http://www.w3.org/2000/svg", "use");
  use.setAttribute("href", `#${id}`);
  svg.appendChild(use);
  return svg;
}

function button(label, { variant, size, iconId, onClick, title } = {}) {
  const b = node("button", "btn");
  b.type = "button";
  if (variant) b.dataset.variant = variant;
  if (size) b.dataset.size = size;
  if (title) b.title = title;
  if (iconId) b.appendChild(icon(iconId, 13));
  b.appendChild(document.createTextNode(label));
  if (onClick) b.addEventListener("click", onClick);
  return b;
}

function badge(kind, label, iconId) {
  const b = node("span", "badge");
  b.dataset.kind = kind;
  if (iconId) b.appendChild(icon(iconId, 11));
  b.appendChild(document.createTextNode(label));
  return b;
}

/** A titled note block: warnings, questions, or validation errors. */
function noteBlock(kind, title, items, iconId) {
  const box = node("div", "note");
  box.dataset.kind = kind;

  const head = node("div", "note-title");
  if (iconId) head.appendChild(icon(iconId, 12));
  head.appendChild(document.createTextNode(title));
  box.appendChild(head);

  if (items.length === 1) {
    box.appendChild(node("p", null, items[0]));
  } else {
    const ul = node("ul");
    for (const item of items) ul.appendChild(node("li", null, item));
    box.appendChild(ul);
  }
  return box;
}

function scrollToEnd() {
  el.thread.scrollTop = el.thread.scrollHeight;
}

/* ── Toasts ─────────────────────────────────────────────────────────────── */

function toast(message, kind = "ok") {
  const t = node("div", "toast");
  t.dataset.kind = kind;
  t.appendChild(icon(kind === "ok" ? "i-check" : "i-alert", 14));
  t.appendChild(node("span", null, message));
  el.toasts.appendChild(t);
  setTimeout(() => t.remove(), kind === "ok" ? 3200 : 6000);
}

/* ── YAML highlighting ──────────────────────────────────────────────────── */

/* Just enough to make the structure readable: keys, strings, numbers and
 * comments. Escaping happens first, so the returned markup can only ever
 * contain the spans added here. */
function highlightYaml(text) {
  const escaped = text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");

  return escaped
    .split("\n")
    .map((line) => {
      const comment = line.match(/^(\s*)(#.*)$/);
      if (comment) return `${comment[1]}<span class="c">${comment[2]}</span>`;

      // `key:` and `- key:` at the start of a line, plus its value.
      return line.replace(
        /^(\s*-?\s*)([A-Za-z_][\w.-]*)(:)(\s*)(.*)$/,
        (_m, indent, key, colon, gap, rest) => {
          let value = rest;
          if (/^(["']).*\1$/.test(rest) || /^[@*]/.test(rest)) {
            value = `<span class="s">${rest}</span>`;
          } else if (/^-?\d+(\.\d+)?$/.test(rest)) {
            value = `<span class="n">${rest}</span>`;
          } else if (/^(true|false|null|~)$/i.test(rest)) {
            value = `<span class="n">${rest}</span>`;
          }
          return `${indent}<span class="k">${key}</span>${colon}${gap}${value}`;
        },
      );
    })
    .join("\n");
}

/* ── Welcome / empty state ──────────────────────────────────────────────── */

function renderWelcome() {
  el.threadInner.replaceChildren();

  const wrap = node("div", "welcome");
  wrap.appendChild(node("h3", null, "Describe the pipeline you need"));
  wrap.appendChild(
    node(
      "p",
      null,
      "Name a source table and where it should land. Every proposal is checked " +
        "against the real blueprints, the live source catalog and the DAGs that " +
        "already exist — and nothing reaches airflow/dags until you save it.",
    ),
  );

  wrap.appendChild(node("div", "suggest-label", "Try one"));
  const list = node("div", "suggestions");
  for (const text of SUGGESTIONS) {
    const s = node("button", "suggestion", text);
    s.type = "button";
    s.addEventListener("click", () => {
      el.input.value = text;
      resize();
      el.input.focus();
      syncSendButton();
    });
    list.appendChild(s);
  }
  wrap.appendChild(list);
  el.threadInner.appendChild(wrap);
}

/* ── Turn rendering ─────────────────────────────────────────────────────── */

function addUserTurn(text) {
  // The welcome block is the empty state; the first real turn replaces it.
  const welcome = el.threadInner.querySelector(".welcome");
  if (welcome) welcome.remove();

  const turn = node("div", "turn");
  turn.appendChild(node("div", "bubble-user", text));
  el.threadInner.appendChild(turn);
  scrollToEnd();
}

function addThinking() {
  const turn = node("div", "turn");
  turn.dataset.role = "thinking";

  const row = node("div", "thinking");
  const dots = node("span", "thinking-dots");
  dots.append(node("i"), node("i"), node("i"));
  row.appendChild(dots);
  row.appendChild(node("span", null, "Grounding the request and validating the DAG…"));
  turn.appendChild(row);

  const skeleton = node("div", "skeleton");
  for (const width of ["62%", "88%", "74%", "45%"]) {
    const bar = node("span");
    bar.style.width = width;
    skeleton.appendChild(bar);
  }
  turn.appendChild(skeleton);

  el.threadInner.appendChild(turn);
  scrollToEnd();
  return turn;
}

/** The result card: path, status, the YAML, and the save control. */
function buildCard(data) {
  const card = node("div", "card");

  const head = node("div", "card-head");
  head.appendChild(node("span", "card-path", data.file_path || data.dag_id));
  head.appendChild(node("span", "spacer"));
  if (data.validated) {
    head.appendChild(badge("ok", "Validated", "i-check"));
  }
  if (data.attempts > 1) {
    head.appendChild(
      node("span", "attempts", `${data.attempts} attempts`),
    );
  }
  card.appendChild(head);

  const pre = node("pre", "code");
  const code = document.createElement("code");
  code.innerHTML = highlightYaml(data.yaml); // escaped inside highlightYaml
  pre.appendChild(code);
  card.appendChild(pre);

  const foot = node("div", "card-foot");

  const savedNote = node("span", "saved-note");
  // Shows the repo-relative path; the absolute one is mostly a home directory
  // and would wrap across the footer, so it becomes the tooltip instead.
  const renderSaved = (relative, absolute) => {
    savedNote.replaceChildren();
    savedNote.appendChild(icon("i-check", 13));
    savedNote.appendChild(document.createTextNode("Saved to "));
    savedNote.appendChild(node("code", null, relative));
    if (absolute) savedNote.title = absolute;
  };

  const copyBtn = button("Copy", {
    variant: "ghost",
    iconId: "i-copy",
    onClick: async () => {
      try {
        await navigator.clipboard.writeText(data.yaml);
        toast("YAML copied to the clipboard.");
      } catch {
        toast("Could not reach the clipboard. Select the YAML and copy it.", "danger");
      }
    },
  });

  const saveBtn = button("Save to airflow/dags/", {
    variant: "primary",
    iconId: "i-save",
    onClick: async () => {
      saveBtn.disabled = true;
      saveBtn.replaceChildren(document.createTextNode("Saving…"));
      try {
        const result = await api("/api/save", { session_id: state.sessionId });
        saveBtn.remove();
        renderSaved(result.relative_to || result.written_to, result.written_to);
        toast("Saved. The scheduler picks it up on its next scan.");
      } catch (err) {
        saveBtn.disabled = false;
        saveBtn.replaceChildren();
        saveBtn.appendChild(icon("i-save", 13));
        saveBtn.appendChild(document.createTextNode("Save to airflow/dags/"));
        toast(err.message, "danger");
      }
    },
  });

  if (data.written_to) {
    renderSaved(data.file_path || data.written_to, data.written_to);
    foot.appendChild(savedNote);
    foot.appendChild(node("span", "spacer"));
    foot.appendChild(copyBtn);
  } else if (data.savable) {
    foot.appendChild(copyBtn);
    foot.appendChild(node("span", "spacer"));
    foot.appendChild(savedNote);
    foot.appendChild(saveBtn);
  } else {
    foot.appendChild(copyBtn);
  }

  card.appendChild(foot);
  return card;
}

function renderResult(turn, data) {
  turn.replaceChildren();
  delete turn.dataset.role;

  if (data.reasoning) {
    turn.appendChild(node("div", "reasoning", data.reasoning));
  }

  // The model refused to invent what it cannot see: a terminal answer, not a
  // failure. Show the questions and let the next turn answer them.
  if (data.status === "needs_clarification") {
    turn.appendChild(
      noteBlock(
        "ask",
        "Needs clarification",
        data.questions.length ? data.questions : ["Add more detail and try again."],
        "i-ask",
      ),
    );
    scrollToEnd();
    return;
  }

  if (!data.validated) {
    turn.appendChild(
      noteBlock(
        "danger",
        `Validation failed after ${data.attempts} attempt${data.attempts === 1 ? "" : "s"}`,
        data.errors.length ? data.errors : ["The generated YAML did not validate."],
        "i-x",
      ),
    );
    if (data.yaml) {
      const card = buildCard({ ...data, savable: false, written_to: null });
      card.querySelector(".card-head").appendChild(
        badge("danger", "Not written", "i-alert"),
      );
      turn.appendChild(card);
    }
    scrollToEnd();
    return;
  }

  turn.appendChild(buildCard(data));

  // Warnings are things that are true even though the DAG is correct, so they
  // sit after the card rather than blocking it.
  if (data.warnings.length) {
    turn.appendChild(noteBlock("warn", "Worth knowing", data.warnings, "i-alert"));
  }

  scrollToEnd();
}

/* ── Sidebar panels ─────────────────────────────────────────────────────── */

function renderPanel() {
  const text = state.panels[state.panel];
  el.panelBody.replaceChildren();

  if (!state.ready) {
    el.panelBody.appendChild(
      node("p", "panel-empty", "Waiting for the source database…"),
    );
    return;
  }
  if (!text || !text.trim()) {
    el.panelBody.appendChild(node("p", "panel-empty", "Nothing here yet."));
    return;
  }
  el.panelBody.appendChild(node("pre", null, text));
}

function selectPanel(name) {
  state.panel = name;
  for (const tab of document.querySelectorAll(".panel-tab")) {
    tab.setAttribute("aria-selected", String(tab.dataset.panel === name));
  }
  renderPanel();
  el.panelBody.scrollTop = 0;
}

/* ── Server calls ───────────────────────────────────────────────────────── */

async function api(path, body) {
  const response = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  let payload = null;
  try {
    payload = await response.json();
  } catch {
    /* A proxy or a crash can return a non-JSON body; fall through to status. */
  }

  if (!response.ok) {
    const detail =
      (payload && (payload.detail || payload.message)) ||
      `Request failed (HTTP ${response.status}).`;
    throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
  }
  return payload;
}

async function bootstrap() {
  let data;
  try {
    const response = await fetch("/api/bootstrap");
    data = await response.json();
  } catch (err) {
    state.ready = false;
    el.statusDot.dataset.state = "down";
    el.providerName.textContent = "server unreachable";
    renderPanel();
    showStartupError(
      "Could not reach the generator server. Is it still running?\n\n" + err.message,
    );
    return;
  }

  state.sessionId = data.session_id;
  state.panels = {
    tables: data.panels.tables,
    dags: data.panels.dags,
    support: data.support,
  };

  const label = data.model ? `${data.provider} · ${data.model}` : data.provider;
  el.providerName.textContent = label;
  el.providerName.title = label;

  if (!data.ready) {
    state.ready = false;
    el.statusDot.dataset.state = "down";
    renderPanel();
    showStartupError(data.error || "Grounding is unavailable.");
    return;
  }

  state.ready = true;
  el.statusDot.dataset.state = "up";
  renderPanel();
  renderWelcome();
  el.input.disabled = false;
  syncSendButton();
  el.input.focus();
}

/** Introspection failed, so no request can be grounded. Say why, in place. */
function showStartupError(message) {
  el.threadInner.replaceChildren();
  const turn = node("div", "turn");
  turn.appendChild(
    noteBlock("danger", "Cannot read the source database", [message], "i-alert"),
  );
  el.threadInner.appendChild(turn);

  el.input.disabled = true;
  el.sendBtn.disabled = true;
}

/* ── Composer ───────────────────────────────────────────────────────────── */

function resize() {
  el.input.style.height = "auto";
  el.input.style.height = `${Math.min(el.input.scrollHeight, 168)}px`;
}

function syncSendButton() {
  el.sendBtn.disabled = state.busy || !state.ready || !el.input.value.trim();
}

function setBusy(busy) {
  state.busy = busy;
  el.input.disabled = busy || !state.ready;
  syncSendButton();
}

async function submit() {
  const request = el.input.value.trim();
  if (!request || state.busy || !state.ready) return;

  el.input.value = "";
  resize();
  setBusy(true);
  addUserTurn(request);
  const turn = addThinking();

  try {
    const data = await api("/api/generate", {
      session_id: state.sessionId,
      request,
    });
    renderResult(turn, data);
  } catch (err) {
    turn.replaceChildren();
    delete turn.dataset.role;
    turn.appendChild(
      noteBlock("danger", "Generation failed", [err.message], "i-x"),
    );
    scrollToEnd();
  } finally {
    setBusy(false);
    el.input.focus();
  }
}

/* ── Wiring ─────────────────────────────────────────────────────────────── */

el.composer.addEventListener("submit", (event) => {
  event.preventDefault();
  submit();
});

el.input.addEventListener("input", () => {
  resize();
  syncSendButton();
});

el.input.addEventListener("keydown", (event) => {
  // Enter sends; Shift+Enter is a newline. IME composition must not submit.
  if (event.key === "Enter" && !event.shiftKey && !event.isComposing) {
    event.preventDefault();
    submit();
  }
});

for (const tab of document.querySelectorAll(".panel-tab")) {
  tab.addEventListener("click", () => selectPanel(tab.dataset.panel));
}

el.resetBtn.addEventListener("click", async () => {
  if (state.busy) return;
  try {
    const data = await api("/api/reset", { session_id: state.sessionId });
    state.sessionId = data.session_id;
  } catch {
    /* A new id is generated below regardless; the old session ages out. */
  }
  renderWelcome();
  toast("Conversation cleared. The source catalog is kept.");
  el.input.focus();
});

const closeSidebar = () => el.app.setAttribute("data-sidebar", "closed");

el.sidebarBtn.addEventListener("click", () => {
  const open = el.app.getAttribute("data-sidebar") === "open";
  el.app.setAttribute("data-sidebar", open ? "closed" : "open");
});

el.scrim.addEventListener("click", closeSidebar);

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") closeSidebar();
});

/* The full placeholder is three lines on a phone, which buries the composer.
 * Swapped rather than truncated, so the short form still says what to type. */
const narrow = window.matchMedia("(max-width: 860px)");
const syncPlaceholder = () => {
  el.input.placeholder = narrow.matches
    ? "Describe the pipeline you want…"
    : "Describe the pipeline you want — e.g. import the users table into Iceberg, hourly";
};
narrow.addEventListener("change", syncPlaceholder);
syncPlaceholder();

renderPanel();
bootstrap();
