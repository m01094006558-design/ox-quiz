// 실시간 OX 퀴즈 서버 (Node.js + ws)
const http = require("http");
const fs = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const HOST_PASSWORD = process.env.HOST_PASSWORD || "";
const MAX_PLAYERS = 500;

if (!HOST_PASSWORD) {
  console.warn("[경고] HOST_PASSWORD 환경변수가 없어서 사회자 로그인이 비활성화됩니다.");
}

const indexHtml = fs.readFileSync(path.join(__dirname, "index.html"));

// ---------- 게임 상태 (메모리에만 저장됩니다) ----------
const game = { round: 0, question: "", pts: 1, phase: "idle", correct: null };
const players = new Map(); // pid -> { sid, name, score, pick }
let nextSid = 1;
const clients = new Set();

// ---------- HTTP ----------
const server = http.createServer((req, res) => {
  const url = (req.url || "/").split("?")[0].replace(/\/+$/, "") || "/";
  if (url === "/healthz") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    return res.end("ok");
  }
  if (url === "/" || url === "/host") {
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-cache" });
    return res.end(indexHtml);
  }
  res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
  res.end("페이지를 찾을 수 없어요.");
});

// ---------- WebSocket ----------
const wss = new WebSocketServer({ server, path: "/ws", maxPayload: 4096 });

function counts() {
  let o = 0, x = 0;
  for (const p of players.values()) {
    if (p.pick === "O") o++;
    else if (p.pick === "X") x++;
  }
  return { o, x };
}

function viewFor(ws) {
  const revealed = game.phase === "revealed";
  const list = [];
  for (const p of players.values()) {
    list.push({
      sid: p.sid,
      name: p.name,
      score: p.score,
      done: p.pick != null,
      pick: revealed ? p.pick : null,
    });
  }
  const mine = ws.pid ? players.get(ws.pid) : null;
  return {
    t: "state",
    hostEnabled: !!HOST_PASSWORD,
    host: !!ws.isHost,
    game,
    players: list,
    me: mine ? { sid: mine.sid, name: mine.name, pick: mine.pick } : null,
    counts: revealed || ws.isHost ? counts() : null,
  };
}

function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}

let timer = null;
function broadcast() {
  if (timer) return;
  timer = setTimeout(() => {
    timer = null;
    for (const ws of clients) send(ws, viewFor(ws));
  }, 80);
}

const clean = (s, n) => String(s || "").replace(/\s+/g, " ").trim().slice(0, n);

wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.isHost = false;
  ws.pid = null;
  clients.add(ws);
  send(ws, viewFor(ws));

  ws.on("pong", () => { ws.isAlive = true; });
  ws.on("close", () => clients.delete(ws));
  ws.on("error", () => {});

  ws.on("message", async (raw) => {
    let m;
    try { m = JSON.parse(raw.toString()); } catch { return; }
    if (!m || typeof m.t !== "string") return;

    // ----- 참가자 -----
    if (m.t === "join") {
      const pid = clean(m.pid, 64);
      const name = clean(m.name, 16);
      if (pid.length < 8 || !name) return;
      let p = players.get(pid);
      if (!p) {
        if (players.size >= MAX_PLAYERS) return;
        p = { sid: nextSid++, name, score: 0, pick: null };
        players.set(pid, p);
      } else {
        p.name = name;
      }
      ws.pid = pid;
      return broadcast();
    }

    if (m.t === "pick") {
      const p = ws.pid && players.get(ws.pid);
      if (!p || game.phase !== "open") return;
      if (m.c !== "O" && m.c !== "X") return;
      p.pick = m.c;
      return broadcast();
    }

    // ----- 사회자 -----
    if (m.t === "auth") {
      const ok = !!HOST_PASSWORD && typeof m.pw === "string" && m.pw === HOST_PASSWORD;
      if (!ok) {
        await new Promise((r) => setTimeout(r, 800)); // 무작위 대입 방지용 지연
        return send(ws, { t: "authFail" });
      }
      ws.isHost = true;
      return send(ws, viewFor(ws));
    }

    if (!ws.isHost) return;

    if (m.t === "ask") {
      const q = clean(m.question, 200);
      const pts = Math.min(100, Math.max(1, parseInt(m.pts, 10) || 1));
      if (!q) return;
      game.round += 1;
      game.question = q;
      game.pts = pts;
      game.phase = "open";
      game.correct = null;
      for (const p of players.values()) p.pick = null;
      return broadcast();
    }

    if (m.t === "reveal") {
      if (game.phase !== "open" || (m.c !== "O" && m.c !== "X")) return;
      for (const p of players.values()) {
        if (p.pick === m.c) p.score += game.pts;
      }
      game.phase = "revealed";
      game.correct = m.c;
      return broadcast();
    }

    if (m.t === "reset") {
      players.clear();
      game.round = 0;
      game.question = "";
      game.pts = 1;
      game.phase = "idle";
      game.correct = null;
      return broadcast();
    }
  });
});

// 끊어진 연결 정리 + 프록시가 연결을 끊지 않도록 주기적으로 ping
setInterval(() => {
  for (const ws of clients) {
    if (!ws.isAlive) { ws.terminate(); clients.delete(ws); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  }
}, 25000);

server.listen(PORT, () => console.log("OX 퀴즈 서버 시작: 포트 " + PORT));
