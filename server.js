'use strict';

// Poro Draft — 폴링(HTTP) 기반 실시간 팀장투표 & 스네이크 드래프트
//
// 설계 개요
//  - WebSocket을 쓰지 않고 짧은 폴링(short polling)으로 실시간을 구현한다.
//    이벤트가 초 단위로 드문드문 발생하는 "저빈도 턴제" 앱이라 1~1.5초 지연은 체감되지 않고,
//    매 요청이 독립적이라 무료 호스팅(프록시/cold start)에서도 연결이 끊길 일이 없다.
//  - 서버가 단일 진실(source of truth). 클라는 GET /api/state 로 상태를 가져가고,
//    POST /api/action 으로 행동한다.
//  - presence(접속 판정)는 "마지막 요청 시각(lastSeen)" 기준 + 유예(grace)로 판단한다.
//    → half-open 소켓 문제/오프라인 오탐/명단 깜빡임이 구조적으로 사라진다.
//  - rev(버전 카운터): 상태가 바뀔 때만 증가. 클라가 마지막 rev를 보내면 변화가 없을 때
//    가벼운 응답(unchanged)만 돌려줘 재렌더/대역폭을 아낀다.

const path = require('path');
const crypto = require('crypto');
const express = require('express');

const PORT = process.env.PORT || 3000;
const TARGET = 10; // 방을 시작하는 데 필요한 인원 (팀장 2 + 팀원 8)
const VOTE_SECONDS = Number(process.env.VOTE_SECONDS) || 30; // 팀장 투표 제한시간 (전원 투표해도 항상 이 시간 동안 진행)
const REVEAL_SECONDS = Number(process.env.REVEAL_SECONDS) || 3; // 팀장 공개 후 드래프트 진입까지
const LOBBY_OFFLINE_SECONDS = Number(process.env.LOBBY_OFFLINE_SECONDS) || 30; // 대기실에서 끊긴 사람 자동 퇴장까지 유예
const DRAFT_OFFLINE_SECONDS = Number(process.env.DRAFT_OFFLINE_SECONDS) || 12; // 드래프트에서 팀장이 오프라인이면 자동 픽까지 대기
// presence 유예: 마지막 요청 이후 이 시간 안이면 "접속 중"으로 본다.
// 클라가 약 1.2초마다 폴링하므로, 5~6번 연속 누락(≈8초)돼야 오프라인으로 판단 → 깜빡임 방지.
const PRESENCE_GRACE_MS = Number(process.env.PRESENCE_GRACE_MS) || 8000;

// 1 > 2 > 2 > 2 > 1 스네이크 드래프트. 값은 "이번 슬롯을 뽑는 팀장 index(0/1)".
// 블록 [1,2,2,2,1]을 팀장 0/1 번갈아 배정 → [0,1,1,0,0,1,1,0]
const PICK_ORDER = buildPickOrder([1, 2, 2, 2, 1]); // 길이 8

function buildPickOrder(blocks) {
  const order = [];
  blocks.forEach((size, i) => {
    const leader = i % 2;
    for (let k = 0; k < size; k++) order.push(leader);
  });
  return order;
}

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

/**
 * rooms: code -> {
 *   code, phase, hostId, players[], votes{voterId:targetId},
 *   leaders[id0,id1], turnIndex, picks[{slot, leader, playerId}],
 *   joinCounter, votingEndsAt, revealEndsAt, rev, voteTimer, revealTimer
 * }
 * player: { id, name, token, device, lastSeen, joinIndex, role, team }
 */
const rooms = new Map();
let lobbyRev = 1; // 방 목록(참여 화면) 버전

function now() { return Date.now(); }

function getRoom(code) {
  let room = rooms.get(code);
  if (!room) {
    room = {
      code,
      phase: 'lobby', // lobby | voting | reveal | draft | result
      hostId: null,
      players: [],
      votes: {},
      leaders: [],
      turnIndex: 0,
      picks: [],
      joinCounter: 0,
      votingEndsAt: null,
      revealEndsAt: null,
      rev: 1,
      voteTimer: null,
      revealTimer: null,
    };
    rooms.set(code, room);
  }
  return room;
}

// 상태 변경 시 호출: 방 rev + 방 목록 rev를 함께 올린다.
function bump(room) {
  if (room) room.rev++;
  lobbyRev++;
}

function isConnected(player) {
  return now() - (player.lastSeen || 0) < PRESENCE_GRACE_MS;
}

function currentPickerId(room) {
  if (room.phase !== 'draft') return null;
  const leaderSlot = PICK_ORDER[room.turnIndex];
  return room.leaders[leaderSlot] || null;
}

function voteCounts(room) {
  const counts = {};
  for (const p of room.players) counts[p.id] = 0;
  for (const target of Object.values(room.votes)) {
    if (counts[target] !== undefined) counts[target]++;
  }
  return counts;
}

// 브로드캐스트용 공개 상태 (token 제외)
function publicState(room) {
  const counts = voteCounts(room);
  return {
    rev: room.rev,
    code: room.code,
    phase: room.phase,
    hostId: room.hostId,
    target: TARGET,
    players: room.players.map((p) => ({
      id: p.id,
      name: p.name,
      connected: isConnected(p),
      device: p.device || 'PC',
      role: p.role || null,
      team: p.team ?? null,
      votes: counts[p.id] || 0,
    })),
    votes: room.votes,
    votedCount: Object.keys(room.votes).length,
    votingEndsAt: room.votingEndsAt || null,
    revealEndsAt: room.revealEndsAt || null,
    leaders: room.leaders,
    turnIndex: room.turnIndex,
    totalPicks: PICK_ORDER.length,
    currentPickerId: currentPickerId(room),
    picks: room.picks,
  };
}

// 참여 화면에 보여줄 방 목록 (접속자 0인 유령 방은 숨김)
function roomListPayload() {
  const list = [];
  for (const room of rooms.values()) {
    const connected = room.players.filter(isConnected).length;
    if (connected === 0) continue;
    list.push({
      code: room.code,
      count: room.players.length,
      connected,
      target: TARGET,
      phase: room.phase,
      joinable: room.phase === 'lobby' && room.players.length < TARGET,
    });
  }
  list.sort((a, b) => (b.joinable - a.joinable) || a.code.localeCompare(b.code));
  return list;
}

// ---- 게임 로직 -------------------------------------------------------------

function clearRoomTimers(room) {
  if (room.voteTimer) { clearTimeout(room.voteTimer); room.voteTimer = null; }
  if (room.revealTimer) { clearTimeout(room.revealTimer); room.revealTimer = null; }
}

function resetToLobby(room) {
  clearRoomTimers(room);
  room.phase = 'lobby';
  room.votes = {};
  room.leaders = [];
  room.turnIndex = 0;
  room.picks = [];
  room.votingEndsAt = null;
  room.revealEndsAt = null;
  for (const p of room.players) { p.role = null; p.team = null; }
}

function startVoting(room) {
  if (room.players.length !== TARGET) {
    return { error: `${TARGET}명이 모여야 시작할 수 있어요. (현재 ${room.players.length}명)` };
  }
  clearRoomTimers(room);
  room.phase = 'voting';
  room.votes = {};
  room.leaders = [];
  room.turnIndex = 0;
  room.picks = [];
  room.revealEndsAt = null;
  for (const p of room.players) { p.role = null; p.team = null; }
  room.votingEndsAt = now() + VOTE_SECONDS * 1000;
  scheduleVoteEnd(room);
  return {};
}

function scheduleVoteEnd(room) {
  if (room.voteTimer) clearTimeout(room.voteTimer);
  const delay = Math.max(0, (room.votingEndsAt || now()) - now());
  room.voteTimer = setTimeout(() => {
    room.voteTimer = null;
    if (room.phase !== 'voting') return;
    finishVoting(room);
  }, delay);
}

function finishVoting(room) {
  clearRoomTimers(room);
  const counts = voteCounts(room);
  // 득표 내림차순, 동점이면 먼저 들어온 순(joinIndex)으로 안정 정렬
  const ranked = [...room.players].sort((a, b) => {
    const d = (counts[b.id] || 0) - (counts[a.id] || 0);
    if (d !== 0) return d;
    return a.joinIndex - b.joinIndex;
  });
  const l0 = ranked[0];
  const l1 = ranked[1];
  room.leaders = [l0.id, l1.id];
  l0.role = 'leader'; l0.team = 0;
  l1.role = 'leader'; l1.team = 1;
  room.votingEndsAt = null;
  // 팀장 공개 단계 (뚝 끊기지 않게 잠깐 보여줌)
  room.phase = 'reveal';
  room.revealEndsAt = now() + REVEAL_SECONDS * 1000;
  room.revealTimer = setTimeout(() => {
    room.revealTimer = null;
    if (room.phase !== 'reveal') return;
    room.phase = 'draft';
    room.turnIndex = 0;
    room.revealEndsAt = null;
    bump(room);
    scheduleDraftAutopick(room);
  }, REVEAL_SECONDS * 1000);
  bump(room);
}

function castVote(room, voterId, targetId) {
  if (room.phase !== 'voting') return { error: '지금은 투표 단계가 아니에요.' };
  const voter = room.players.find((p) => p.id === voterId);
  const target = room.players.find((p) => p.id === targetId);
  if (!voter || !target) return { error: '없는 플레이어예요.' };
  if (voterId === targetId) return { error: '자기 자신에게는 투표할 수 없어요.' };

  room.votes[voterId] = targetId;
  // 전원이 투표해도 즉시 종료하지 않는다. 항상 제한시간(VOTE_SECONDS)이 지나야 확정 →
  // 마감 전까지 자유롭게 바꿔 찍을 여유를 보장한다.
  return {};
}

function doPick(room, pickerId, targetId) {
  if (room.phase !== 'draft') return { error: '지금은 드래프트 단계가 아니에요.' };
  const expected = currentPickerId(room);
  if (pickerId !== expected) return { error: '지금은 당신의 픽 차례가 아니에요.' };

  const target = room.players.find((p) => p.id === targetId);
  if (!target) return { error: '없는 플레이어예요.' };
  if (target.role) return { error: '이미 배정된 플레이어예요.' };

  const leaderSlot = PICK_ORDER[room.turnIndex];
  target.role = 'member';
  target.team = leaderSlot;
  room.picks.push({ slot: room.turnIndex, leader: leaderSlot, playerId: target.id });
  room.turnIndex++;
  if (room.turnIndex >= PICK_ORDER.length) room.phase = 'result';
  return {};
}

function assignHostIfNeeded(room) {
  const host = room.players.find((p) => p.id === room.hostId && isConnected(p));
  if (!host) {
    const next = room.players.find(isConnected);
    room.hostId = next ? next.id : (room.players[0] ? room.players[0].id : null);
  }
}

// 드래프트에서 현재 팀장이 오프라인이면 일정 시간 뒤 자동 랜덤 픽 (게임 멈춤 방지)
function scheduleDraftAutopick(room) {
  // 실제 실행은 presence tick에서 lastSeen 경과로 판단하므로 여기선 별도 타이머 불필요.
}

// ---- HTTP API --------------------------------------------------------------

// 방 목록 (참여 화면). rev가 같으면 변화 없음.
app.get('/api/lobby', (req, res) => {
  const clientRev = Number(req.query.rev) || 0;
  if (clientRev === lobbyRev) return res.json({ rev: lobbyRev, unchanged: true });
  res.json({ rev: lobbyRev, rooms: roomListPayload() });
});

// 방 참여 / 재접속
app.post('/api/join', (req, res) => {
  const code = String(req.body.room || '').trim().toLowerCase();
  const name = String(req.body.name || '').trim().slice(0, 20);
  const device = req.body.device === 'M' ? 'M' : 'PC';
  const token = req.body.token || null;
  if (!code) return res.json({ error: '방 코드를 입력해 주세요.' });

  const room = getRoom(code);
  let player = token ? room.players.find((p) => p.token === token) : null;

  if (player) {
    // 재접속: 기존 자리 복구
    player.lastSeen = now();
    player.device = device;
    if (name) player.name = name;
  } else {
    if (room.phase !== 'lobby') {
      return res.json({ error: '이미 진행 중인 방이에요. 시작 전 대기실에서만 참여할 수 있어요.' });
    }
    if (room.players.length >= TARGET) {
      return res.json({ error: `방이 가득 찼어요. (정원 ${TARGET}명)` });
    }
    if (!name) return res.json({ error: '이름을 입력해 주세요.' });
    player = {
      id: crypto.randomUUID(),
      name,
      token: crypto.randomUUID(),
      device,
      lastSeen: now(),
      joinIndex: room.joinCounter++,
      role: null,
      team: null,
    };
    room.players.push(player);
  }

  if (!room.hostId) room.hostId = player.id;
  bump(room);
  res.json({ ok: true, selfId: player.id, token: player.token, code });
});

// 상태 폴링. 이 요청 자체가 presence(lastSeen)를 갱신한다.
app.get('/api/state', (req, res) => {
  const code = String(req.query.room || '').trim().toLowerCase();
  const token = req.query.token || null;
  const clientRev = Number(req.query.rev) || 0;
  const room = rooms.get(code);
  if (!room) return res.json({ gone: true });

  const player = token ? room.players.find((p) => p.token === token) : null;
  if (!player) return res.json({ gone: true }); // 자리가 사라짐(대기실 유예 퇴장 등)
  player.lastSeen = now();

  if (clientRev === room.rev) return res.json({ rev: room.rev, unchanged: true });
  res.json({ rev: room.rev, room: publicState(room) });
});

// 행동: start / vote / pick / reset / leave
app.post('/api/action', (req, res) => {
  const code = String(req.body.room || '').trim().toLowerCase();
  const token = req.body.token || null;
  const type = req.body.type;
  const room = rooms.get(code);
  if (!room) return res.json({ error: '방을 찾을 수 없어요.' });
  const player = token ? room.players.find((p) => p.token === token) : null;
  if (!player) return res.json({ error: '먼저 방에 참여해 주세요.' });
  player.lastSeen = now();
  const selfId = player.id;
  const isHost = room.hostId === selfId;

  let result = {};
  switch (type) {
    case 'start':
      if (!isHost) return res.json({ error: '방장만 시작할 수 있어요.' });
      result = startVoting(room);
      break;
    case 'vote':
      result = castVote(room, selfId, req.body.targetId);
      break;
    case 'pick':
      result = doPick(room, selfId, req.body.targetId);
      break;
    case 'reset':
      if (!isHost) return res.json({ error: '방장만 초기화할 수 있어요.' });
      resetToLobby(room);
      break;
    case 'leave':
      if (room.phase === 'lobby') {
        room.players = room.players.filter((p) => p.id !== selfId);
        assignHostIfNeeded(room);
        if (room.players.length === 0) { clearRoomTimers(room); rooms.delete(room.code); }
      }
      break;
    default:
      return res.json({ error: '알 수 없는 요청이에요.' });
  }

  if (result.error) return res.json({ error: result.error });
  bump(room);
  res.json({ ok: true });
});

// ---- presence tick: 오프라인 전환/대기실 퇴장/드래프트 자동픽 처리 ----------
// 2초마다 각 방을 점검한다. 폴링이 끊긴(=화면을 떠난) 사람을 유예 후 정리하고,
// 그 변화를 rev에 반영해 다른 사람들 화면이 따라오게 한다.
setInterval(() => {
  const t = now();
  for (const room of [...rooms.values()]) {
    let changed = false;

    // 버려진 방 정리: 아무도 오랫동안 폴링하지 않으면(전원 이탈) 방 삭제 (메모리 누수 방지)
    const maxSeen = room.players.reduce((m, p) => Math.max(m, p.lastSeen || 0), 0);
    if (room.players.length === 0 || t - maxSeen > 90000) {
      clearRoomTimers(room);
      rooms.delete(room.code);
      lobbyRev++;
      continue;
    }

    // presence 상태가 직전 tick과 달라졌으면 rev를 올려 전파
    for (const p of room.players) {
      const c = isConnected(p);
      if (p._wasConnected !== c) { p._wasConnected = c; changed = true; }
    }

    // 방장 승계
    const hostOk = room.players.some((p) => p.id === room.hostId && isConnected(p));
    if (!hostOk) { assignHostIfNeeded(room); changed = true; }

    // 대기실: 오래 끊긴 사람 자동 퇴장
    if (room.phase === 'lobby') {
      const before = room.players.length;
      room.players = room.players.filter((p) => t - (p.lastSeen || 0) <= LOBBY_OFFLINE_SECONDS * 1000);
      if (room.players.length !== before) {
        assignHostIfNeeded(room);
        changed = true;
      }
      if (room.players.length === 0) {
        clearRoomTimers(room);
        rooms.delete(room.code);
        lobbyRev++;
        continue;
      }
    }

    // 드래프트: 현재 팀장이 오래 오프라인이면 자동 픽 (다음 픽까지 이어서)
    if (room.phase === 'draft') {
      let guard = 0;
      while (room.phase === 'draft' && guard++ < PICK_ORDER.length + 1) {
        const pid = currentPickerId(room);
        const picker = room.players.find((p) => p.id === pid);
        if (!picker) break;
        const offlineFor = t - (picker.lastSeen || 0);
        if (isConnected(picker) || offlineFor <= DRAFT_OFFLINE_SECONDS * 1000) break;
        const avail = room.players.filter((p) => !p.role);
        if (!avail.length) break;
        const target = avail[Math.floor(Math.random() * avail.length)];
        doPick(room, pid, target.id);
        changed = true;
      }
    }

    if (changed) bump(room);
  }
}, 2000);

app.listen(PORT, () => {
  console.log(`Poro Draft 서버 실행 중: http://localhost:${PORT}`);
});
