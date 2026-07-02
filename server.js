'use strict';

const path = require('path');
const crypto = require('crypto');
const express = require('express');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const TARGET = 10; // 방을 시작하는 데 필요한 인원 (팀장 2 + 팀원 8)

// 1 > 2 > 2 > 2 > 1 스네이크 드래프트.
// 값은 "이번 슬롯을 뽑는 팀장 index(0 또는 1)".
// 블록 [1,2,2,2,1]을 팀장 0/1 번갈아 배정 → [0,1,1,0,0,1,1,0]
const PICK_ORDER = buildPickOrder([1, 2, 2, 2, 1]); // 길이 8

function buildPickOrder(blocks) {
  const order = [];
  blocks.forEach((size, i) => {
    const leader = i % 2; // 0, 1, 0, 1, 0 ...
    for (let k = 0; k < size; k++) order.push(leader);
  });
  return order;
}

const app = express();
app.use(express.static(path.join(__dirname, 'public')));

const server = app.listen(PORT, () => {
  console.log(`Poro Draft 서버 실행 중: http://localhost:${PORT}`);
});

const wss = new WebSocketServer({ server });

/**
 * rooms: code -> {
 *   code, phase, hostId, players[], votes{voterId:targetId},
 *   leaders[id0,id1], turnIndex, picks[{slot, leader, playerId}]
 * }
 * player: { id, name, token, ws, connected, joinIndex, role, team }
 */
const rooms = new Map();

function getRoom(code) {
  let room = rooms.get(code);
  if (!room) {
    room = {
      code,
      phase: 'lobby', // lobby | voting | draft | result
      hostId: null,
      players: [],
      votes: {},
      leaders: [],
      turnIndex: 0,
      picks: [],
      joinCounter: 0,
    };
    rooms.set(code, room);
  }
  return room;
}

function send(ws, obj) {
  if (ws && ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
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

// 브로드캐스트용 공개 상태 (token/ws 제외)
function publicState(room) {
  const counts = voteCounts(room);
  return {
    code: room.code,
    phase: room.phase,
    hostId: room.hostId,
    target: TARGET,
    players: room.players.map((p) => ({
      id: p.id,
      name: p.name,
      connected: p.connected,
      role: p.role || null, // 'leader' | 'member' | null
      team: p.team ?? null, // 0 | 1 | null
      votes: counts[p.id] || 0,
    })),
    votes: room.votes, // {voterId: targetId} — 누가 냈는지(투표 완료 표시용)
    votedCount: Object.keys(room.votes).length,
    leaders: room.leaders,
    turnIndex: room.turnIndex,
    totalPicks: PICK_ORDER.length,
    currentPickerId: currentPickerId(room),
    picks: room.picks,
  };
}

function broadcast(room) {
  const msg = { type: 'state', room: publicState(room) };
  for (const p of room.players) send(p.ws, msg);
}

function assignHostIfNeeded(room) {
  const host = room.players.find((p) => p.id === room.hostId && p.connected);
  if (!host) {
    const next = room.players.find((p) => p.connected);
    room.hostId = next ? next.id : null;
  }
}

function resetToLobby(room) {
  room.phase = 'lobby';
  room.votes = {};
  room.leaders = [];
  room.turnIndex = 0;
  room.picks = [];
  for (const p of room.players) {
    p.role = null;
    p.team = null;
  }
}

function startVoting(room) {
  if (room.players.length !== TARGET) return { error: `${TARGET}명이 모여야 시작할 수 있어요. (현재 ${room.players.length}명)` };
  room.phase = 'voting';
  room.votes = {};
  room.leaders = [];
  room.turnIndex = 0;
  room.picks = [];
  for (const p of room.players) {
    p.role = null;
    p.team = null;
  }
  return {};
}

function finishVoting(room) {
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
  l0.role = 'leader';
  l0.team = 0;
  l1.role = 'leader';
  l1.team = 1;
  room.phase = 'draft';
  room.turnIndex = 0;
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

  if (room.turnIndex >= PICK_ORDER.length) {
    room.phase = 'result';
  }
  return {};
}

function castVote(room, voterId, targetId) {
  if (room.phase !== 'voting') return { error: '지금은 투표 단계가 아니에요.' };
  const voter = room.players.find((p) => p.id === voterId);
  const target = room.players.find((p) => p.id === targetId);
  if (!voter || !target) return { error: '없는 플레이어예요.' };
  if (voterId === targetId) return { error: '자기 자신에게는 투표할 수 없어요.' };

  room.votes[voterId] = targetId;

  // 전원 투표 완료 시 자동으로 팀장 확정 & 드래프트 진입
  if (Object.keys(room.votes).length >= room.players.length) {
    finishVoting(room);
  }
  return {};
}

wss.on('connection', (ws) => {
  ws.roomCode = null;
  ws.playerId = null;

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }
    handleMessage(ws, msg);
  });

  ws.on('close', () => {
    const room = ws.roomCode ? rooms.get(ws.roomCode) : null;
    if (!room) return;
    const player = room.players.find((p) => p.id === ws.playerId);
    if (player) {
      player.connected = false;
      player.ws = null;
    }
    assignHostIfNeeded(room);
    broadcast(room);
  });
});

function handleMessage(ws, msg) {
  if (!msg || typeof msg.type !== 'string') return;

  if (msg.type === 'join') {
    const code = String(msg.room || '').trim().toLowerCase();
    const name = String(msg.name || '').trim().slice(0, 20);
    if (!code) return send(ws, { type: 'error', message: '방 코드를 입력해 주세요.' });

    const room = getRoom(code);

    // 재접속: token 일치하는 기존 플레이어에 다시 연결
    let player = msg.token ? room.players.find((p) => p.token === msg.token) : null;

    if (player) {
      player.connected = true;
      player.ws = ws;
      if (name) player.name = name;
    } else {
      if (room.phase !== 'lobby') {
        return send(ws, { type: 'error', message: '이미 진행 중인 방이에요. 시작 전 대기실에서만 참여할 수 있어요.' });
      }
      if (room.players.length >= TARGET) {
        return send(ws, { type: 'error', message: `방이 가득 찼어요. (정원 ${TARGET}명)` });
      }
      if (!name) return send(ws, { type: 'error', message: '이름을 입력해 주세요.' });
      player = {
        id: crypto.randomUUID(),
        name,
        token: crypto.randomUUID(),
        ws,
        connected: true,
        joinIndex: room.joinCounter++,
        role: null,
        team: null,
      };
      room.players.push(player);
    }

    if (!room.hostId) room.hostId = player.id;

    ws.roomCode = code;
    ws.playerId = player.id;

    send(ws, { type: 'joined', selfId: player.id, token: player.token, code });
    broadcast(room);
    return;
  }

  const room = ws.roomCode ? rooms.get(ws.roomCode) : null;
  if (!room) return send(ws, { type: 'error', message: '먼저 방에 참여해 주세요.' });
  const selfId = ws.playerId;
  const isHost = room.hostId === selfId;

  let result = {};
  switch (msg.type) {
    case 'start':
      if (!isHost) return send(ws, { type: 'error', message: '방장만 시작할 수 있어요.' });
      result = startVoting(room);
      break;
    case 'vote':
      result = castVote(room, selfId, msg.targetId);
      break;
    case 'pick':
      result = doPick(room, selfId, msg.targetId);
      break;
    case 'reset':
      if (!isHost) return send(ws, { type: 'error', message: '방장만 초기화할 수 있어요.' });
      resetToLobby(room);
      break;
    case 'leave':
      // 대기실에서만 완전히 나가기 허용
      if (room.phase === 'lobby') {
        room.players = room.players.filter((p) => p.id !== selfId);
        assignHostIfNeeded(room);
      }
      break;
    default:
      return;
  }

  if (result.error) return send(ws, { type: 'error', message: result.error });
  broadcast(room);
}
