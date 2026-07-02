# 🦔 Poro Draft

10인 실시간 **팀장 투표 + 스네이크 드래프트** 웹앱.

## 흐름
1. **대기실** — 같은 *방 코드*로 10명이 모이면 방장이 시작
2. **투표** — 각자 팀장 후보에게 1표(자신 제외). 전원 투표 시 **최다 득표 2명**이 팀장으로 자동 확정
3. **드래프트** — 두 팀장이 `1 > 2 > 2 > 2 > 1` 순서로 팀원 8명을 번갈아 픽 (각 팀 4명씩 → 팀장 포함 **5:5**)
4. **결과** — 완성된 두 팀 표시. 방장이 "다시 하기"로 대기실 복귀 가능

모든 상태는 WebSocket으로 실시간 동기화되고, 새로고침해도 자리(재접속)가 유지됩니다.

## 로컬 실행
```bash
npm install
npm start
```
→ 브라우저에서 `http://localhost:3000` 접속.
같은 와이파이라면 다른 사람은 `http://<내PC_IP>:3000` 으로 접속하면 됩니다.

포트 변경: `PORT=4000 npm start`

## 인터넷 배포 (아무 곳에서나 접속)
WebSocket을 지원하는 Node 호스팅이면 됩니다. 가장 쉬운 예 — **Render.com** (무료):

1. 이 폴더를 GitHub 저장소로 push
2. Render → **New → Web Service** → 저장소 선택
3. 설정
   - Build Command: `npm install`
   - Start Command: `npm start`
4. 배포 후 나오는 `https://...onrender.com` 주소를 친구들에게 공유

> Render는 `PORT` 환경변수를 자동 주입하며, 코드가 `process.env.PORT`를 사용하므로 별도 설정이 필요 없습니다. WebSocket(wss)도 자동 지원됩니다. Railway / Fly.io도 동일한 방식으로 동작합니다.

## 파일 구조
- `server.js` — Express 정적 서빙 + WebSocket 게임 서버 (방/투표/드래프트 상태 관리)
- `public/index.html` — 클라이언트 UI (단일 파일, 순수 JS)
- `package.json` — 의존성(express, ws) 및 start 스크립트

## 참고 (기본값 조정)
- 인원수: `server.js`의 `TARGET` (기본 10)
- 픽 순서: `server.js`의 `buildPickOrder([1, 2, 2, 2, 1])` 블록 배열
- `public/index.html`의 `PICK_ORDER` 표시용 배열도 서버와 동일하게 맞춰야 합니다.
