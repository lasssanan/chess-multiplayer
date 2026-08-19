/**
 * Voice Room + 2-Player Chess Server
 * ==================================
 * Sirf 3 kaam karta hai:
 *   1. Room banao / join karo (6 char code)
 *   2. Mic voice chat ke liye WebRTC signaling relay
 *   3. 2 player chess ke moves dono taraf sync
 *
 * Run:  npm install ws  &&  node server.js
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const ROOM_CODE_LENGTH = 6;
const ROOM_CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const HEARTBEAT_INTERVAL = 30_000;
const MAX_PLAYERS = 8;          // voice room me itne log
const MAX_CHAT_HISTORY = 60;

/* ---------------------------- state ---------------------------- */
const rooms = new Map();          // code -> room
const socketToRoom = new Map();   // socketId -> code
const socketsById = new Map();    // socketId -> ws

/* ---------------------------- http ----------------------------- */
const MIME = {
    '.html': 'text/html; charset=utf-8',
    '.js': 'application/javascript',
    '.css': 'text/css',
    '.json': 'application/json',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.svg': 'image/svg+xml',
    '.ico': 'image/x-icon'
};

const httpServer = http.createServer((req, res) => {
    const clean = (req.url || '/').split('?')[0];
    let file = clean === '/' || clean === '/index.html' ? 'index.html' : clean.slice(1);

    if (['server.js', 'package.json', 'package-lock.json'].includes(file) || file.includes('..')) {
        res.writeHead(403, { 'Content-Type': 'text/plain' });
        return res.end('Forbidden');
    }

    const full = path.join(__dirname, file);
    fs.readFile(full, (err, data) => {
        if (err) {
            res.writeHead(err.code === 'ENOENT' ? 404 : 500);
            return res.end(err.code === 'ENOENT' ? 'Not found' : 'Server error');
        }
        res.writeHead(200, {
            'Content-Type': MIME[path.extname(full)] || 'application/octet-stream',
            'Cache-Control': 'no-store'
        });
        res.end(data);
    });
});

/* ------------------------- websocket --------------------------- */
const wss = new WebSocketServer({ server: httpServer });

wss.on('connection', (ws) => {
    ws.id = generateId();
    ws.isAlive = true;
    socketsById.set(ws.id, ws);

    send(ws, { type: 'welcome', id: ws.id });

    ws.on('pong', () => { ws.isAlive = true; });
    ws.on('message', (raw) => {
        try { route(ws, JSON.parse(raw.toString())); }
        catch (e) { send(ws, { type: 'error', message: 'Bad message' }); }
    });
    ws.on('close', () => handleDisconnect(ws));
    ws.on('error', () => handleDisconnect(ws));

    console.log(`[WS] connected ${ws.id}`);
});

const heartbeat = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (!ws.isAlive) { handleDisconnect(ws); return ws.terminate(); }
        ws.isAlive = false;
        ws.ping();
    });
}, HEARTBEAT_INTERVAL);
wss.on('close', () => clearInterval(heartbeat));

/* --------------------------- router ---------------------------- */
function route(ws, msg) {
    switch (msg.type) {
        /* room */
        case 'create_room': return createRoom(ws, msg);
        case 'join_room':   return joinRoom(ws, msg);
        case 'leave_room':  return leaveRoom(ws);
        case 'kick_player': return kickPlayer(ws, msg);
        case 'chat':        return chat(ws, msg);

        /* voice (WebRTC signaling — server sirf postman hai) */
        case 'rtc_offer':
        case 'rtc_answer':
        case 'rtc_ice':     return relayToPeer(ws, msg);
        case 'mic_state':   return micState(ws, msg);

        /* chess */
        case 'chess_seat':   return chessSeat(ws, msg);
        case 'chess_move':   return chessMove(ws, msg);
        case 'chess_reset':  return chessReset(ws);
        case 'chess_resign': return chessResign(ws);

        default: console.warn('[WS] unknown type', msg.type);
    }
}

/* ---------------------------- rooms ---------------------------- */
function createRoom(ws, msg) {
    if (socketToRoom.has(ws.id)) return send(ws, { type: 'error', message: 'Pehle se room me ho' });

    let code;
    do { code = generateRoomCode(); } while (rooms.has(code));

    const room = {
        code,
        host: ws.id,
        players: new Map(),
        chatHistory: [],
        chess: newChessState()
    };
    room.players.set(ws.id, { id: ws.id, name: cleanName(msg.playerName), micOn: false });
    rooms.set(code, room);
    socketToRoom.set(ws.id, code);

    send(ws, {
        type: 'room_joined',
        code,
        you: ws.id,
        host: true,
        players: playerList(room),
        chatHistory: room.chatHistory,
        chess: publicChess(room)
    });
    system(room, `${cleanName(msg.playerName)} ne room banaya`);
    console.log(`[ROOM] created ${code}`);
}

function joinRoom(ws, msg) {
    if (socketToRoom.has(ws.id)) return send(ws, { type: 'error', message: 'Pehle se room me ho' });

    const code = (msg.code || '').toUpperCase().trim();
    const room = rooms.get(code);
    if (!room) return send(ws, { type: 'error', message: 'Room nahi mila' });
    if (room.players.size >= MAX_PLAYERS) return send(ws, { type: 'error', message: 'Room full hai' });

    const name = cleanName(msg.playerName);
    room.players.set(ws.id, { id: ws.id, name, micOn: false });
    socketToRoom.set(ws.id, code);

    send(ws, {
        type: 'room_joined',
        code,
        you: ws.id,
        host: room.host === ws.id,
        players: playerList(room),
        chatHistory: room.chatHistory,
        chess: publicChess(room)
    });

    broadcast(code, {
        type: 'player_joined',
        player: { id: ws.id, name, micOn: false },
        players: playerList(room)
    }, ws);

    system(room, `${name} room me aaya`);
    console.log(`[ROOM] ${name} joined ${code}`);
}

function leaveRoom(ws) {
    const code = socketToRoom.get(ws.id);
    if (code) removePlayer(ws, code);
}

/* host kisi player ko room se nikal sakta hai */
function kickPlayer(ws, msg) {
    const room = roomOf(ws);
    if (!room) return;
    if (room.host !== ws.id) return send(ws, { type: 'error', message: 'Sirf host kick kar sakta hai' });

    const targetId = msg.playerId;
    if (!targetId || targetId === ws.id) return;
    const target = socketsById.get(targetId);
    if (!target || socketToRoom.get(targetId) !== room.code) return;

    const p = room.players.get(targetId);
    const name = p ? p.name : 'Player';

    send(target, { type: 'kicked', reason: 'Host ne aapko room se nikal diya' });
    removePlayer(target, room.code);
    system(room, `${name} ko host ne kick kiya`);
    console.log(`[ROOM] ${name} kicked from ${room.code}`);
}

function chat(ws, msg) {
    const room = roomOf(ws);
    if (!room) return;
    const p = room.players.get(ws.id);
    const m = {
        type: 'chat',
        playerId: ws.id,
        playerName: p ? p.name : '???',
        message: String(msg.message || '').substring(0, 400),
        timestamp: Date.now()
    };
    if (!m.message.trim()) return;
    pushHistory(room, m);
    broadcast(room.code, m);
}

/* ---------------------------- voice ---------------------------- */
function relayToPeer(ws, msg) {
    const room = roomOf(ws);
    if (!room) return;
    const target = socketsById.get(msg.to);
    if (!target || socketToRoom.get(msg.to) !== room.code) return;
    send(target, { ...msg, from: ws.id });
}

function micState(ws, msg) {
    const room = roomOf(ws);
    if (!room) return;
    const p = room.players.get(ws.id);
    if (p) p.micOn = !!msg.micOn;
    broadcast(room.code, { type: 'mic_state', playerId: ws.id, micOn: !!msg.micOn });
}

/* ---------------------------- chess ---------------------------- */
function newChessState() {
    return {
        white: null,          // socket id
        black: null,
        fenBoard: startBoard(),
        turn: 'w',
        moves: [],            // {from,to,promo,san}
        result: null,         // null | '1-0' | '0-1' | '1/2-1/2'
        reason: null
    };
}

function startBoard() {
    return [
        ['r','n','b','q','k','b','n','r'],
        ['p','p','p','p','p','p','p','p'],
        [null,null,null,null,null,null,null,null],
        [null,null,null,null,null,null,null,null],
        [null,null,null,null,null,null,null,null],
        [null,null,null,null,null,null,null,null],
        ['P','P','P','P','P','P','P','P'],
        ['R','N','B','Q','K','B','N','R']
    ];
}

function publicChess(room) {
    const c = room.chess;
    return {
        white: c.white,
        blackk: undefined,
        black: c.black,
        board: c.fenBoard,
        turn: c.turn,
        moves: c.moves,
        result: c.result,
        reason: c.reason,
        whiteName: nameOf(room, c.white),
        blackName: nameOf(room, c.black)
    };
}

function chessSeat(ws, msg) {
    const room = roomOf(ws);
    if (!room) return;
    const c = room.chess;
    const side = msg.side; // 'w' | 'b' | 'leave'

    if (side === 'leave') {
        if (c.white === ws.id) c.white = null;
        if (c.black === ws.id) c.black = null;
    } else if (side === 'w') {
        if (c.white && c.white !== ws.id) return send(ws, { type: 'error', message: 'White seat busy hai' });
        if (c.black === ws.id) c.black = null;
        c.white = ws.id;
    } else if (side === 'b') {
        if (c.black && c.black !== ws.id) return send(ws, { type: 'error', message: 'Black seat busy hai' });
        if (c.white === ws.id) c.white = null;
        c.black = ws.id;
    }
    broadcast(room.code, { type: 'chess_state', chess: publicChess(room) });
}

function chessMove(ws, msg) {
    const room = roomOf(ws);
    if (!room) return;
    const c = room.chess;
    if (c.result) return;

    const seat = c.white === ws.id ? 'w' : (c.black === ws.id ? 'b' : null);
    if (!seat) return send(ws, { type: 'error', message: 'Tum khiladi nahi ho — pehle seat lo' });
    if (seat !== c.turn) return send(ws, { type: 'error', message: 'Abhi tumhari baari nahi' });

    // Client validated move; server sirf board apply karta hai (trusted 2-player casual game)
    const { from, to, promo, board, turn, san, result, reason } = msg;
    if (!Array.isArray(board)) return;

    c.fenBoard = board;
    c.turn = turn === 'w' ? 'w' : 'b';
    c.moves.push({ from, to, promo: promo || null, san: san || '' });
    if (result) { c.result = result; c.reason = reason || null; }

    broadcast(room.code, {
        type: 'chess_move',
        by: seat,
        from, to, promo: promo || null, san: san || '',
        chess: publicChess(room)
    });

    if (c.result) system(room, `Game khatam: ${c.reason || c.result}`);
}

function chessReset(ws) {
    const room = roomOf(ws);
    if (!room) return;
    const c = room.chess;
    const keepW = c.white, keepB = c.black;
    room.chess = newChessState();
    room.chess.white = keepW;
    room.chess.black = keepB;
    broadcast(room.code, { type: 'chess_state', chess: publicChess(room) });
    system(room, 'Nayi baazi shuru!');
}

function chessResign(ws) {
    const room = roomOf(ws);
    if (!room) return;
    const c = room.chess;
    const seat = c.white === ws.id ? 'w' : (c.black === ws.id ? 'b' : null);
    if (!seat || c.result) return;
    c.result = seat === 'w' ? '0-1' : '1-0';
    c.reason = (seat === 'w' ? 'White' : 'Black') + ' ne resign kiya';
    broadcast(room.code, { type: 'chess_state', chess: publicChess(room) });
    system(room, `Game khatam: ${c.reason}`);
}

/* -------------------------- disconnect ------------------------- */
function handleDisconnect(ws) {
    const code = socketToRoom.get(ws.id);
    socketsById.delete(ws.id);
    if (code) removePlayer(ws, code);
}

function removePlayer(ws, code) {
    const room = rooms.get(code);
    if (!room) return;
    const p = room.players.get(ws.id);
    room.players.delete(ws.id);
    socketToRoom.delete(ws.id);

    const name = p ? p.name : '???';

    const c = room.chess;
    if (c.white === ws.id) c.white = null;
    if (c.black === ws.id) c.black = null;

    if (room.players.size === 0) {
        rooms.delete(code);
        console.log(`[ROOM] deleted ${code}`);
        return;
    }

    if (room.host === ws.id) {
        room.host = room.players.keys().next().value;
        broadcast(code, { type: 'host_changed', host: room.host });
    }

    broadcast(code, {
        type: 'player_left',
        playerId: ws.id,
        players: playerList(room),
        chess: publicChess(room)
    });
    system(room, `${name} chala gaya`);
}

/* --------------------------- helpers --------------------------- */
function system(room, text) {
    const m = {
        type: 'chat',
        playerId: '__system__',
        playerName: 'SYSTEM',
        message: text,
        timestamp: Date.now(),
        isSystem: true
    };
    pushHistory(room, m);
    broadcast(room.code, m);
}

function pushHistory(room, m) {
    room.chatHistory.push(m);
    if (room.chatHistory.length > MAX_CHAT_HISTORY) room.chatHistory.shift();
}

function roomOf(ws) {
    const code = socketToRoom.get(ws.id);
    return code ? rooms.get(code) : null;
}

function nameOf(room, id) {
    if (!id) return null;
    const p = room.players.get(id);
    return p ? p.name : null;
}

function playerList(room) {
    return Array.from(room.players.values()).map(p => ({
        id: p.id, name: p.name, micOn: p.micOn, isHost: p.id === room.host
    }));
}

function broadcast(code, msg, exclude = null) {
    const room = rooms.get(code);
    if (!room) return;
    const data = JSON.stringify(msg);
    room.players.forEach((_, id) => {
        const ws = socketsById.get(id);
        if (ws && ws !== exclude && ws.readyState === 1) ws.send(data);
    });
}

function send(ws, msg) {
    if (ws && ws.readyState === 1) ws.send(JSON.stringify(msg));
}

function cleanName(n) {
    const s = String(n || '').trim().substring(0, 16);
    return s || 'Player';
}

function generateRoomCode() {
    let c = '';
    for (let i = 0; i < ROOM_CODE_LENGTH; i++) c += ROOM_CODE_CHARS[Math.floor(Math.random() * ROOM_CODE_CHARS.length)];
    return c;
}

function generateId() {
    return Math.random().toString(36).slice(2, 10) + Date.now().toString(36);
}

httpServer.listen(PORT, '0.0.0.0', () => {
    console.log(`\n🎙️  Voice Room + Chess`);
    console.log(`    HTTP → http://localhost:${PORT}`);
    console.log(`    WS   → ws://localhost:${PORT}\n`);
});
