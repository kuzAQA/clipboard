import http from 'node:http';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { WebSocket, WebSocketServer } from 'ws';

export const MAX_EVENT = 1024 * 1024;
const ROOM_TTL = 30 * 60 * 1000;
export function createRelayServer(frontendOrigin = process.env.FRONTEND_ORIGIN) {
  const rooms = new Map();
  const serve = (request, response) => {
    if (request.url !== '/health' || request.method !== 'GET') {
      response.writeHead(404).end();
      return;
    }
    response.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-store' });
    response.end('ok');
  };
  const server = http.createServer(serve);
  const wss = new WebSocketServer({ noServer: true, maxPayload: MAX_EVENT, perMessageDeflate: false });

  function send(client, payload) {
    if (client?.readyState === WebSocket.OPEN) client.send(JSON.stringify(payload));
  }

  function presence(room) {
    for (const client of room.members.values()) {
      if (client?.readyState === WebSocket.OPEN) {
        const peers = [...room.members.values()].filter((other) => other && other !== client && other.readyState === WebSocket.OPEN).length;
        send(client, { type: 'presence', peers });
      }
    }
  }

  function detach(client, leave = false) {
    const room = client.room;
    if (!room || room.members.get(client.device) !== client) return;
    room.members.delete(client.device);
    room.lastEvent.delete(client.device);
    if (room.members.size === 0) {
      if (leave) rooms.delete(client.roomId);
      else room.emptySince = Date.now();
    } else presence(room);
    client.room = null;
  }

  wss.on('connection', (client) => {
    client.isAlive = true;
    client.on('pong', () => { client.isAlive = true; });
    client.on('error', () => {});
    client.on('close', () => detach(client));
    client.on('message', (raw, binary) => {
      if (binary) return client.close(1003);
      let data;
      try { data = JSON.parse(raw.toString()); }
      catch { return client.close(1003); }
      if (data?.type === 'join' && !client.room) {
        if (!/^[0-9a-f]{64}$/.test(data.room) || !/^[0-9a-f]{32}$/.test(data.device)) return client.close(1008);
        let room = rooms.get(data.room);
        if (!room) {
          room = { members: new Map(), values: [null, null, null], seq: 0, lastEvent: new Map(), emptySince: null };
          rooms.set(data.room, room);
        }
        if (!room.members.has(data.device) && room.members.size === 2) {
          send(client, { type: 'error', code: 'full' });
          return client.close(4003);
        }
        const previous = room.members.get(data.device);
        room.members.set(data.device, client);
        room.emptySince = null;
        client.room = room;
        client.roomId = data.room;
        client.device = data.device;
        client.windowStart = Date.now();
        client.updates = 0;
        if (previous && previous !== client) previous.close(4000);
        send(client, { type: 'snapshot', slots: room.values });
        presence(room);
        return;
      }
      if (!client.room) return client.close(1008);
      if (data?.type === 'leave') {
        detach(client, true);
        return client.close(1000);
      }
      if (data?.type !== 'update' || !Number.isInteger(data.slot) || data.slot < 0 || data.slot > 2 ||
          !/^[0-9a-f]{32}$/.test(data.event) || !/^[A-Za-z0-9_-]{16}$/.test(data.iv) ||
          typeof data.data !== 'string' || !/^[A-Za-z0-9_-]{22,}$/.test(data.data)) return client.close(1008);
      const now = Date.now();
      if (now - client.windowStart >= 1000) { client.windowStart = now; client.updates = 0; }
      if (++client.updates > 20) return send(client, { type: 'error', code: 'rate' });
      const room = client.room;
      if (room.lastEvent.get(client.device) === data.event) return;
      room.lastEvent.set(client.device, data.event);
      const update = { type: 'update', slot: data.slot, event: data.event, iv: data.iv, data: data.data, seq: ++room.seq };
      room.values[data.slot] = update;
      for (const member of room.members.values()) send(member, update);
    });
  });

  server.on('upgrade', (request, socket, head) => {
    let allowed = false;
    try {
      const origin = request.headers.origin;
      allowed = request.url === '/relay' && origin === (frontendOrigin || `http://${request.headers.host}`);
    } catch { /* missing or invalid Origin */ }
    if (!allowed) return socket.destroy();
    wss.handleUpgrade(request, socket, head, (client) => wss.emit('connection', client, request));
  });

  const sweep = setInterval(() => {
    const now = Date.now();
    for (const [id, room] of rooms) if (room.emptySince && now - room.emptySince > ROOM_TTL) rooms.delete(id);
    for (const client of wss.clients) {
      if (!client.isAlive) client.terminate();
      else { client.isAlive = false; client.ping(); }
    }
  }, 5000);
  sweep.unref();
  server.on('close', () => clearInterval(sweep));
  return { server, rooms, wss };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const host = process.env.HOST || '127.0.0.1';
  const port = Number(process.env.PORT || 18787);
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error('Set a valid PORT.');
  createRelayServer().server.listen(port, host, () => {
    process.stdout.write(`Relay listening on http://${host}:${port}\n`);
  });
}
