import test from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes, webcrypto } from 'node:crypto';
import { once } from 'node:events';
import { WebSocket } from 'ws';
import { createRelayServer, MAX_EVENT } from './relay.mjs';
const FRONTEND_ORIGIN = 'https://kuzaqa.github.io';

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const b64 = (bytes) => Buffer.from(bytes).toString('base64url');

async function keys(token) {
  const material = await webcrypto.subtle.importKey('raw', token, 'HKDF', false, ['deriveBits', 'deriveKey']);
  const salt = encoder.encode('headers shared text v1');
  const room = Buffer.from(await webcrypto.subtle.deriveBits({ name: 'HKDF', hash: 'SHA-256', salt, info: encoder.encode('room') }, material, 256)).toString('hex');
  const key = await webcrypto.subtle.deriveKey({ name: 'HKDF', hash: 'SHA-256', salt, info: encoder.encode('content') }, material, { name: 'AES-GCM', length: 256 }, false, ['encrypt', 'decrypt']);
  return { room, key };
}

async function encrypted(room, key, slot, text) {
  const event = randomBytes(16).toString('hex');
  const iv = randomBytes(12);
  const data = await webcrypto.subtle.encrypt({ name: 'AES-GCM', iv, additionalData: encoder.encode(`${room}:${slot}:${event}`) }, key, encoder.encode(JSON.stringify({ text })));
  return { type: 'update', slot, event, iv: b64(iv), data: b64(data) };
}

async function plaintext(room, key, update) {
  const data = await webcrypto.subtle.decrypt({ name: 'AES-GCM', iv: Buffer.from(update.iv, 'base64url'), additionalData: encoder.encode(`${room}:${update.slot}:${update.event}`) }, key, Buffer.from(update.data, 'base64url'));
  return JSON.parse(decoder.decode(data)).text;
}

async function client(base, origin = FRONTEND_ORIGIN) {
  const ws = new WebSocket(base.replace('http:', 'ws:') + '/relay', { headers: { Origin: origin } });
  const inbox = [];
  const waiting = [];
  ws.on('message', (raw) => {
    const value = JSON.parse(raw.toString());
    const index = waiting.findIndex((entry) => entry.type === value.type);
    if (index >= 0) waiting.splice(index, 1)[0].resolve(value);
    else inbox.push(value);
  });
  await once(ws, 'open');
  return {
    ws,
    next(type) {
      const index = inbox.findIndex((item) => item.type === type);
      if (index >= 0) return Promise.resolve(inbox.splice(index, 1)[0]);
      return new Promise((resolve, reject) => {
        const entry = { type, resolve, reject };
        waiting.push(entry);
        setTimeout(() => {
          const index = waiting.indexOf(entry);
          if (index >= 0) { waiting.splice(index, 1); reject(new Error(`Timed out waiting for ${type}`)); }
        }, 2000).unref();
      });
    },
  };
}

test('pair, three slots, encrypted snapshots, limits and reconnect', async () => {
  const relay = createRelayServer(FRONTEND_ORIGIN);
  relay.server.listen(0, '127.0.0.1');
  await once(relay.server, 'listening');
  const base = `http://127.0.0.1:${relay.server.address().port}`;
  const { room, key } = await keys(randomBytes(32));
  const a = await client(base);
  const b = await client(base);
  const devices = [randomBytes(16).toString('hex'), randomBytes(16).toString('hex')];
  try {
    const health = await fetch(`${base}/health`);
    assert.equal(health.status, 200);
    assert.equal(await health.text(), 'ok');
    assert.equal((await fetch(`${base}/clipboard.html`)).status, 404);
    a.ws.send(JSON.stringify({ type: 'join', room, device: devices[0] }));
    assert.deepEqual((await a.next('snapshot')).slots, [null, null, null]);
    b.ws.send(JSON.stringify({ type: 'join', room, device: devices[1] }));
    await b.next('snapshot');
    assert.equal((await a.next('presence')).peers, 0);
    assert.equal((await a.next('presence')).peers, 1);
    assert.equal((await b.next('presence')).peers, 1);

    for (let slot = 0; slot < 3; slot++) {
      const source = slot % 2 ? b : a;
      const receiver = slot % 2 ? a : b;
      const original = `Слот ${slot}\n🙂  `;
      const packet = await encrypted(room, key, slot, original);
      assert.equal(JSON.stringify(packet).includes(original), false);
      source.ws.send(JSON.stringify(packet));
      const own = await source.next('update');
      const remote = await receiver.next('update');
      assert.equal(await plaintext(room, key, remote), original);
      assert.equal(own.seq, remote.seq);
      source.ws.send(JSON.stringify(packet));
      await new Promise((resolve) => setTimeout(resolve, 25));
      assert.equal(relay.rooms.get(room).seq, slot + 1, 'duplicate event ignored');
    }

    a.ws.send(JSON.stringify(await encrypted(room, key, 0, 'слева')));
    b.ws.send(JSON.stringify(await encrypted(room, key, 0, 'справа')));
    const concurrentA = [await a.next('update'), await a.next('update')];
    const concurrentB = [await b.next('update'), await b.next('update')];
    assert.deepEqual(concurrentA.map((update) => update.seq), concurrentB.map((update) => update.seq));
    const lastAccepted = await plaintext(room, key, concurrentA[1]);
    assert.equal(relay.rooms.get(room).values[0].seq, concurrentA[1].seq);

    const thirdDevice = randomBytes(16).toString('hex');
    const third = await client(base);
    third.ws.send(JSON.stringify({ type: 'join', room, device: thirdDevice }));
    assert.equal((await third.next('error')).code, 'full');
    await once(third.ws, 'close');

    b.ws.terminate();
    await once(b.ws, 'close');
    const recovered = await client(base);
    recovered.ws.send(JSON.stringify({ type: 'join', room, device: thirdDevice }));
    assert.equal(await plaintext(room, key, (await recovered.next('snapshot')).slots[0]), lastAccepted);
    recovered.ws.close();
    await once(recovered.ws, 'close');
    const rejoined = await client(base);
    try {
      rejoined.ws.send(JSON.stringify({ type: 'join', room, device: devices[1] }));
      const snapshot = await rejoined.next('snapshot');
      for (let slot = 0; slot < 3; slot++) assert.equal(await plaintext(room, key, snapshot.slots[slot]), slot === 0 ? lastAccepted : `Слот ${slot}\n🙂  `);
      const changed = await encrypted(room, key, 0, 'последнее значение');
      rejoined.ws.send(JSON.stringify(changed));
      assert.equal(await plaintext(room, key, await a.next('update')), 'последнее значение');
      await rejoined.next('update');
    } finally {
      const closed = once(rejoined.ws, 'close');
      rejoined.ws.close();
      await closed;
    }
    a.ws.close();
    await once(a.ws, 'close');
    const afterOutage = await client(base);
    afterOutage.ws.send(JSON.stringify({ type: 'join', room, device: randomBytes(16).toString('hex') }));
    assert.equal(await plaintext(room, key, (await afterOutage.next('snapshot')).slots[0]), 'последнее значение');
    afterOutage.ws.close();

    const tampered = await encrypted(room, key, 0, 'secret');
    const altered = Buffer.from(tampered.data, 'base64url');
    altered[0] ^= 1;
    tampered.data = b64(altered);
    await assert.rejects(plaintext(room, key, tampered));

    const oversized = await client(base);
    oversized.ws.send(JSON.stringify({ type: 'join', room: randomBytes(32).toString('hex'), device: randomBytes(16).toString('hex') }));
    await oversized.next('snapshot');
    oversized.ws.send('x'.repeat(MAX_EVENT + 1));
    const [code] = await once(oversized.ws, 'close');
    assert.equal(code, 1009);

    await assert.rejects(client(base, 'null'));
    await assert.rejects(client(base, 'https://other.example'));
  } finally {
    a.ws.close();
    b.ws.close();
    for (const ws of relay.wss.clients) ws.terminate();
    await new Promise((resolve) => relay.server.close(resolve));
  }
});

test('isolates sessions and rejects updates from a replaced connection', async () => {
  const relay = createRelayServer(FRONTEND_ORIGIN);
  relay.server.listen(0, '127.0.0.1');
  await once(relay.server, 'listening');
  const base = `http://127.0.0.1:${relay.server.address().port}`;
  const first = await keys(randomBytes(32));
  const second = await keys(randomBytes(32));
  const device = randomBytes(16).toString('hex');
  const old = await client(base);
  const current = await client(base);
  const isolated = await client(base);

  try {
    old.ws.send(JSON.stringify({ type: 'join', room: first.room, device }));
    await old.next('snapshot');
    const staleConnection = [...relay.wss.clients].find((member) => member.device === device);

    const oldClosed = once(old.ws, 'close');
    current.ws.send(JSON.stringify({ type: 'join', room: first.room, device }));
    await current.next('snapshot');
    await oldClosed;

    isolated.ws.send(JSON.stringify({ type: 'join', room: second.room, device: randomBytes(16).toString('hex') }));
    await isolated.next('snapshot');
    const update = await encrypted(second.room, second.key, 0, 'другая сессия');
    let leaked = false;
    const detectLeak = (raw) => {
      const message = JSON.parse(raw.toString());
      if (message.type === 'update' && message.event === update.event) leaked = true;
    };
    current.ws.on('message', detectLeak);
    isolated.ws.send(JSON.stringify(update));
    assert.equal(await plaintext(second.room, second.key, await isolated.next('update')), 'другая сессия');
    await new Promise((resolve) => setImmediate(resolve));
    current.ws.off('message', detectLeak);
    assert.equal(leaked, false);

    staleConnection.emit('message', Buffer.from(JSON.stringify(await encrypted(first.room, first.key, 0, 'устаревшее'))), false);
    assert.equal(relay.rooms.get(first.room).seq, 0);
    assert.equal(relay.rooms.get(second.room).seq, 1);
  } finally {
    for (const ws of relay.wss.clients) ws.terminate();
    await new Promise((resolve) => relay.server.close(resolve));
  }
});
