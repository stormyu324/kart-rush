// 卡丁狂飙 · 联网对战服务器
// 每个房间对应一个 Durable Object，负责大厅状态和实时转发比赛数据。
import { DurableObject } from 'cloudflare:workers';

const MAX_PLAYERS = 6;
const TRACK_COUNT = 12;
const CCS = [50, 100, 150];

export class RaceRoom extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.settings = { track: 0, cc: 100, ai: true };
    this.phase = 'lobby';
    this.hostId = null;
    this.seq = 0;
    ctx.blockConcurrencyWhile(async () => {
      const meta = await ctx.storage.get('meta');
      if (meta) Object.assign(this, meta);
    });
  }

  async save() {
    await this.ctx.storage.put('meta', { settings: this.settings, phase: this.phase, hostId: this.hostId, seq: this.seq });
  }

  // 已加入的玩家（排除正在断开的连接）
  players(exclude) {
    return this.ctx.getWebSockets()
      .filter(ws => ws !== exclude)
      .map(ws => ws.deserializeAttachment())
      .filter(a => a && a.id)
      .sort((a, b) => a.joined - b.joined);
  }

  async fetch(request) {
    if (request.headers.get('Upgrade') !== 'websocket') return new Response('expected websocket', { status: 426 });
    const [client, server] = Object.values(new WebSocketPair());
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({});
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, raw) {
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    const me = ws.deserializeAttachment() || {};

    switch (m.t) {
      case 'join': {
        if (me.id) return;
        const list = this.players(ws);
        if (list.length >= MAX_PLAYERS) return this.reject(ws, '房间已满（最多 6 人）');
        if (this.phase === 'racing' && list.length) return this.reject(ws, '比赛正在进行，请等这一场结束后再加入');
        if (!list.length) this.phase = 'lobby';
        const taken = new Set(list.map(p => p.driver));
        let driver = Number.isInteger(m.driver) && m.driver >= 0 && m.driver < 6 ? m.driver : 0;
        if (taken.has(driver)) driver = [0, 1, 2, 3, 4, 5].find(d => !taken.has(d));
        const att = { id: 'p' + (++this.seq), name: String(m.name || '').replace(/[<>&"'\u0000-\u001f]/g, '').trim().slice(0, 12) || '车手', driver, joined: Date.now() };
        ws.serializeAttachment(att);
        if (!this.hostId || !list.some(p => p.id === this.hostId)) this.hostId = att.id;
        await this.save();
        ws.send(JSON.stringify({ t: 'welcome', id: att.id }));
        this.lobby();
        break;
      }
      case 'driver': {
        if (!me.id || this.phase !== 'lobby') return;
        const d = m.driver;
        const taken = new Set(this.players().filter(p => p.id !== me.id).map(p => p.driver));
        if (Number.isInteger(d) && d >= 0 && d < 6 && !taken.has(d)) { me.driver = d; ws.serializeAttachment(me); }
        this.lobby();
        break;
      }
      case 'settings': {
        if (me.id !== this.hostId || this.phase !== 'lobby' || !m.settings) return;
        const s = m.settings;
        if (Number.isInteger(s.track) && s.track >= 0 && s.track < TRACK_COUNT) this.settings.track = s.track;
        if (CCS.includes(s.cc)) this.settings.cc = s.cc;
        if (typeof s.ai === 'boolean') this.settings.ai = s.ai;
        await this.save();
        this.lobby();
        break;
      }
      case 'start': {
        if (me.id !== this.hostId || this.phase !== 'lobby') return;
        this.phase = 'racing';
        await this.save();
        const ps = this.players();
        const used = new Set(ps.map(p => p.driver));
        const grid = [];
        if (this.settings.ai) for (let d = 0; d < 6 && grid.length + ps.length < MAX_PLAYERS; d++) if (!used.has(d)) grid.push({ nid: 'ai' + d, driver: d, name: null });
        for (const p of ps) grid.push({ nid: p.id, driver: p.driver, name: p.name });
        this.broadcast({ t: 'start', settings: this.settings, grid, host: this.hostId });
        break;
      }
      case 'back': {
        if (me.id !== this.hostId) return;
        this.phase = 'lobby';
        await this.save();
        this.broadcast({ t: 'back' });
        this.lobby();
        break;
      }
      // 比赛中的实时数据：原样转发给其他人
      case 's':
      case 'ev':
      case 'finish': {
        if (!me.id) return;
        m.id = me.id;
        this.broadcast(m, ws);
        break;
      }
    }
  }

  reject(ws, msg) {
    try { ws.send(JSON.stringify({ t: 'error', msg })); ws.close(4000, 'rejected'); } catch {}
  }

  async webSocketClose(ws) { await this.leave(ws); }
  async webSocketError(ws) { await this.leave(ws); }

  async leave(ws) {
    const me = ws.deserializeAttachment() || {};
    try { ws.close(1000, 'bye'); } catch {}
    if (!me.id) return;
    ws.serializeAttachment({});
    const rest = this.players(ws);
    if (this.hostId === me.id) this.hostId = rest.length ? rest[0].id : null;
    if (!rest.length) this.phase = 'lobby';
    await this.save();
    this.broadcast({ t: 'left', id: me.id, host: this.hostId }, ws);
    this.lobby(ws);
  }

  lobby(exclude) {
    this.broadcast({ t: 'lobby', players: this.players(exclude), host: this.hostId, settings: this.settings, phase: this.phase }, exclude);
  }

  broadcast(msg, exclude) {
    const s = JSON.stringify(msg);
    for (const ws of this.ctx.getWebSockets()) {
      if (ws === exclude) continue;
      const a = ws.deserializeAttachment();
      if (!a || !a.id) continue;
      try { ws.send(s); } catch {}
    }
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const m = url.pathname.match(/^\/kart\/ws\/([A-Za-z0-9]{4,8})$/);
    if (m) return env.ROOMS.get(env.ROOMS.idFromName(m[1].toUpperCase())).fetch(request);
    return new Response('Not found', { status: 404 });
  },
};
