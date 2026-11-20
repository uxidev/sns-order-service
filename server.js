/**
 * SNS 주문 서비스 - 퓨쳐인프라 독립 실행형 (온투코웍스 없이 단독 동작)
 * Node 22+ / 외부 패키지 0 / 실행: node server.js
 */
import http from 'node:http';
import fsp from 'node:fs/promises';
import path from 'node:path';

const CFG = {
  port: Number(process.env.PORT || 8080),
  dataDir: process.env.DATA_DIR || './data',
  token: process.env.ADMIN_TOKEN || '',
  base: process.env.PROVIDER_BASE || 'https://justanotherpanel.com/api/v2',
  key: process.env.PROVIDER_KEY || '',
  pollMin: Math.max(1, Number(process.env.POLL_MIN || 5)),
  autoRefill: String(process.env.AUTO_REFILL || 'false') === 'true',
  dropThreshold: Number(process.env.DROP_THRESHOLD || 0.1),
  watchDays: Math.max(1, Number(process.env.WATCH_DAYS || 30)),
  maxRefills: Math.max(0, Number(process.env.MAX_REFILLS || 1)),
  notifyUrl: process.env.NOTIFY_WEBHOOK || '',
};
const DB_FILE = path.join(CFG.dataDir, 'db.json');
let db = { orders: [], services: [], servicesAt: null, events: [] };
const now = () => new Date().toISOString();

function log(event, extra = {}) {
  const row = { at: now(), event, ...extra };
  db.events.push(row);
  if (db.events.length > 2000) db.events = db.events.slice(-1500);
  console.log(JSON.stringify(row));
}

async function loadDb() {
  await fsp.mkdir(CFG.dataDir, { recursive: true });
  try {
    db = JSON.parse(await fsp.readFile(DB_FILE, 'utf8'));
    db.orders ||= []; db.services ||= []; db.events ||= [];
  } catch { await saveDb(); }
}

async function saveDb() {
  const tmp = DB_FILE + '.tmp';
  await fsp.writeFile(tmp, JSON.stringify(db, null, 2));
  await fsp.rename(tmp, DB_FILE);
}

async function notify(text) {
  if (!CFG.notifyUrl) return;
  try {
    await fetch(CFG.notifyUrl, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ text }),
    });
  } catch (e) { console.error('notify failed', e.message); }
}

async function provider(action, params = {}) {
  if (!CFG.key) throw new Error('PROVIDER_KEY 가 설정되지 않았습니다');
  const body = new URLSearchParams({ key: CFG.key, action, ...params });
  const res = await fetch(CFG.base, { method: 'POST', body });
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); }
  catch { throw new Error('공급사 응답 파싱 실패: ' + text.slice(0, 200)); }
  if (data && data.error) throw new Error('공급사 오류: ' + data.error);
  return data;
}

async function refreshServices() {
  const list = await provider('services');
  db.services = (Array.isArray(list) ? list : []).map((s) => ({
    service: String(s.service),
    name: s.name,
    type: s.type,
    category: s.category,
    rate: Number(s.rate),
    min: Number(s.min),
    max: Number(s.max),
    refill: Boolean(s.refill),
    cancel: Boolean(s.cancel),
  }));
  db.servicesAt = now();
  await saveDb();
  log('services_refreshed', { count: db.services.length });
  return db.services;
}

function findService(id) {
  return db.services.find((s) => s.service === String(id));
}

async function placeOrder(input) {
  const svc = findService(input.service);
  if (!svc) throw new Error('서비스 ' + input.service + ' 를 찾을 수 없습니다. /services 로 먼저 동기화하세요');
  const qty = Number(input.quantity);
  if (!Number.isFinite(qty) || qty < svc.min) throw new Error('최소 수량 ' + svc.min + ' 이상이어야 합니다');
  if (qty > svc.max) throw new Error('최대 수량 ' + svc.max + ' 이하이어야 합니다');
  if (!input.link) throw new Error('대상 링크(link)가 필요합니다');

  const order = {
    id: 'o_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6),
    service: svc.service,
    serviceName: svc.name,
    quantity: qty,
    link: input.link,
    isPrivate: Boolean(input.isPrivate),
    status: 'pending',
    providerOrderId: null,
    startCount: input.startCount ?? null,
    createdAt: now(),
    completedAt: null,
    refills: 0,
    snapshots: [],
    history: [{ at: now(), event: 'created' }],
  };
  db.orders.push(order);
  await saveDb();

  try {
    const res = await provider('add', {
      service: svc.service,
      link: input.link,
      quantity: String(qty),
    });
    order.providerOrderId = String(res.order);
    order.status = 'active';
    order.history.push({ at: now(), event: 'submitted', providerOrderId: order.providerOrderId });
    log('order_submitted', { id: order.id, providerOrderId: order.providerOrderId });
    await notify('발주 완료 · ' + svc.name + ' x' + qty + '\n' + input.link + '\n공급사 주문 ' + order.providerOrderId);
  } catch (e) {
    order.status = 'failed';
    order.error = e.message;
    order.history.push({ at: now(), event: 'failed', error: e.message });
    log('order_failed', { id: order.id, error: e.message });
    await notify('발주 실패 · ' + svc.name + '\n' + e.message);
  }
  await saveDb();
  return order;
}

async function syncOrder(order) {
  if (!order.providerOrderId) return order;
  if (!['active', 'inprogress', 'partial'].includes(order.status)) return order;
  try {
    const st = await provider('status', { order: order.providerOrderId });
    const map = {
      Pending: 'active', 'In progress': 'inprogress', Processing: 'inprogress',
      Completed: 'completed', Partial: 'partial', Canceled: 'canceled',
    };
    const next = map[st.status] || order.status;
    if (next !== order.status) {
      order.history.push({ at: now(), event: 'status', from: order.status, to: next });
      order.status = next;
      if (next === 'completed') {
        order.completedAt = now();
        order.startCount = st.start_count ?? order.startCount;
      }
      log('order_status', { id: order.id, status: next, remains: st.remains });
    }
    order.providerStatus = st.status;
    order.remains = st.remains ?? null;
  } catch (e) {
    log('sync_failed', { id: order.id, error: e.message });
  }
  return order;
}

function dropRate(order) {
  const s = order.snapshots || [];
  if (s.length < 2) return null;
  const peak = Math.max(...s.map((x) => x.count));
  const last = s[s.length - 1].count;
  if (!peak) return null;
  return { peak, last, dropRate: (peak - last) / peak, over: (peak - last) / peak >= CFG.dropThreshold };
}

async function takeSnapshot(order) {
  if (!order.startCount) return;
  const s = order.snapshots || (order.snapshots = []);
  s.push({ at: now(), count: order.startCount + (order.quantity || 0) });
  if (s.length > 800) order.snapshots = s.slice(-600);
}

async function fetchFollowerCount(link) {
  const m = String(link).match(/(?:youtube\.com\/(?:channel\/|@|c\/|user\/)|youtu\.be\/)([^\/\?]+)/i);
  if (m) {
    const handle = m[1].startsWith('UC') ? m[1] : '@' + m[1].replace(/^@/, '');
    const url = handle.startsWith('UC')
      ? 'https://www.youtube.com/channel/' + handle
      : 'https://www.youtube.com/' + handle;
    const res = await fetch(url, { headers: { 'user-agent': 'Mozilla/5.0 (compatible; SnsOrderBot/1.0)' } });
    const html = await res.text();
    const hit = html.match(/"subscriberCountText":\{"accessibility".*?"simpleText":"([^"]+)"/)
      || html.match(/([\d.,]+[천만KMB]?)\s*(?:명|subscribers)/i);
    if (hit) return parseCount(hit[1]);
  }
  const ig = String(link).match(/instagram\.com\/([^\/\?]+)/i);
  if (ig) {
    const res = await fetch('https://www.instagram.com/' + ig[1] + '/?__a=1&__d=dis', {
      headers: { 'user-agent': 'Mozilla/5.0 (compatible; SnsOrderBot/1.0)' },
    });
    if (res.ok) {
      const j = await res.json().catch(() => null);
      const n = j?.graphql?.user?.edge_followed_by?.count;
      if (n) return Number(n);
    }
  }
  return null;
}

function parseCount(raw) {
  const s = String(raw).replace(/[,\s]/g, '').toLowerCase();
  const num = parseFloat(s);
  if (!Number.isFinite(num)) return null;
  if (s.includes('만')) return Math.round(num * 10000);
  if (s.includes('천')) return Math.round(num * 1000);
  if (s.includes('억')) return Math.round(num * 100000000);
  if (s.endsWith('m')) return Math.round(num * 1000000);
  if (s.endsWith('k')) return Math.round(num * 1000);
  return Math.round(num);
}

async function watchDrop(order) {
  if (order.isPrivate) return;
  if (!order.completedAt) return;
  const age = (Date.now() - new Date(order.completedAt).getTime()) / 86400000;
  if (age > CFG.watchDays) return;
  const count = await fetchFollowerCount(order.link);
  if (count == null) { log('snapshot_skip', { id: order.id, reason: 'count_unavailable' }); return; }
  order.snapshots.push({ at: now(), count });
  if (order.snapshots.length > 800) order.snapshots = order.snapshots.slice(-600);
  const d = dropRate(order);
  if (d && d.over && order.refills < CFG.maxRefills) {
    log('drop_detected', { id: order.id, ...d });
    await notify('이탈 감지 · ' + order.serviceName + '\n최고 ' + d.peak + ' → 현재 ' + d.last + ' (' + (d.dropRate * 100).toFixed(1) + '%)');
    if (CFG.autoRefill) await refill(order, { force: true });
  }
}

async function refill(order, opts = {}) {
  const d = dropRate(order);
  if (!d) throw new Error('스냅샷 2개 미만 - 이탈률 계산 불가');
  if (!d.over && !opts.force) throw new Error('이탈률 ' + (d.dropRate * 100).toFixed(1) + '% - 기준 미달');
  if (order.isPrivate) throw new Error('비공개 계정은 리필 보장 제외');
  if (order.refills >= CFG.maxRefills) throw new Error('리필 한도 도달 (' + order.refills + '/' + CFG.maxRefills + ')');
  const need = Math.ceil(d.peak - d.last);
  if (need <= 0) throw new Error('리필 수량 0');
  const child = await placeOrder({
    service: order.service,
    link: order.link,
    quantity: need,
    isPrivate: order.isPrivate,
  });
  order.refills += 1;
  order.history.push({ at: now(), event: 'refill', childId: child.id, quantity: need });
  await saveDb();
  return { parent: order.id, refillOrder: child, quantity: need };
}

async function tick() {
  let changed = false;
  for (const o of db.orders) {
    if (['active', 'inprogress', 'partial'].includes(o.status)) {
      const before = o.status;
      await syncOrder(o);
      if (o.status !== before) changed = true;
    }
  }
  for (const o of db.orders) {
    if (o.status === 'completed') await watchDrop(o);
  }
  if (changed) await saveDb();
  else await saveDb();
}

function auth(req) {
  if (!CFG.token) return true;
  const h = req.headers.authorization || '';
  return h === 'Bearer ' + CFG.token;
}

function json(res, code, body) {
  const s = JSON.stringify(body);
  res.writeHead(code, { 'content-type': 'application/json; charset=utf-8', 'content-length': Buffer.byteLength(s) });
  res.end(s);
}

async function readBody(req) {
  const chunks = [];
  let size = 0;
  for await (const c of req) {
    size += c.length;
    if (size > 1_000_000) throw new Error('본문이 너무 큽니다');
    chunks.push(c);
  }
  if (!chunks.length) return {};
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}
