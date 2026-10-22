// Version 2: k6 owns arrival scheduling, dropped work and latency aggregation.
import http from 'k6/http';
import exec from 'k6/execution';
import { check, group, sleep } from 'k6';
import { Counter } from 'k6/metrics';

const config = JSON.parse(open(__ENV.CLUSTER_CONFIG));
const size = Number(__ENV.VALUE_BYTES || 64);
const reads = Number(__ENV.READS_PERCENT || 95);
const vus = Number(__ENV.LOAD_VUS || 32);
const rate = Number(__ENV.RATE || 5);
const seconds = Number(__ENV.SECONDS || 10);
if (!__ENV.K6_SUMMARY) throw new Error('K6_SUMMARY must name a task-owned output file');
if (![64, 1024].includes(size) || ![0, 50, 95, 100].includes(reads) ||
    ![1, 8, 32].includes(vus) || ![5, 25, 100].includes(rate) || ![10, 120].includes(seconds)) {
  throw new Error('unsupported version-2 workload cell');
}
if (typeof config.Token !== 'string' || config.Token.length < 24 || config.Token.length > 128 ||
    !Array.isArray(config.Nodes) || config.Nodes.length !== 9 ||
    config.Nodes.some(n => !/^127\.0\.0\.1:[0-9]+$/.test(n.HTTP)) ||
    ['controller', 'a', 'b'].some(g => config.Nodes.filter(n => n.Group === g).length !== 3)) {
  throw new Error('require the existing nine-node owned loopback cluster config');
}

export const options = {
  scenarios: { transactions: {
    executor: 'constant-arrival-rate', rate, timeUnit: '1s', duration: `${seconds}s`,
    preAllocatedVUs: vus, maxVUs: vus, gracefulStop: '65s',
  } },
  setupTimeout: '5m',
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(95)', 'p(99)'],
  thresholds: {
    checks: ['rate==1'], transaction_failed: ['count==0'],
    transaction_rejected: ['count==0'], transaction_uncertain: ['count==0'],
  },
};
if (reads > 0) options.thresholds['group_duration{group:::get}'] = ['p(99)<5000'];
if (reads < 100) options.thresholds['group_duration{group:::put_get}'] = ['p(99)<5000'];

const started = new Counter('transaction_started');
const finished = new Counter('transaction_finished');
const failed = new Counter('transaction_failed');
const rejected = new Counter('transaction_rejected');
const uncertain = new Counter('transaction_uncertain');
const busy = new Counter('native_http_busy');
http.setResponseCallback(http.expectedStatuses(200, 409, 429));

export function shard(key) {
  let hash = 2166136261;
  for (let i = 0; i < key.length; i++) hash = Math.imul(hash ^ key.charCodeAt(i), 16777619) >>> 0;
  return hash % 4; // Workload keys are ASCII; this matches native FNV-1a over UTF-8.
}

export function value(index) {
  const prefix = `${index.toString().padStart(10, '0')}|`;
  return prefix + 'abcdefghijklmnopqrstuvwxyz'.repeat(Math.ceil(size / 26)).slice(0, size - prefix.length);
}

// An init-time invariant checks the actual routed fixture against native limits.
const occupancy = [0, 0, 0, 0];
for (let i = 0; i < 128; i++) occupancy[shard(`read-${i}`)]++;
for (let i = 0; i < 32; i++) occupancy[shard(`write-${i}`)]++;
if (occupancy.some(n => n > 41) || value(421).length !== size) throw new Error('fixture capacity invariant');

function problem(kind, message) {
  const error = new Error(message);
  error.kind = kind;
  return error;
}

function requestGroup(groupName, path, command, deadline) {
  const nodes = config.Nodes.filter(n => n.Group === groupName);
  while (Date.now() < deadline) {
    for (const node of nodes) {
      if (Date.now() >= deadline) break;
      const response = http.post(`http://${node.HTTP}${path}`, JSON.stringify(command), {
        headers: { Authorization: `Bearer ${config.Token}`, 'Content-Type': 'application/json' },
        timeout: `${Math.min(3000, Math.max(1, deadline - Date.now()))}ms`,
        tags: { raft_group: groupName, name: path },
      });
      if (response.status === 429) busy.add(1);
      if ([0, 409, 429, 500, 502, 503, 504].includes(response.status)) continue;
      if (response.status >= 400 && response.status < 500) throw problem('rejected', `HTTP ${response.status} on ${path}`);
      if (response.status !== 200) throw problem('failed', `HTTP ${response.status} on ${path}`);
      let result;
      try { result = response.json(); } catch (_) { throw problem('uncertain', 'invalid native JSON response'); }
      if (!result.error) return result;
      if (result.error === 'wrong owner') return null; // Refresh committed ownership, retaining identity/sequence.
      if (['invalid operation', 'client capacity', 'key capacity', 'value capacity',
           'stale sequence', 'identity conflict'].includes(result.error)) {
        throw problem('rejected', result.error);
      }
      // Raft apply/leadership errors can be uncertain. Retry only this same command.
    }
    if (Date.now() < deadline) sleep(Math.min(0.05, (deadline - Date.now()) / 1000));
  }
  throw problem('uncertain', 'native commit deadline; retry the same identity and sequence');
}

function operation(client, seq, op, key, input = '') {
  const invoke = Date.now();
  const command = { Op: op, Key: key, Value: input, Client: client, Seq: seq, Shard: shard(key) };
  // Synthetic values only. These bounded receipts never include tokens, paths or response bodies.
  const receipt = { version: 2, client, seq, op, key, input, invoke };
  console.log(JSON.stringify({ ...receipt, phase: 'invoke' }));
  while (Date.now() - invoke < 30000) {
    const state = requestGroup('controller', '/config', {}, invoke + 30000).state;
    if (!state || !state.Owners || !state.Owners[command.Shard]) throw problem('failed', 'invalid ownership response');
    const owner = state.Owners[command.Shard];
    if (!['a', 'b'].includes(owner.Group)) throw problem('failed', 'invalid owner group');
    command.Epoch = owner.Epoch;
    const result = requestGroup(owner.Group, '/op', command, invoke + 30000);
    if (result !== null) {
      const actual = result.value || '';
      console.log(JSON.stringify({ ...receipt, phase: 'return', return: Date.now(), value: actual }));
      return actual;
    }
  }
  throw problem('uncertain', 'ownership deadline; retry the same identity and sequence');
}

export function setup() {
  let seq = 0;
  for (let i = 0; i < 128; i++) {
    if (operation('seed-v2', ++seq, 'put', `read-${i}`, value(i)) !== value(i)) throw new Error('read fixture mismatch');
  }
  for (let i = 0; i < 32; i++) {
    if (operation('seed-v2', ++seq, 'put', `write-${i}`, value(i)) !== value(i)) throw new Error('write fixture mismatch');
  }
  // This read establishes warmth outside the measured scenario.
  if (operation('seed-v2', ++seq, 'get', 'read-0') !== value(0)) throw new Error('warm read mismatch');
}

// k6 isolates module state per VU and runs each VU's iterations serially.
let sequence = 0;
export default function () {
  const slot = exec.vu.idInTest - 1;
  if (slot < 0 || slot >= 32) exec.test.abort('version-2 requires at most 32 stable VU identities');
  const client = `bench-v2-${slot}`;
  const iteration = exec.scenario.iterationInTest;
  const isRead = ((iteration * 37 + 421) % 100) < reads;
  const key = isRead ? `read-${iteration % 128}` : `write-${slot}`;
  const expected = value(isRead ? iteration % 128 : 128 + iteration);
  const tags = { transaction: isRead ? 'get' : 'put_get' };
  started.add(1, tags);
  // Initialize failure counters even on entirely successful runs.
  failed.add(0); rejected.add(0); uncertain.add(0);
  try {
    group(tags.transaction, () => {
      const actual = operation(client, ++sequence, isRead ? 'get' : 'put', key, isRead ? '' : expected);
      let valid = actual === expected;
      if (!isRead) valid = operation(client, ++sequence, 'get', key) === expected && valid;
      if (!check(valid, { 'exact committed value and write readback': v => v }, tags)) throw problem('failed', 'value oracle mismatch');
    });
    finished.add(1, tags);
  } catch (error) {
    ({ rejected, uncertain, failed }[error.kind] || failed).add(1, tags);
    console.log(JSON.stringify({ version: 2, phase: 'stop', client, seq: sequence, key,
      outcome: error.kind || 'failed', reason: error.kind ? error.message : 'adapter failure' }));
    // Advancing an unresolved session could invalidate deduplication. Keep its last receipt and stop.
    exec.test.abort('native operation failed; inspect safe receipts and interrupted iterations');
  }
}

export function handleSummary(data) {
  // Preserve k6's native aggregation unchanged; no custom percentile calculation.
  return { [__ENV.K6_SUMMARY]: JSON.stringify(data) };
}
