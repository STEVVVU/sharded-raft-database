// Pure adapter contract checks. No k6 process, network, service or timing campaign.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(`${__dirname}/benchmark-k6.js`, 'utf8');

async function harness(mode = 'normal') {
  const calls = [], receipts = [], values = new Map(), metrics = new Map();
  const config = { Token: 'synthetic-test-token-00000000', Nodes: [] };
  for (const group of ['controller', 'a', 'b']) {
    for (let i = 0; i < 3; i++) config.Nodes.push({ Group: group, HTTP: `127.0.0.1:${10000 + config.Nodes.length}` });
  }
  let lost = false, sequence = 0;
  const http = {
    expectedStatuses: () => null, setResponseCallback: () => {},
    post(url, body, params) {
      const command = JSON.parse(body);
      assert.equal(params.headers.Authorization, `Bearer ${config.Token}`);
      calls.push({ url, ...command });
      let result;
      if (url.endsWith('/config')) {
        result = { state: { Owners: Array.from({ length: 4 }, (_, i) => ({ Group: i % 2 ? 'b' : 'a', Epoch: 1 })) } };
      } else {
        if (mode === 'reject-http') return { status: 401 };
        if (mode === 'reject') return { status: 200, json: () => ({ error: 'key capacity' }) };
        if (command.Client.startsWith('bench-')) {
          // Only one VU is driven in each isolated module, matching k6's per-VU state.
          assert.ok(command.Seq >= sequence);
          sequence = command.Seq;
        }
        if (command.Op === 'put') values.set(command.Key, command.Value);
        result = { value: values.get(command.Key) || '' };
        if (mode === 'lost-return' && !lost && command.Client.startsWith('bench-')) {
          lost = true;
          return { status: 0 };
        }
      }
      return { status: 200, json: () => result };
    },
  };
  class Metric {
    constructor(name) { this.name = name; metrics.set(name, []); }
    add(value) { metrics.get(this.name).push(value); }
  }
  const execution = { vu: { idInTest: 1 }, scenario: { iterationInTest: 0 }, test: { abort: () => { throw new Error('aborted'); } } };
  const context = vm.createContext({
    __ENV: { CLUSTER_CONFIG: 'owned-config', K6_SUMMARY: 'owned-summary', READS_PERCENT: '0', VALUE_BYTES: '1024' },
    open: () => JSON.stringify(config), console: { log: line => receipts.push(JSON.parse(line)) },
  });
  const imports = {
    'k6/http': { default: http }, 'k6/execution': { default: execution },
    k6: { check: (value, checks) => Object.values(checks).every(check => check(value)), group: (_name, action) => action(), sleep: () => {} },
    'k6/metrics': { Counter: Metric },
  };
  const module = new vm.SourceTextModule(source, { context });
  await module.link(name => new vm.SyntheticModule(Object.keys(imports[name]), function () {
    for (const [key, value] of Object.entries(imports[name])) this.setExport(key, value);
  }, { context }));
  await module.evaluate();
  return { module: module.namespace, calls, receipts, values, metrics };
}

(async () => {
  const fixture = await harness();
  fixture.module.setup();
  assert.equal(fixture.values.size, 160);
  const occupancy = [0, 0, 0, 0];
  for (const [key, value] of fixture.values) {
    occupancy[fixture.module.shard(key)]++;
    assert.equal(Buffer.byteLength(value), 1024);
  }
  assert.deepEqual(occupancy, [39, 41, 41, 39]);
  assert.deepEqual([...new Set(fixture.calls.filter(c => c.Client).map(c => c.Client))], ['seed-v2']);
  const nativeSummary = { metrics: { group_duration: { values: { 'p(99)': 7 } } } };
  assert.equal(fixture.module.handleSummary(nativeSummary)['owned-summary'], JSON.stringify(nativeSummary));

  const lost = await harness('lost-return');
  lost.module.default();
  const operations = lost.calls.filter(c => c.Client);
  assert.deepEqual(operations.map(c => [c.Op, c.Client, c.Seq]), [
    ['put', 'bench-v2-0', 1], ['put', 'bench-v2-0', 1], ['get', 'bench-v2-0', 2],
  ]);
  assert.equal(lost.metrics.get('transaction_finished').reduce((a, b) => a + b, 0), 1);
  assert.notEqual(operations[0].Value, lost.module.value(0), 'first write must change its seeded value');
  assert.ok(lost.receipts.every(r => !JSON.stringify(r).includes('synthetic-test-token')));

  const rejected = await harness('reject');
  assert.throws(() => rejected.module.default(), /aborted/);
  assert.equal(rejected.calls.filter(c => c.Client).length, 1);
  assert.equal(rejected.metrics.get('transaction_rejected').at(-1), 1);
  assert.equal(rejected.receipts.at(-1).seq, 1);
  const unauthorized = await harness('reject-http');
  assert.throws(() => unauthorized.module.default(), /aborted/);
  assert.equal(unauthorized.calls.filter(c => c.Client).length, 1);
  assert.equal(unauthorized.metrics.get('transaction_rejected').at(-1), 1);
  console.log('4 pure adapter contracts passed: bounded fixture, retained retry identity, native capacity and HTTP rejection fail-stop');
})().catch(error => { console.error(error); process.exitCode = 1; });
