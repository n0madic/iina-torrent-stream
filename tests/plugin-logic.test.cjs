const assert = require("node:assert/strict");
const test = require("node:test");

const {
  SingleFlight,
  redactDaemonToken,
  withDaemonToken,
} = require("../.test-dist/plugin-logic.js");

test("SingleFlight shares a concurrent attempt", async () => {
  const flight = new SingleFlight();
  let starts = 0;
  let resolve;
  const start = () => {
    starts++;
    return new Promise((done) => {
      resolve = done;
    });
  };

  const first = flight.run(start);
  const second = flight.run(start);

  assert.strictEqual(second, first);
  assert.equal(starts, 1);
  resolve(4111);
  assert.equal(await first, 4111);
});

test("SingleFlight permits retry after rejection", async () => {
  const flight = new SingleFlight();
  let starts = 0;

  await assert.rejects(
    flight.run(async () => {
      starts++;
      throw new Error("start failed");
    }),
    /start failed/,
  );

  assert.equal(await flight.run(async () => ++starts), 2);
});

test("withDaemonToken preserves and encodes query parameters", () => {
  assert.equal(withDaemonToken("/health", "a b"), "/health?token=a%20b");
  assert.equal(
    withDaemonToken("/play?source=magnet%3Afoo", "a&b"),
    "/play?source=magnet%3Afoo&token=a%26b",
  );
});

test("redactDaemonToken removes capabilities from diagnostics", () => {
  assert.equal(
    redactDaemonToken(
      "http://127.0.0.1:4111/play?source=x&token=secret&next=y",
    ),
    "http://127.0.0.1:4111/play?source=x&token=<redacted>&next=y",
  );
});
