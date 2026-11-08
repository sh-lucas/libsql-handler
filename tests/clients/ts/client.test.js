// End-to-end checks with the official TypeScript client, which speaks Hrana
// over HTTP (v2 JSON, since the handler offers no protobuf). Run with
// `just test-clients`; LIBSQL_URL points at a running handler.
import { test } from "node:test";
import assert from "node:assert/strict";
import { createClient } from "@libsql/client";

const url = process.env.LIBSQL_URL;
if (!url) throw new Error("LIBSQL_URL not set");
const db = createClient({ url, intMode: "bigint" });
const table = `ts_${Date.now()}`;

test("execute with values of every type", async () => {
  const rs = await db.execute({
    sql: "SELECT ? AS n, ? AS big, ? AS f, ? AS t, ? AS b, NULL AS z",
    args: [1n, 9007199254740993n, 1.5, "héllo", new Uint8Array([0, 255, 1])],
  });
  assert.deepEqual(rs.columns, ["n", "big", "f", "t", "b", "z"]);
  const row = rs.rows[0];
  assert.equal(row.n, 1n);
  assert.equal(row.big, 9007199254740993n);
  assert.equal(row.f, 1.5);
  assert.equal(row.t, "héllo");
  assert.deepEqual(new Uint8Array(row.b), new Uint8Array([0, 255, 1]));
  assert.equal(row.z, null);
});

test("named arguments", async () => {
  const rs = await db.execute({ sql: "SELECT :a AS a, @b AS b, $c AS c", args: { a: 1n, b: "x", c: null } });
  assert.deepEqual([rs.rows[0].a, rs.rows[0].b, rs.rows[0].c], [1n, "x", null]);
});

test("DDL, writes, rowsAffected and lastInsertRowid", async () => {
  await db.execute(`CREATE TABLE ${table} (id INTEGER PRIMARY KEY, v TEXT)`);
  const rs = await db.execute({ sql: `INSERT INTO ${table} (v) VALUES (?), (?)`, args: ["a", "b"] });
  assert.equal(rs.rowsAffected, 2);
  assert.equal(rs.lastInsertRowid, 2n);
});

test("batch is atomic", async () => {
  await assert.rejects(
    db.batch([
      { sql: `INSERT INTO ${table} (v) VALUES (?)`, args: ["c"] },
      `SELECT * FROM missing_table`,
    ], "write"),
  );
  const rs = await db.execute(`SELECT count(*) AS n FROM ${table}`);
  assert.equal(rs.rows[0].n, 2n, "the failed batch must roll back");

  const res = await db.batch([`INSERT INTO ${table} (v) VALUES ('c')`, `SELECT count(*) AS n FROM ${table}`], "write");
  assert.equal(res[1].rows[0].n, 3n);
});

test("interactive transaction: rollback and commit", async () => {
  const tx = await db.transaction("write");
  await tx.execute(`INSERT INTO ${table} (v) VALUES ('d')`);
  const inside = await tx.execute(`SELECT count(*) AS n FROM ${table}`);
  assert.equal(inside.rows[0].n, 4n);
  await tx.rollback();
  assert.equal((await db.execute(`SELECT count(*) AS n FROM ${table}`)).rows[0].n, 3n);

  const tx2 = await db.transaction("write");
  await tx2.execute(`INSERT INTO ${table} (v) VALUES ('e')`);
  await tx2.commit();
  assert.equal((await db.execute(`SELECT count(*) AS n FROM ${table}`)).rows[0].n, 4n);
});

test("executeMultiple and errors", async () => {
  await db.executeMultiple(`INSERT INTO ${table} (v) VALUES ('f'); INSERT INTO ${table} (v) VALUES ('g');`);
  assert.equal((await db.execute(`SELECT count(*) AS n FROM ${table}`)).rows[0].n, 6n);
  await assert.rejects(db.execute("SELECT * FROM missing_table"), /no such table/);
});

test("cleanup", async () => {
  await db.execute(`DROP TABLE ${table}`);
  db.close();
});
