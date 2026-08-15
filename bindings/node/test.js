#!/usr/bin/env node
/**
 * test.js — MooFile Node.js binding test suite.
 *
 * Run:  node test.js
 * Requires: npm install, libmoofile.so built
 */

const { Collection, MooFileError } = require('./moofile');
const assert = require('assert');
const fs = require('fs');
const path = require('path');
const os = require('os');

// ---------------------------------------------------------------------------
// Test infra
// ---------------------------------------------------------------------------

let testsRun = 0;
let testsFailed = 0;
let currentTest = '';

function test(name) { currentTest = name; }

function check(cond, msg) {
    if (!cond) {
        console.error(`  FAIL [${currentTest}] ${msg || 'assertion failed'}`);
        testsFailed++;
    }
}

function checkEqual(a, b, msg) {
    const aStr = JSON.stringify(a);
    const bStr = JSON.stringify(b);
    if (aStr !== bStr) {
        console.error(`  FAIL [${currentTest}] ${msg || 'not equal'}: ${aStr} !== ${bStr}`);
        testsFailed++;
    }
}

function tmpDir() {
    const d = fs.mkdtempSync(path.join(os.tmpdir(), 'moofile-node-'));
    return d;
}

function tmpPath(dir, name) {
    return path.join(dir, name);
}

function cleanup(dir) {
    fs.rmSync(dir, { recursive: true, force: true });
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

function testOpenDefault() {
    test('open default');
    const dir = tmpDir();
    const db = new Collection(tmpPath(dir, 'test.bson'));
    check(db.handle !== null);
    check(db.count() === 0);
    db.close();
    cleanup(dir);
}

function testInsertAndFind() {
    test('insert and find');
    const dir = tmpDir();
    const db = new Collection(tmpPath(dir, 'crud.bson'), { indexes: ['email'] });

    const doc = db.insert({ name: 'Alice', email: 'a@test.com', age: 30 });
    check(doc._id !== undefined, 'missing _id');
    check(doc.name === 'Alice');

    const found = db.findOne({ email: 'a@test.com' });
    check(found !== null);
    check(found.name === 'Alice');

    db.close();
    cleanup(dir);
}

function testInsertMany() {
    test('insert many');
    const dir = tmpDir();
    const db = new Collection(tmpPath(dir, 'many.bson'));
    const docs = db.insertMany([{ x: 1 }, { x: 2 }, { x: 3 }]);
    check(Array.isArray(docs));
    check(docs.length === 3);
    check(db.count() === 3);
    db.close();
    cleanup(dir);
}

function testDuplicateRejected() {
    test('duplicate _id rejected');
    const dir = tmpDir();
    const db = new Collection(tmpPath(dir, 'dup.bson'));
    db.insert({ _id: 'a', v: 1 });
    try {
        db.insert({ _id: 'a', v: 2 });
        check(false, 'should have thrown');
    } catch (e) {
        check(e instanceof MooFileError);
    }
    db.close();
    cleanup(dir);
}

function testFindFiltered() {
    test('find with filter');
    const dir = tmpDir();
    const db = new Collection(tmpPath(dir, 'filter.bson'), { indexes: ['status'] });
    db.insertMany([
        { status: 'active', age: 30 },
        { status: 'inactive', age: 25 },
        { status: 'active', age: 20 },
    ]);
    const docs = db.find({ status: 'active' }).toArray();
    check(docs.length === 2);
    db.close();
    cleanup(dir);
}

function testFindComparison() {
    test('find with comparison operators');
    const dir = tmpDir();
    const db = new Collection(tmpPath(dir, 'cmp.bson'));
    db.insertMany([{ age: 20 }, { age: 30 }, { age: 40 }]);

    check(db.find({ age: { $gt: 25 } }).toArray().length === 2);
    check(db.find({ age: { $gte: 30 } }).toArray().length === 2);
    check(db.find({ age: { $lt: 30 } }).toArray().length === 1);
    check(db.find({ age: { $lte: 30 } }).toArray().length === 2);
    check(db.find({ age: { $gte: 25, $lte: 35 } }).toArray().length === 1);

    db.close();
    cleanup(dir);
}

function testFindLogical() {
    test('find with $and/$or/$not');
    const dir = tmpDir();
    const db = new Collection(tmpPath(dir, 'logic.bson'));
    db.insertMany([
        { status: 'a', age: 30 },
        { status: 'b', age: 25 },
        { status: 'a', age: 20 },
    ]);

    check(db.find({ $and: [{ status: 'a' }, { age: { $gt: 25 } }] }).toArray().length === 1);
    check(db.find({ $or: [{ status: 'b' }, { age: { $lt: 25 } }] }).toArray().length === 2);
    check(db.find({ $not: { status: 'a' } }).toArray().length === 1);

    db.close();
    cleanup(dir);
}

function testUpdate() {
    test('update operations');
    const dir = tmpDir();
    const db = new Collection(tmpPath(dir, 'update.bson'));
    db.insert({ _id: 'a', name: 'Alice', age: 30 });

    // $set
    check(db.updateOne({ _id: 'a' }, { age: 31, city: 'NYC' }), 'update failed');
    const doc = db.findOne({ _id: 'a' });
    check(doc.age === 31);
    check(doc.city === 'NYC');

    // $inc
    db.updateOne({ _id: 'a' }, {}, [], { age: 5 });
    check(db.findOne({ _id: 'a' }).age === 36);

    // $unset
    db.updateOne({ _id: 'a' }, {}, ['city']);
    check(db.findOne({ _id: 'a' }).city === undefined);

    // No match throws, matching the Rust and Python contract
    let threw = false;
    try {
        db.updateOne({ _id: 'none' }, { x: 1 });
    } catch (e) {
        threw = e instanceof MooFileError && /no document matches/.test(e.message);
    }
    check(threw, 'updateOne with no match should throw MooFileError');

    // updateMany does not throw — it reports 0
    check(db.updateMany({ _id: 'none' }, { x: 1 }) === 0);

    db.close();
    cleanup(dir);
}

function testFindOptions() {
    test('find options: sort / skip / limit');
    const dir = tmpDir();
    const db = new Collection(tmpPath(dir, 'findopts.bson'));
    db.insertMany([
        { _id: 'a', age: 30, dept: 'eng', pay: 100 },
        { _id: 'b', age: 20, dept: 'eng', pay: 200 },
        { _id: 'c', age: 50, dept: 'ops', pay: 300 },
        { _id: 'd', age: 40, dept: 'ops', pay: 400 },
    ]);

    const asc = db.find({}, { sort: 'age' }).toArray().map(d => d._id);
    checkEqual(asc, ['b', 'a', 'd', 'c'], 'ascending sort');

    const desc = db.find({}, { sort: 'age', desc: true }).toArray().map(d => d._id);
    checkEqual(desc, ['c', 'd', 'a', 'b'], 'descending sort');

    const page = db.find({}, { sort: 'age', skip: 1, limit: 2 }).toArray().map(d => d._id);
    checkEqual(page, ['a', 'd'], 'skip then limit');

    const filtered = db.find({ dept: 'ops' }, { sort: 'age', limit: 1 }).toArray();
    check(filtered.length === 1 && filtered[0]._id === 'd', 'filter + sort + limit');

    // A typo in an option name is an error, not a silent full scan
    let threw = false;
    try {
        db.find({}, { limt: 2 }).toArray();
    } catch (e) {
        threw = /unknown find option/.test(e.message);
    }
    check(threw, 'unknown find option should be rejected');

    db.close();
    cleanup(dir);
}

function testGroupAgg() {
    test('find options: group / agg');
    const dir = tmpDir();
    const db = new Collection(tmpPath(dir, 'groupagg.bson'));
    db.insertMany([
        { _id: 'a', dept: 'eng', pay: 100 },
        { _id: 'b', dept: 'eng', pay: 200 },
        { _id: 'c', dept: 'ops', pay: 300 },
        { _id: 'd', dept: 'ops', pay: 400 },
    ]);

    const rows = db.find({}, {
        group: 'dept',
        agg: ['count', { func: 'sum', field: 'pay' }, { func: 'mean', field: 'pay' }],
        sort: 'dept',
    }).toArray();

    check(rows.length === 2, 'two groups');
    // The group key keeps its original type — a plain string, not a quoted one
    check(rows[0].dept === 'eng', `group key should be "eng", got ${JSON.stringify(rows[0].dept)}`);
    check(rows[0].count === 2, 'eng count');
    check(rows[0].sum_pay === 300, 'eng sum');
    check(rows[0].mean_pay === 150, 'eng mean');
    check(rows[1].dept === 'ops', 'second group key');
    check(rows[1].sum_pay === 700, 'ops sum');

    db.close();
    cleanup(dir);
}

function testCursorIteration() {
    test('cursors are iterable and free themselves');
    const dir = tmpDir();
    const db = new Collection(tmpPath(dir, 'cursor.bson'));
    db.insertMany([{ n: 1 }, { n: 2 }, { n: 3 }]);

    const seen = [];
    for (const doc of db.find({})) seen.push(doc.n);
    check(seen.length === 3, 'for..of over a cursor');

    // Draining a cursor releases it; close() afterwards must not double-free
    const cur = db.find({});
    cur.toArray();
    cur.close();
    cur.close();

    db.close();
    cleanup(dir);
}

function testUseAfterClose() {
    test('operations after close are rejected');
    const dir = tmpDir();
    const db = new Collection(tmpPath(dir, 'closed.bson'));
    db.insert({ x: 1 });
    db.close();

    let threw = false;
    try {
        db.count();
    } catch (e) {
        threw = e instanceof MooFileError;
    }
    check(threw, 'count after close should throw');

    db.close(); // idempotent
    cleanup(dir);
}

function testDelete() {
    test('delete operations');
    const dir = tmpDir();
    const db = new Collection(tmpPath(dir, 'del.bson'));
    db.insertMany([{ _id: 'a' }, { _id: 'b' }, { _id: 'c' }]);

    check(db.deleteOne({ _id: 'a' }));
    check(db.count() === 2);
    check(db.deleteOne({ _id: 'none' }) === false);

    check(db.deleteMany({ _id: { $ne: 'b' } }) === 1);
    check(db.count() === 1);

    db.close();
    cleanup(dir);
}

function testVectorSearch() {
    test('vector search');
    const dir = tmpDir();
    const db = new Collection(tmpPath(dir, 'vec.bson'), { vector_indexes: { emb: 3 } });
    db.insertMany([
        { _id: 'a', emb: [1.0, 0.0, 0.0] },
        { _id: 'b', emb: [0.5, 0.5, 0.0] },
        { _id: 'c', emb: [0.0, 0.0, 1.0] },
    ]);

    const results = db.vectorSearch('emb', [1.0, 0.0, 0.0], 3).toArray();
    check(results.length === 3);
    check(results[0].doc._id === 'a', 'first should be nearest');

    db.close();
    cleanup(dir);
}

function testTextSearch() {
    test('text search');
    const dir = tmpDir();
    const db = new Collection(tmpPath(dir, 'txt.bson'), { text_indexes: ['content'] });
    db.insertMany([
        { _id: '1', content: 'machine learning is fascinating' },
        { _id: '2', content: 'deep learning only' },
        { _id: '3', content: 'cooking' },
    ]);

    const results = db.textSearch('content', 'machine learning', 5).toArray();
    check(results.length === 2);
    check(results[0].doc._id === '1', 'first should be most relevant');

    db.close();
    cleanup(dir);
}

function testHybridSearch() {
    test('hybrid search');
    const dir = tmpDir();
    const db = new Collection(tmpPath(dir, 'hy.bson'), {
        text_indexes: ['content'],
        vector_indexes: { emb: 2 },
    });
    db.insertMany([
        { _id: 'a', content: 'machine learning', emb: [1.0, 0.0] },
        { _id: 'b', content: 'deep learning', emb: [0.0, 0.9] },
        { _id: 'c', content: 'cooking', emb: [0.0, 0.0] },
    ]);

    const results = db.hybridSearch('content', 'emb', 'machine learning', [1.0, 0.0], 3).toArray();
    check(results.length === 3);
    check(results[0].doc._id === 'a');

    db.close();
    cleanup(dir);
}

function testBatch() {
    test('batch commit');
    const dir = tmpDir();
    const db = new Collection(tmpPath(dir, 'batch.bson'));

    db.batch(() => {
        db.insert({ _id: 'a', v: 1 });
        db.insert({ _id: 'b', v: 2 });
    });

    check(db.count() === 2);

    // Rollback on error
    try {
        db.batch(() => {
            db.insert({ _id: 'c' });
            throw new Error('simulated');
        });
    } catch (_) {}

    check(db.count() === 2, 'rollback should discard');

    db.close();
    cleanup(dir);
}

function testStats() {
    test('stats');
    const dir = tmpDir();
    const db = new Collection(tmpPath(dir, 'stats.bson'));
    db.insertMany([{ x: 1 }, { x: 2 }]);

    const s = db.stats();
    check(s.documents === 2);
    check(s.file_size_bytes > 0);

    db.deleteMany({});
    check(db.stats().dead_records >= 2);

    db.compact();
    check(db.stats().dead_records === 0);

    db.close();
    cleanup(dir);
}

function testReembedWithoutConfig() {
    test('reembed surfaces the core error for an unconfigured field');
    const dir = tmpDir();
    const db = new Collection(tmpPath(dir, 'reembed.bson'));
    db.insert({ summary: 'hello' });

    // Exercises the FFI round trip -- including error marshalling -- without
    // needing a model download.
    let threw = false;
    try {
        db.reembed('summary');
    } catch (e) {
        threw = true;
        check(/autoembed/i.test(e.message), `error should name autoembed: ${e.message}`);
    }
    check(threw, 'reembed on an unconfigured field must throw, not return 0');

    db.close();
    cleanup(dir);
}

function testPersistence() {
    test('data persists across close/reopen');
    const dir = tmpDir();
    const p = tmpPath(dir, 'persist.bson');

    const db1 = new Collection(p, { indexes: ['email'] });
    db1.insert({ email: 'a@test.com' });
    db1.close();

    const db2 = new Collection(p, { indexes: ['email'] });
    check(db2.count() === 1);
    check(db2.findOne({ email: 'a@test.com' }) !== null);
    db2.close();

    cleanup(dir);
}

function testReadonly() {
    test('readonly rejects writes');
    const dir = tmpDir();
    const p = tmpPath(dir, 'ro.bson');

    const db1 = new Collection(p);
    db1.insert({ x: 1 });
    db1.close();

    const db2 = new Collection(p, { readonly: true });
    try {
        db2.insert({ x: 2 });
        check(false, 'readonly should throw');
    } catch (e) {
        check(e instanceof MooFileError);
    }
    db2.close();
    cleanup(dir);
}

function testExists() {
    test('exists');
    const dir = tmpDir();
    const db = new Collection(tmpPath(dir, 'ex.bson'));
    check(!db.exists({ x: 1 }));
    db.insert({ x: 1 });
    check(db.exists({ x: 1 }));
    db.close();
    cleanup(dir);
}

function testReplaceOne() {
    test('replace one');
    const dir = tmpDir();
    const db = new Collection(tmpPath(dir, 'rep.bson'));
    db.insert({ _id: 'a', old: 'data' });
    check(db.replaceOne({ _id: 'a' }, { new: 'data' }));
    const doc = db.findOne({ _id: 'a' });
    check(doc.new === 'data');
    check(doc.old === undefined, 'old field should be gone');
    check(doc._id === 'a', '_id preserved');
    db.close();
    cleanup(dir);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

const tests = [
    testOpenDefault,
    testInsertAndFind,
    testInsertMany,
    testDuplicateRejected,
    testFindFiltered,
    testFindComparison,
    testFindLogical,
    testUpdate,
    testDelete,
    testVectorSearch,
    testTextSearch,
    testHybridSearch,
    testBatch,
    testStats,
    testReembedWithoutConfig,
    testPersistence,
    testReadonly,
    testExists,
    testReplaceOne,
    testFindOptions,
    testGroupAgg,
    testCursorIteration,
    testUseAfterClose,
];

console.log('MooFile Node.js Test Suite');
console.log('=========================\n');

for (const t of tests) {
    testsRun++;
    try {
        t();
    } catch (e) {
        console.error(`  FAIL [${currentTest}] exception: ${e.message}`);
        console.error(e.stack);
        testsFailed++;
    }
}

console.log(`\n====================`);
console.log(`Tests:   ${testsRun}`);
console.log(`Passed:  ${testsRun - testsFailed}`);
console.log(`Failed:  ${testsFailed}`);

process.exit(testsFailed > 0 ? 1 : 0);
