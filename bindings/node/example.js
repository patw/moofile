#!/usr/bin/env node
/**
 * example.js — MooFile Node.js usage examples.
 *
 * Run:  node example.js
 * Requires: npm install, libmoofile.so built
 */

const { Collection } = require('./moofile');
const path = require('path');
const fs = require('fs');
const os = require('os');

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'moofile-ex-'));

console.log('=== MooFile Node.js Examples ===\n');

// -----------------------------------------------------------------------
// 1. Basic CRUD
// -----------------------------------------------------------------------
{
    const db = new Collection(path.join(dir, 'contacts.bson'), {
        indexes: ['email'],
    });

    // Insert
    const alice = db.insert({ name: 'Alice', email: 'alice@example.com', age: 30 });
    console.log(`1. Inserted: ${alice.name} (_id: ${alice._id})`);

    // Insert many
    db.insertMany([
        { name: 'Bob', email: 'bob@example.com', age: 25 },
        { name: 'Carol', email: 'carol@example.com', age: 35 },
    ]);
    console.log(`   Total contacts: ${db.count()}`);

    // Find one
    const found = db.findOne({ email: 'alice@example.com' });
    console.log(`2. Found: ${found.name}, age ${found.age}`);

    // Update
    db.updateOne({ email: 'alice@example.com' }, { age: 31 });
    console.log(`3. Updated age: ${db.findOne({ email: 'alice@example.com' }).age}`);

    // Find with filter
    const over30 = db.find({ age: { $gte: 30 } }).toArray();
    console.log(`4. Contacts over 30: ${over30.length}`);

    // Delete
    db.deleteOne({ email: 'bob@example.com' });
    console.log(`5. After delete, total: ${db.count()}`);

    db.close();
}

// -----------------------------------------------------------------------
// 2. Vector Search
// -----------------------------------------------------------------------
{
    const db = new Collection(path.join(dir, 'vectors.bson'), {
        vector_indexes: { embedding: 3 },
    });

    db.insertMany([
        { _id: 'doc1', title: 'ML Guide', embedding: [1.0, 0.0, 0.0] },
        { _id: 'doc2', title: 'Deep Learning', embedding: [0.5, 0.5, 0.0] },
        { _id: 'doc3', title: 'Cooking', embedding: [0.0, 0.0, 1.0] },
    ]);

    const results = db.vectorSearch('embedding', [1.0, 0.0, 0.0], 3).toArray();
    console.log('\n6. Vector Search Results:');
    for (const { doc, score } of results) {
        console.log(`   ${doc.title}: similarity = ${score.toFixed(4)}`);
    }

    db.close();
}

// -----------------------------------------------------------------------
// 3. Text Search
// -----------------------------------------------------------------------
{
    const db = new Collection(path.join(dir, 'text.bson'), {
        text_indexes: ['content'],
    });

    db.insertMany([
        { _id: '1', content: 'Machine learning is transforming AI' },
        { _id: '2', content: 'Deep neural networks for ML' },
        { _id: '3', content: 'Cooking recipes for dinner' },
    ]);

    const results = db.textSearch('content', 'machine learning', 5).toArray();
    console.log('\n7. Text Search Results:');
    for (const { doc, score } of results) {
        console.log(`   [${doc._id}] ${doc.content}: score = ${score.toFixed(4)}`);
    }

    db.close();
}

// -----------------------------------------------------------------------
// 4. Batch Atomic Write
// -----------------------------------------------------------------------
{
    const db = new Collection(path.join(dir, 'batch.bson'));

    db.batch(() => {
        db.insert({ _id: 'a', type: 'transaction', amount: 100 });
        db.insert({ _id: 'b', type: 'transaction', amount: -50 });
        // All or nothing — if this throws, both inserts are rolled back
    });

    const count = db.count({ type: 'transaction' });
    console.log(`\n8. Batch insert: ${count} transactions committed atomically`);

    db.close();
}

// -----------------------------------------------------------------------
// 5. Autoembedding (requires a GGUF model)
// -----------------------------------------------------------------------
{
    console.log('\n9. Autoembedding (skipped — requires GGUF model file)');
    console.log('   To run: configure auto_embed with a model URI');
    console.log('   const db = new Collection("semantic.bson", {');
    console.log('     vector_indexes: { embedding: 1024 },');
    console.log('     auto_embed: {');
    console.log('       content: {');
    console.log('         model: "hf:jsonMartin/voyage-4-nano-gguf:voyage-4-nano-q8_0.gguf",');
    console.log('         target: "embedding",');
    console.log('         dims: 1024,');
    console.log('         precision: "int8",');
    console.log('       }');
    console.log('     }');
    console.log('   });');
    console.log('   db.insert({ content: "Machine learning is fascinating" });');
    console.log('   // doc.embedding auto-generated');
    console.log('   const results = db.semantic("content", "deep learning", 5);');
}

console.log('\n=== Done ===');

// Cleanup
fs.rmSync(dir, { recursive: true, force: true });
