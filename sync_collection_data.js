const { MongoClient: mongo } = require('mongodb');
require('dotenv').config();
const async = require('async');
const es = require('./src/es');

let db;
let dbRep;
let running = false;
const es_index = 'data_index';
const table = 'data_set_info';

const loadDB = async () => {
    if (db) {
        return db;
    }
    try {
        const client = await mongo.connect(process.env.MONGO_URL_PROD, { useNewUrlParser: true, useUnifiedTopology: true });
        db = client.db(process.env.USER_DB);
    } catch (err) {
        console.log(err);
    }
    return db;
};

const loadDBRep = async () => {
    if (dbRep) {
        return dbRep;
    }
    try {
        const client = await mongo.connect(process.env.MONGO_URL_PROD_REP, { useNewUrlParser: true, useUnifiedTopology: true });
        dbRep = client.db(process.env.USER_DB);
    } catch (err) {
        console.log(err);
    }
    return dbRep;
};

const updateData = (queryA, newvalues) => new Promise(async (resolve, reject) => {
    const db = await loadDB();
    db.collection(table).updateOne(queryA, newvalues, (err, doc) => {
        if (err) console.log(err);
        resolve();
    });
});

const handleAsync = result => new Promise(async (resolve) => {
    const bulkArray = [];
    result.forEach((data) => {
        data.mid = data._id;
        delete data._id;
        bulkArray.push(data);
    });
    let resultBulk;
    try {
        resultBulk = await es.bulk(es_index, bulkArray);
    } catch (err) {
        console.log(`INDEX ERROR::${err}`);
        resolve();
        return;
    }
    if (!resultBulk.body.errors) {
        async.eachSeries(result, async (data) => {
            const newvalues = { $set: { processed: true } };
            const queryA = { _id: data.mid };
            await updateData(queryA, newvalues);
        }, (err) => {
            resolve();
            if (err) {
                console.log(err);
            }
        });
    } else {
        resolve();
    }
});

const handle = async (dbo) => {
    const query = { processed: false };
    const result = await dbo.collection(table).find(query)
        .limit(200)
        .toArray();
    if (result.length > 0) {
        await handleAsync(result);
    }
    running = false;
};

const update = async () => {
    const dbo = await loadDBRep();
    handle(dbo);
};

const forloop = async () => {
    if (!running) {
        running = true;
        try {
            await update();
        } catch (err) {
            console.log(err);
            running = false;
        }
    }
};
(async () => {
    console.log(`SYNC_COLLECTION_DATA START`);
    setInterval(() => forloop(), 500);
})().catch(() => {
    console.log('FAILED TO START SYNC_COLLECTION_DATA');
});
