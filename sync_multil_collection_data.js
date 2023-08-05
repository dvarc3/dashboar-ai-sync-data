const { MongoClient: mongo } = require('mongodb');
require('dotenv').config();
const async = require('async');
const moment = require('moment');
const es = require('./src/es');
const dataMapping = require('./mapping/data.json');
const { isNil } = require('./src/utils');

let db;
let dbRep;
let running = false;
let lastDayRunning = false;
let es_index = 'nogame-data_set_info_index';
const table = 'nogame-data_set_info';
let time_day = '08_02_2023'; //MM-DD-YYYY

const loadDB = async () => {
    if (db) {
        return db;
    }
    try {
        const client = await mongo.connect(process.env.MONGO_URL_PROD, { useNewUrlParser: true, useUnifiedTopology: true });
        db = client.db(process.env.DASHBOARDAI_DB);
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
        dbRep = client.db(process.env.DASHBOARDAI_DB);
    } catch (err) {
        console.log(err);
    }
    return dbRep;
};

const updateData = (queryA, newvalues, col) => new Promise(async (resolve) => {
    const dbx = await loadDB();
    dbx.collection(col).updateOne(queryA, newvalues, (err) => {
        if (err) console.log(err);
        resolve();
    });
});

const handleAsync = (result, col, date) => new Promise(async (resolve) => {
    const start = new Date();
    const bulkArray = [];
    result.forEach((data) => {
        data.mid = data._id;
        delete data._id;
        bulkArray.push(data);
    });
    let resultBulk;
    try {
        es_index = `data_set_info_index_${date}`;
        if (time_day !== date) {
            const result = await es.initIndexWithSettings(es_index, dataMapping, {
                analysis: {
                    normalizer: {
                        keyword_lowercase: {
                            type: 'custom',
                            filter: ['lowercase']
                        }
                    },
                    analyzer: {
                        irrespective_character_analyzer: { 
                            tokenizer: 'icu_tokenizer',
                            filter: ['icu_folding', 'lowercase']
                        }
                    }
                }
            });
            if (result === undefined || (result !== undefined && result.statusCode === 200)) {
                time_day = date;
                resultBulk = await es.bulk(es_index, bulkArray);
            } else {
                console.log('CREATE INDEX ERROR::');
                resolve();
                return;
            }
        } else {
            resultBulk = await es.bulk(es_index, bulkArray);
        }
    } catch (err) {
        console.log(`INDEX ERROR:: ${JSON.stringify(err)}`);
        resolve();
        return;
    }
    if (!resultBulk.body.errors) {
        const arrayJson = resultBulk.body.items.length;
        for (let index = 0; index < arrayJson.length; index++) {
            const element = arrayJson[index];
            if (isNil(element.index) && element.index.status !== 200 && element.index.status !== 201) {
                resolve();
                return;
            }
        }
        async.eachSeries(result, async (data) => {
            const newvalues = { $set: { processed: true } };
            const queryA = { _id: data.mid };
            await updateData(queryA, newvalues, col);
        }, (err) => {
            resolve();
            if (err) {
                console.log(err);
            }
        });
    } else {
        resolve();
    }
    console.log(`End : ${new Date().getTime() - start.getTime()} ms`);
});

const handle = async (dbo, col, date) => {
    const query = { processed: false };
    const result = await dbo.collection(col).find(query)
        .limit(500)
        .toArray();
    if (result.length > 0) {
        await handleAsync(result, col, date);
    }
    running = false;
};

const handleLastDay = async (dbo, col, date) => {
    const query = { processed: false };
    const result = await dbo.collection(col).find(query)
        .limit(500)
        .toArray();
    if (result.length > 0) {
        await handleAsync(result, col, date);
    }
    lastDayRunning = false;
};

const getColName = async date => `${table}_${date}`;

const update = async () => {
    const date = new Date();
    const currentDay = moment(date, 'YYY-MM-DD').format('MM_DD_YYYY');

    const dbo = await loadDBRep();
    handle(dbo, table, currentDay);

    if (date.getHours() < 1 && date.getMinutes() < 5) {
        const lastDay = moment(date, 'YYY-MM-DD').subtract(1, 'D').format('MM_DD_YYYY');
        handleLastDay(dbo, await getColName(lastDay), lastDay);
    } else {
        lastDayRunning = false;
    }
};


const forloop = async () => {
    if (!running && !lastDayRunning) {
        running = true;
        lastDayRunning = true;
        try {
            await update();
        } catch (err) {
            console.log(err);
            running = false;
            lastDayRunning = false;
        }
    }
};
(async () => {
    console.log(`SYNC_COLLECTION_DATA START`);
    setInterval(() => forloop(), 1000);
})().catch(() => {
    console.log('FAILED TO START SYNC_COLLECTION_DATA');
});
