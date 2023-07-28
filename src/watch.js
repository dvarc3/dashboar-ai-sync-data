
const async = require('async');
const es = require('../src/es');

const watch_collection = (con, db, coll, index, resume_token, path) => {
    const q = async.queue(async (value) => {
        const data = value.fullDocument;
        if (data) {
            data.mid = data._id;
            const _id = data._id;
            delete data._id;
            if (value.operationType === 'insert' || value.operationType === 'replace' || value.operationType === 'update') {
                await es.insert(index, _id, data);
            } else if (value.operationType === 'delete') {
                await es.deleteId(index, _id);
            }
        }
    }, 1);
    q.error((err, task) => {
        console.log(`error : ${err}`);
        console.log(task);
    });
    con.db(db).collection(coll).watch({ fullDocument: 'updateLookup', resumeAfter: resume_token })
        .on('change', async (value) => {
            q.push(value);
        })
        .on('error', (err) => {
            console.log(`${new Date()} error: ${err}`);
            watch_collection(con, db, coll, index, resume_token, path);
        });
};
module.exports = {
    watch_collection
};
