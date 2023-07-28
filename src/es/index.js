const { Client } = require('es7');

const URL = 'http://localhost:9200';
const client = new Client({ node: URL });
const deleteClient = new Client({ node: URL, requestTimeout: 1000 * 60 * 20 }); // 1200 seconds

const insert = async (index, id, body) => {
    const result = await client.index({
        index,
        id,
        body,
        refresh: true
    });
    return result;
};

const bulk = async (x_index, dataset) => {
    const body = dataset.flatMap(doc => [{ index: { _index: x_index, _id: doc.mid } }, doc]);
    const result = await client.bulk({ refresh: true, body });
    return result;
};

const bulkUpdate = async (x_index, dataset, doc_as_upsert) => {
    if (doc_as_upsert) {
        const body = dataset.flatMap(doc => [{ update: { _index: x_index, _id: doc.mid } }, { doc: doc , doc_as_upsert}]);

        const result = await client.bulk({ refresh: true, body });
        return result;
    } else {
        const body = dataset.flatMap(doc => [{ update: { _index: x_index, _id: doc.mid } }, { doc: doc }]);

        const result = await client.bulk({ refresh: true, body });
        return result;
    }
};

const update = async (index, id, body) => {
    const result = await client.update({
        index,
        id,
        body: {
            doc: body
        }
    });
    return result;
};

const indexExits = async (index) => {
    const result = await client.indices.exists({
        index
    });
    return result;
};

const initIndex = async (index, properties) => {
    const checkIndex = await indexExits(index);
    if (!checkIndex.body) {
        const result = await client.indices.create({
            index,
            body: {
                mappings: {
                    properties
                }
            }
        });
        console.log(result);
    }
};

const initIndexWithSettings = async (index, properties, settings) => {
    const checkIndex = await indexExits(index);
    if (!checkIndex.body) {
        const result = await client.indices.create({
            index,
            body: {
                settings,
                mappings: {
                    properties
                }
            }
        });
        console.log(result);
    }
};

const search = async (index, body) => {
    const result = await client.search({
        index,
        body: {
            size: 1,
            sort: { created_time: 'desc' },
            query: {
                match_all: {}
            }
        }
    });
    return result;
};

const closeIndex = async (index) => {
    const result = await client.indices.close({
        index
    });
    return result;
};

const putSettings = async (index, settings) => {
    const result = await client.indices.putSettings({
        index,
        body:
        {
            settings
        }
    });
    return result;
};

const openIndex = async (index) => {
    const result = await client.indices.open({
        index
    });
    return result;
};

/**
 * add new value mapping . Ví dụ : update created_time
 * {
        properties: {
            created_time: {
                type: 'date'
            }
        }
    }
 */
const updateMapping = async (index, fieldUpdate) => {
    const result = await client.indices.putMapping({
        index,
        body:
        {
            properties: fieldUpdate
        }
    });
    console.log(result);
};

/**
 * delete index
 * @param {*} index
 */
const deleteIndex = index => new Promise((resolve) => {
    client.indices.delete({
        index
    }).then(resp => resolve(resp), (err) => {
        resolve(err);
    });
});


const deleteByQuery = (index, query) => new Promise((resolve) => {
    deleteClient.deleteByQuery({ 
        index, body: query
    }).then(resp => resolve(resp), (err) => {
        // console.log(JSON.stringify(err));
        resolve(err);
    });
});

module.exports = {
    update,
    insert,
    initIndex,
    search,
    deleteIndex,
    updateMapping,
    bulk,
    closeIndex,
    putSettings,
    openIndex,
    bulkUpdate,
    initIndexWithSettings,
    deleteByQuery,
    client,
    deleteClient
};
