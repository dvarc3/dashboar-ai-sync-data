const es = require('./src/es');
const dataMapping = require('./mapping/data.json');
const dataLabelMapping = require('./mapping/data_label.json');

const settings = {
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

};

const initIndex = async () => {
    console.log(await es.initIndexWithSettings('data_index', dataMapping, settings));
    console.log(await es.initIndexWithSettings('data_label_index', dataLabelMapping, settings));
};

initIndex();
