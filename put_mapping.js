const es = require('./src/es');
const dataLabelMapping = require('./mapping/data_label.json');
require('dotenv').config();

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
    console.log(await es.initIndexWithSettings(`${process.env.PROJECT_ID}-data_label_set_info_index`, dataLabelMapping, settings));
};

initIndex();
