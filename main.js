'use strict';

var lngr = lngr || {};

const {MongoClient} = require('mongodb');
const {BigQuery} = require('@google-cloud/bigquery');
const request = require("request-promise");
const filesystem = require("fs");

lngr.KEYWORDS = [
  null, 'web', 'web server', 'web front', 'mobile', 'cloud',
  'machine learning', 'infrastructure', 'iot', 'linux', 'game',
];
lngr.MONGO_DB_URL = 'mongodb://localhost:27017/';
lngr.MONGO_DB_NAME = 'langrank';
lngr.MONGO_QUERY = { type: 'data' };
lngr.HEADERS = {
  'Accept': 'application/vnd.github.v3+json',
  'User-Agent': 'lang-rank',
  'Content-Type': 'application/json',
};
lngr.WAIT_SECOND = 8;
lngr.RETRY_LIMIT = 2;

lngr.sleep = function (sec) {
  return new Promise((resolve, _) => {
    setTimeout(resolve, sec * 1000);
  });
};

lngr.makeLanguageParameter = (lang) => 
    lang
    .toLowerCase()
    .replace(/ /g, '-')
    .replace(/\+/g, 'p')
    .replace(/#/g, 'sharp'); 

lngr.getRecord = async function(initialize = false) {
  let conn;
  try {
    conn = await MongoClient.connect(lngr.MONGO_DB_URL);
    const db =  conn.db(lngr.MONGO_DB_NAME);
    const col = db.collection(lngr.key_name);
    const doc = await col.findOne(lngr.MONGO_QUERY);
    if (doc) {
      return doc;
    } else if (initialize) {
      const newdoc = Object.assign({ data: {} }, lngr.MONGO_QUERY);
      await col.insertOne(newdoc);
      return newdoc;
    }
    throw new Error('No data record found but initialization not performed');
  } finally {
    if (conn) {
      conn.close();
    }
  }
};

lngr.updateRecord = async function(update) {
  let conn;
  try {
    conn = await MongoClient.connect(lngr.MONGO_DB_URL);
    const db =  conn.db(lngr.MONGO_DB_NAME);
    const col = db.collection(lngr.key_name);
    const ret = await col.updateOne(lngr.MONGO_QUERY, update);
    return ret;
  } finally {
    if (conn) {
      conn.close();
    }
  }
};

lngr.queryTopLanguages = async function(k = 200) {
  const bigqueryClient = new BigQuery();
  const bquery = `SELECT lng.name, COUNT(lng.name) AS count
  FROM \`bigquery-public-data.github_repos.languages\`, UNNEST(language) AS lng
  GROUP BY lng.name
  ORDER BY count DESC
  LIMIT ${k}`;
  const options = {
    query: bquery,
    location: 'US',
  };
  const [job] = await bigqueryClient.createQueryJob(options);
  const [rows] = await job.getQueryResults();
  return rows;
};

lngr.convertRowsToArray = (rows) => rows.map(row => row.name);

lngr.batchCountRepositories = async function (word, langs, data, onUpdate) {
  const wordParam = word ? word.replace(/ /g, '+') : null;
  for (const lang of langs) {
    if (!data.hasOwnProperty(lang)) {
      const langParam = lngr.makeLanguageParameter(lang);
      const url = 'https://api.github.com/search/repositories' +
        `?q=${wordParam ? wordParam + '+' : ''}language:${langParam}&per_page=1`;
      const options = {
        method: 'GET',
        uri: url,
        headers: lngr.HEADERS,
      };
      let ok = false;
      let err;
      for (let retry = 0; retry <= lngr.RETRY_LIMIT; ++retry) {
        await request(options)
        .then(function (resp) {
          const json = JSON.parse(resp);
          const val = {
            count: json.total_count,
            exact: !(json.incomplete_results),
          };
          data[lang] = val;
          ok = true;
        }).catch(function (e) {
          err = e;
          ok = false;
        });
        if (ok) {
          onUpdate();
          await lngr.sleep(lngr.WAIT_SECOND);
          break;
        } else {
          await lngr.sleep(lngr.WAIT_SECOND);
        }
      }
      if (!ok) {
        data[lang] = {
          count: 0,
          exact: false,
          is_err: true
        };
        onUpdate();
        console.error(err);
      }
    }
  }
};

lngr.invalidateLanguage = function(dataRef, dataOut) {
  for (const lang of Object.keys(dataRef)) {
    if (dataRef[lang].is_err) {
      dataOut[lang] = {
        count: 0,
        exact: false,
        is_err: true,
      };
    }
  }
};

lngr.main = async function(key_name = 'default') {
  lngr.key_name = key_name;

  const load = async function(record) {
    let langs;
    if (record.hasOwnProperty('langs')) {
      const rows = record.langs;
      langs = lngr.convertRowsToArray(rows);
    } else {
      const rows = await lngr.queryTopLanguages();
      record.langs = rows;
      lngr.updateRecord({$set: { langs: rows }});
      langs = lngr.convertRowsToArray(rows);
    }
    for (const word of lngr.KEYWORDS) {
      const dataKey = word || 'all';
      if (!record.data.hasOwnProperty(dataKey)) {
        record.data[dataKey] = {};
      }
      if (dataKey !== 'all') {
        lngr.invalidateLanguage(record.data['all'], record.data[dataKey]);
        const update = {$set: { data: record.data }};
        lngr.updateRecord(update);
      }
      await lngr.batchCountRepositories(word, langs, record.data[dataKey], () => {
        const update = {$set: { data: record.data }};
        lngr.updateRecord(update);
      });
    }
  };

  const writeOut = function(record) {
    for (const word of Object.keys(record.data)) {
      const arr = [];
      for (const lang of Object.keys(record.data[word])) {
        const elem = record.data[word][lang];
        const elemAll = record.data['all'][lang];
        const rat = (elemAll.count > 0 ? (elem.count / elemAll.count) : 0.0);
        arr.push({
          language: lang,
          repos_count: elem.count,
          ratio: rat
        });
      }
      arr.sort((a, b) => b.repos_count - a.repos_count);
      let lines = 'position, language, repos_count, ratio\r\n';
      for (let i = 0; i < arr.length; ++i) {
        const e = arr[i];
        lines += `${(i+1)}, ${e.language}, ${e.repos_count}, ${e.ratio}\r\n`;
      }
      const wordName = word ? word.replace(/ /g, '-') : null;
      const filename = `${lngr.key_name}_${wordName}.csv`;
      filesystem.writeFileSync(filename, lines);
    }
  };

  const record = await lngr.getRecord(true);
  load(record);
  writeOut(record);
};

lngr.main(...process.argv.slice(2));
