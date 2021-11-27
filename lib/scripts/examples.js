/* eslint-disable no-console */
import { inspectIt } from 'acropolis-nd/lib/scripts/nodeOnly.js';

import { confCH, runOptions } from '../../config.js';
import { ClickHouseUndici } from '../client.js';

import { fetch } from 'undici';
// const chInst = new ClickHouseUndici(confCH, { logger: getLogger('clHS', loggerConfig.level), verifyServer: true })}
const logger = (runOptions.allowInspect === true) ? console : null;
inspectIt(confCH, logger, 'Example', { breakLength: 140 });

// 'http://admin:nickmiln@127.0.0.1:8123/ping'

const { connections } = confCH;

const client = new ClickHouseUndici(confCH.uri, { connections, credentials: confCH.credentials, logger: console });

client.test()
  .then((resp) => {
    inspectIt({ resp }, console, 'client.test', { breakLength: 140 });
  }).catch((err) => {
    inspectIt({ err }, console, 'client.test_err', { breakLength: 140 });
    console.log(err);
  });

/*

client._request('/ping')
  .then((resp) => {
    console.log({ resp });
  }).catch((err) => {
    console.log(err);
  });

const result = await client.request('/ping');
console.log ({ result }



client.ping()
  .then((resp) => {
    const { statusCode, headers, trailers, body } = resp;
    console.log({x: '================================', statusCode, trailers, headers, body, isPaused: body.isPaused() });
    body.setEncoding('utf8');
    body.on('data', (dt) => console.log({ dt }));
    body.on('end', () => {
      console.log('trailers', trailers);
    });
  }).catch((err) => {
    console.log(err);
  });

/*


async function fetchJson() {
  let res = fetch(`${confCH.uri}/ping`)
  console.log({res})
  res = await (res);
  console.log({res})
  console.log({body: res.body})
  const data = await res.text()
  console.log({data});
}
fetchJson();
*/