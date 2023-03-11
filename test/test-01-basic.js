/* eslint-disable function-paren-newline */
/* eslint-disable comma-dangle */
/* eslint-disable no-await-in-loop */
/* eslint-disable no-bitwise */
/* eslint-disable no-underscore-dangle */
/* eslint-disable no-undef */

import { describe, it, before, after } from 'node:test';
import { strict } from 'node:assert';

import { setTimeout as setTimeoutAsync } from 'timers/promises';
import { objRndFlat } from 'acropolis-nd/lib/Eratosthenes.js';
import { ConLog, consolDummy, inspectIt } from 'acropolis-nd/lib/scripts/nodeOnly.js';
import { CHclient, flagsCH } from '../index.js';

import { confCH } from '../acropolis-ch-conf.js';
import { sqlTests } from '../lib/structures/tests.js';
import { filterFormats } from '../lib/sql/varsCH/formats.js';

const logger = (Object.keys(ConLog.levels).includes(process.argv.at(2) )) ? 
    new ConLog(process.argv[2], { inclTS: true, inspectDefaults: {colors: true} }) : consolDummy

 describe('basic functionality',  { concurrency: true }, () => {
  let client;
  let result;
  const testDbName = 'testCH';
  const NSnumbers = `${testDbName}.numbers`;
  const NSobjRndFlat = `${testDbName}.objRndFlat`;
  // [ 'spare1', 'spare2', 'resolve', 'emitOnRequest', 'throwNon200', 'throwClient' ]
  const sqlExec = async (fn) => {
    result = await fn;
    inspectIt({ result }, logger, { breakLength: 140 });
    strict.equal(result.statusCode, 200 )
    return result;
  };

  before(async () => {
    client = new CHclient(confCH.uri, confCH.credentials, { connections: 10 }); 
  });

  after(async () => {
    await setTimeoutAsync(100);
    await client.close();
    await setTimeoutAsync(100); // give it some time if prints are pending
  });

  it('client ok and ping ok', async () => {
    strict.ok(client._props.server_ok) 
    result = await client.ping();
    strict.equal(result.statusCode, 200);
  });

  it('ping response time', async () => {
    result = await client.pingMS();
    strict.ok(result.ms < 300)
    // const res = await client.get('SELECT * FROM test.test1 FORMAT JSON');
  });

  it('selectNoDBtouched', async () => {
    result = await client.get('SELECT 1');
    strict.equal(result.body, '1\n');
    result = await client.get('SELECT 1 FORMAT JSON');
    strict.equal(result.body.rows, 1);
    
  });

  it('createTables & populate', async () => {
    // creates an randomly filled array
    const rndObjArr = (start = 1, end = 1000) => {
      const resArr = [];
      for (let cnt = start; cnt < end - start + 2; cnt += 1) {
        resArr.push({ id: cnt, ...objRndFlat() });
      }
      return resArr;
    };

    const ns = NSobjRndFlat;
    result = await sqlExec(client.post(`CREATE DATABASE IF NOT EXISTS ${testDbName}`));
    result = await sqlExec(client.post(`DROP TABLE IF EXISTS ${ns}`));
    result = await sqlExec(client.post(sqlTests.createTableObjRndFlat({ nameSpace: ns })));
    const jsonArr = rndObjArr(1, 10);
    // eslint-disable-next-line no-param-reassign
    jsonArr.forEach((x) => { x.dt = x.dt.getTime(); x.dtCr = x.dtCr.toISOString().substring(0, 19); }); // strip Z from dates; or cast to int
    await sqlExec(client.post(`INSERT INTO ${ns} FORMAT JSONEachRow`, JSON.stringify(jsonArr)));
  });

  it('pipeInOut', async () => {
    const format = 'CSV';  // fastest TabSeparatedRaw
    const rowsCount = 1000000;
    const ns = NSnumbers;
    result = await client.post(`DROP TABLE IF EXISTS ${ns}`);
    result = await client.post(sqlTests.createTableNumbers(ns));
    const flags = flagsCH.unsetFlag('resolve', client.defaultFlags);
    result = await client.get(`SELECT * FROM numbers(1, ${rowsCount}) FORMAT ${format}`, { flags }); // JSONStringsEachRow csv 
    await client.post(`INSERT INTO ${ns} FORMAT ${format}`, result.body);
    result = await client.get(`SELECT count(*) AS count FROM ${ns} FORMAT JSONEachRow`);
    strict.equal(result.body.count, rowsCount.toString());  // toString coz  UInt64 are coming as strings
  });

  it('checkDecoding', async () => {
    // sql must return one liner for most of JSON-like formats only JSON can resolve in multiple liners
    const decFormats = filterFormats((x) => x.out === true);
    // eslint-disable-next-line arrow-body-style
    const promises = decFormats.map(async (format, idx) => {
      return { idx, format: `${format}`, result: await client.get(`SELECT count(*) AS count FROM ${NSobjRndFlat} FORMAT ${format}`, { flags: 4 }) };
      // return {id: `${format}`, result: await client.get(`SELECT * FROM ${NSobjRndFlat} Limit 2 FORMAT ${format}`, {}, 4)};
    });
    result = await Promise.all(promises);
    result.map((res) => {
      logger.inspectIt({ statusCode: res.result.statusCode, body: res.result.body }, res.format);
      strict.equal(res.result.statusCode, 200);
      return true;
    });
  });

  it('MultipleConnections', async () => {
    const clientLocal = new CHclient(confCH.uri, confCH.credentials, { connections: 20 });
    const requestCount = 10;
    const rowsCount = 1000000;
    const flags = flagsCH.flagsToNum(['resolve']); //
    const fnFetch = async () => clientLocal.get(`SELECT * FROM numbers(1, ${rowsCount}) FORMAT CSV`, { flags });
    // eslint-disable-next-line quotes
    const fnHttpConnections = async () => clientLocal.get("SELECT * FROM system.metrics WHERE metric = 'HTTPConnection' FORMAT JSON", { flags });
    const promisesArr = [];
    for (let idx = 0; idx < requestCount; idx += 1) { promisesArr.push(fnFetch()); }  // for some reason array.fill doesn't work here
    promisesArr.push(fnHttpConnections());
    const resultsAll = await Promise.all(promisesArr);
    const [firstResult] = resultsAll;
    const resSConnections = resultsAll[resultsAll.length - 1];
    // logger.inspectIt(resSConnections);
    const HTTPConnections = resSConnections.body.data[0].value | 0;

    let allSame = resultsAll.slice(0, resultsAll.length - 2).map((el) => el.body.length); // got content and same for all
    strict.ok( HTTPConnections >= 3); // parallel connections though actual connections depends on client connections and other factors
    allSame = allSame.every((len) => len === firstResult.body.length);
    strict.ok(allSame);
    return

    logger.inspectIt({ HTTPConnections, allSame, parallelTasks: promisesArr.length });
    clientLocal.close();
  });

  it('checkEvents', async () => { // checkEvents
      
    const dirtyCount = (counter, tag, inc) => { counter[tag] = counter[tag] ? counter[tag] + inc : inc; return counter; };
    const counter = {};
    const localClient = new CHclient(confCH.uri, confCH.credentials, { connections: 1, name: 'localClient' });
    const display = async (group, event, args) => {
      if (group === localClient.name) { // filter coz can have same event from other group (client)
        dirtyCount(counter, event, 1);
        logger.inspectIt({ group, event, ...args }, 'event', { depth: 40, breakLength: 140 });
      }
    };
    localClient.events.onAll(display);
    result = await localClient.get('SELECT * FROM numbers(1, 1) FORMAT CSV', { flags: flagsCH.flagsToNum(['resolve', 'emitOnRequest']) });
    result = await localClient.get('SELECT * FROM numbers(1, 1) FORMAT CSV', { flags: flagsCH.flagsToNum(['resolve']) });
    result = await localClient.get('SELECT * FROM numbers(1, 1) FORMAT CSV', { flags: flagsCH.flagsToNum(['resolve', 'emitOnRequest']) });
    try {
      result = await localClient.get('SELECT * FROM "foo1.2346" FORMAT CSV', { flags: flagsCH.flagsToNum(['resolve', 'throwNon200']) });
    // eslint-disable-next-line no-empty
    } catch (err) {}// 'self induced 404 no worries'
    await localClient.close();
    delete counter.Created;  // just in case coz not sure we get it depending on timings and race conditions
    const sumOfEvents = Object.values(counter).reduce((sum, x) => sum + x);
    const sumOfEventsTypes = Object.keys(counter).length;
    strict.equal(sumOfEvents, 4);
    strict.equal(sumOfEventsTypes, 3); // { Created: 1, Request: 2, Error: 1, Closed: 1 }
  });
});
