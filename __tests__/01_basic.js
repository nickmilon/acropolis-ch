/* eslint-disable function-paren-newline */
/* eslint-disable comma-dangle */
/* eslint-disable no-await-in-loop */
/* eslint-disable no-bitwise */
/* eslint-disable no-underscore-dangle */
/* eslint-disable no-undef */

import { setTimeout as setTimeoutAsync } from 'timers/promises';
import { ConLog, consolDummy } from '../../acropolis-nd/lib/scripts/nodeOnly.js';
import { UndiciCH, flagsCH, eventsCH } from '../lib/client.js';
import { confCH, runOptions } from '../config.js';
import { sqlTests } from '../lib/structures/tests.js';
import { rndObjArr, } from '../lib/scripts/base.js';
import { filterFormats } from '../lib/sql/varsCH/formats.js';

const logLevel = runOptions?.tests?.logLevel || 'log';

const logger = (logLevel === 'silent') ? consolDummy : new ConLog('debug', { inclTS: true });

// eslint-disable-next-line no-console
console.log(`set logLevel variable in config.js in one of available Levels: ${ConLog.availableLevelsStr()}`);

describe('basic functionality', () => {
  let client;
  let result;
  const testDbName = 'testCH';
  const NSnumbers = `${testDbName}.numbers`;
  const NSobjRndFlat = `${testDbName}.objRndFlat`;
  // [ 'spare1', 'spare2', 'resolve', 'emitOnRequest', 'throwNon200', 'throwClient' ]
  const sqlExec = async (fn) => {
    result = await fn;
    // inspectIt({ result }, logger, sql, { breakLength: 140 });
    expect(result.statusCode).toBe(200);
    return result;
  };

  beforeAll(async () => {
    // const display = (name, event, args) => logger.inspectIt(name, event, args);
    // const display = (name, event, ...args) => console.log({ name, event, args });
    // client = new UndiciCH(confCH.uri, { connections: 10, credentials: confCH.credentials, logger: console });
    client = new UndiciCH(confCH.uri, confCH.credentials, { connections: 10 });
    // client.events.onAll(display);
    /*
    client.events.on('Created', (name, event, args) => display(name, event, args));
    client.events.on('Request', (name, event, args) => display(name, event, args));
    client.events.on('Closed', (name, event, args) => console.log('xxxx', name, event, { args }));
    client.events.on('Error', (name, event, args) => console.log('', name, event, { args }));
    */
    // client.events.on('Closed', (name, event, => console.log('req xxx', { name, event }));
  });

  afterAll(async () => {
    await setTimeoutAsync(100);
    await client.close();
    await setTimeoutAsync(100); // give it some time if prints are pending
  });

  it('client ok and ping ok', async () => {
    expect(client._props.server_ok).toBeTruthy();
    result = await client.ping();
    expect(result.statusCode).toBe(200);
  });

  it('ping response time', async () => {
    result = await client.pingMS();
    expect(result.ms).toBeLessThan(300);
    // const res = await client.get('SELECT * FROM test.test1 FORMAT JSON');
  });

  it('selectNoDBtouched', async () => {
    result = await client.get('SELECT 1');
    expect(result.body).toBe('1\n');
    result = await client.get('SELECT 1 FORMAT JSON');
    expect(result.body.rows).toBe(1);
  });

  it('createTables & populate', async () => {
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
    expect(result.body.count).toBe(rowsCount.toString());  // toString coz  UInt64 are coming as strings
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
      expect(res.result.statusCode).toBe(200);
      return true;
    });
  });

  it('MultipleConnections', async () => {
    const clientLocal = new UndiciCH(confCH.uri, confCH.credentials, { connections: 20 });
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
    expect(HTTPConnections).toBeGreaterThanOrEqual(4);  // parallel connections though actual connections depends on client connections and other factors
    let allSame = resultsAll.slice(0, resultsAll.length - 2).map((el) => el.body.length); // got content and same for all
    allSame = allSame.every((len) => len === firstResult.body.length);
    expect(allSame).toBeTruthy();
    logger.inspectIt({ HTTPConnections, allSame, parallelTasks: promisesArr.length });
    clientLocal.close();
  });

  it('checkEvents', async () => { // checkEvents
    const dirtyCount = (counter, tag, inc) => { counter[tag] = counter[tag] ? counter[tag] + inc : inc; return counter; };
    const counter = {};
    const localClient = new UndiciCH(confCH.uri, confCH.credentials, { connections: 1, name: 'localClient' });
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
    expect(sumOfEvents).toBe(4);
    expect(sumOfEventsTypes).toBe(3); // { Created: 1, Request: 2, Error: 1, Closed: 1 }
  });
});
