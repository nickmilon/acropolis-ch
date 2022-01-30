/* eslint-disable no-param-reassign */
/* eslint-disable guard-for-in */
/* eslint-disable no-restricted-syntax */
/* eslint-disable brace-style */
/* eslint-disable object-curly-newline */
/* eslint-disable camelcase */
/* eslint-disable function-paren-newline */
/* eslint-disable comma-dangle */
/* eslint-disable no-await-in-loop */
/* eslint-disable no-bitwise */
/* eslint-disable no-underscore-dangle */
/* eslint-disable no-undef */

import { setTimeout as setTimeoutAsync } from 'timers/promises';
import { setInterval } from 'timers';
import { pipeline } from 'stream/promises';
import { ConLog, consolDummy } from 'acropolis-nd/lib/scripts/nodeOnly.js';
import * as Pythagoras from 'acropolis-nd/lib/Pythagoras.js';
import { CHclient, flagsCH } from '../lib/client.js';
import { confCH, runOptions } from '../config.js';
import { sqlPrettify } from '../lib/sql/fragments.js';
import { limitOffset, SELECT, SELECTraw } from '../lib/sql/select.js';
import { createContext } from '../lib/context.js';
import { formatStr } from '../lib/sql/varsCH/formats.js';
import { castTransform, castData, castResponse, columnsFromStructStr, rxDefaultRowMach } from '../lib/helpers/transforms.js';
import { PageScrollExample, scrollSelect } from '../lib/solutions/pagination.js';
import { ReadableArrOrFn, TransformParseRaw } from '../lib/helpers/streams.js';
import { settingsCH } from '../lib/sql/varsCH/settings.js';
import {
  DROP_TABLE,
  TRUNCATE_TABLE,
  SHOW_CREATE,
  EXISTS,
  CREATE_DATABASE,
  DROP_DATABASE,
  CREATE_TABLE_fromSchema,
  CREATE_TABLE_fromTb,
  CREATE_TABLE_fromSelect,
  ALTER_TABLE_DELETE,
  ALTER_TABLE_UPDATE,
  OPTIMIZE_TABLE,
  INSERT_INTO,
} from '../lib/sql/basic.js';

import { engine } from '../lib/structures/tests.js';
import { coreTypes } from '../lib/sql/varsCH/types.js';
import { isFloat32Array } from 'util/types';

const logLevel = runOptions?.tests?.logLevel || 'log';

const logger = (logLevel === 'silent') ? consolDummy : new ConLog(logLevel, { inclTS: true });

// eslint-disable-next-line no-console
console.log(`set logLevel variable in config.js in one of available Levels: ${ConLog.availableLevelsStr()}`);

describe('sql statements', () => {
  let client;
  let result;
  let sqlStr;
  let data;
  const testDb = 'testCH';
  const sumOfNaturalNumbers = (n) => (n * (n + 1)) / 2;
  // client.post(query, sqlStr, expStatus = 200);
  const sqlLog = (str) => `-----\n[${sqlPrettify(str)}]\n----`;
  const req = async (sql, bodyData = '', statusCodeExpected = 200) => {
    logger.log(sqlLog(sql));
    result = await client.request(sql, bodyData, { flags: flagsCH.flagsToNum(['resolve']) });
    if (result.statusCode !== statusCodeExpected) {
      logger.inspectIt({ bodyData, result, statusCodeExpected }, 'req');
      logger.log(sql);
    }
    expect(result.statusCode).toBe(statusCodeExpected);
    return result;
  };

  const check = async (sql, numOfRows, firstNum) => {
    result = await req(sql);
    const { body } = result;
    body.data = body.data.flat();
    if (numOfRows !== undefined) {
      expect(body.rows).toBe(numOfRows);
      expect(body.data.length).toBe(numOfRows);
    }
    if (firstNum !== undefined) { expect(body.data[0]).toBe(firstNum); }
    return result;
  };

  beforeAll(async () => {
    client = new CHclient(confCH.uri, confCH.credentials, { connections: 10 });
    // client.defaultFlags = ['resolve', 'throwClient', 'throwNon200']; 
    await req(CREATE_DATABASE(testDb));
  });

  afterAll(async () => {
    // await req(DROP_DATABASE(testDb));
    await setTimeoutAsync(200);
    await client.close();
    await setTimeoutAsync(200); // give it some time if prints are pending
  });

  it('Pipe Etc', async () => {
    const nsTable1 = `${testDb}.Stream1`;
    const nsTable2 = `${testDb}.Stream2`;
    const rowsCount = 100000;
    const struct = `(id UInt32, str String, strRnd String) 
                    ENGINE = MergeTree() ORDER BY (id, str) SETTINGS index_granularity = 8192`;
    const columns = columnsFromStructStr(struct);
    const checkCRCs = (ns) => `
      SELECT
      sum(row) AS sumRow,
      sum(crc) AS sumCRC,
      sum(id) AS sumID
        FROM
        (
            SELECT
                1 as row,
                crc32(strRnd) AS crc,
                id
            FROM ${ns}
        ) FORMAT JSONEachRow`;

    const checkStringMismatch = (ns1, ns2) => `
      SELECT *
      FROM
      (
        SELECT
            id,
            strRnd,
            Stream2.id AS id2,
            Stream2.strRnd AS strRnd2
        FROM ${ns1} AS Stream1
        INNER JOIN ${ns2} AS Stream2 ON Stream1.id = Stream2.id
      )
      WHERE strRnd != strRnd2
      LIMIT 10 FORMAT PrettyCompactMonoBlock`;

    const dataFnClosure = (count) => {
      let cntCurrent = 0;
      const dataFn = () => {
        // eslint-disable-next-line no-cond-assign
        if ((cntCurrent += 1) <= count) {
          const values = `(${cntCurrent}, 'the quick brown fox', randomPrintableASCII(80))\n`; // randomStringUTF8
          // const values = `(${cntCurrent}, 'the quick brown fox', '012345678901234567890012345678901234567890')\n`
          return values;
        }
        return null;
      };
      return dataFn;
    };

    /** ---------------------------------------------------------------------------------
    *
    *
    * @param {string} [format='JSONEachRow'] CH format
    * @return {object} counter
    */
    const scanStream = async (format = 'JSONEachRow') => {
      const funcTrx = async (row) => {
        if (format === 'JSONEachRow') {
          const rowTestTransform = JSON.parse(row);
          rowTestTransform.str += ' transformed';
          return JSON.stringify(rowTestTransform);
        }
        return row;
      };
      await req(TRUNCATE_TABLE(undefined, nsTable2));
      const transformStream = new TransformParseRaw(format, { funcTrx });
      // ------------
      const sqlArr = INSERT_INTO(undefined, `${nsTable2}`, transformStream, { columns, FORMAT: format });
      let resultsOut = client.request(...sqlArr); // @IMPORTANT no wait here otherwise will hung forever
      // ------------
      sqlStr = `SELECT * FROM ${nsTable1} ORDER BY id ASC FORMAT ${format}`;
      const resultReader = await client.request(sqlStr, '', { flags: 0 });
      await pipeline(resultReader.body, transformStream); // process.stdout)
      resultsOut = await resultsOut; // if we want to check results do an await here after stream ends;
      if (resultsOut.statusCode !== 200) { logger.dir({ resultsOut });}
      expect(resultsOut.statusCode).toBe(200);
      return { counters: transformStream.counters };
    };
    // ----------------------------------------------------------------------------------------
    await req(DROP_TABLE(undefined, nsTable1));
    await req(DROP_TABLE(undefined, nsTable2));
    await req(CREATE_TABLE_fromSchema(undefined, nsTable1, struct));
    await req(CREATE_TABLE_fromSchema(undefined, nsTable2, struct));

    // --------------------------------------------------------------- test ReadableArrOrFn with an array
    const arr = Array(10).fill(`(${999}, 'the quick brown fox', randomPrintableASCII(20))\n`);
    result = await client.request(...INSERT_INTO(undefined, nsTable1, new ReadableArrOrFn(arr), { FORMAT: 'Values', columns }));
    expect(result.statusCode).toBe(200);
    await req(TRUNCATE_TABLE(undefined, nsTable1));
    // ---------------------------------------------------------------
    const readStream = new ReadableArrOrFn(dataFnClosure(rowsCount));
    const sqlArr = INSERT_INTO(undefined, nsTable1, readStream, { FORMAT: 'Values', columns }); // !Values is the only format we can insert functions
    logger.time(`INSERT_INTO rows: ${rowsCount}`);
    result = await client.request(...sqlArr);
    expect(result.statusCode).toBe(200);
    logger.timeEnd(`INSERT_INTO rows: ${rowsCount}`);
    const chOpts = settingsCH.output_format_json_quote_64bit_integers(0); // BTW check if chOpts are working
    result = await client.request(SELECT('sum(id) AS sumNatNum', { FROM: nsTable1, FORMAT: formatStr.JSONEachRow }), '', { chOpts });
    expect(result.statusCode).toBe(200);
    expect(result.body.sumNatNum).toBe(sumOfNaturalNumbers(rowsCount)); // like checksum;
    result = await client.request(checkCRCs(nsTable1));
    expect(result.statusCode).toBe(200);
    const crcObj1 = result.body;  // save it to compare later with table2

    const parserFormatsAvailable = rxDefaultRowMach(undefined, true).filter((x) => (!x.includes('Strings')));
    // eslint-disable-next-line no-restricted-syntax
    for (const format of parserFormatsAvailable) {
      const msg = `scanStream ${format.padEnd(20)} rows: ${rowsCount}`;
      // logger.log(`${msg}`);
      logger.time(msg);
      await scanStream(format);
      logger.timeEnd(msg);
      // await setTimeoutAsync(100);  // let it stabilze counters etc //
      // expect(counters.rows - rowsCount).toBe(rowsCount ); // we outputted all rows
      result = await client.request(checkCRCs(nsTable2)); // check crc etc to make sure we have copied properly
      expect(result.statusCode).toBe(200);
      const crcObj2 = result.body;
      expect(crcObj1.sumRow).toBe(crcObj2.sumRow);
      expect(crcObj1.sumID1).toBe(crcObj2.sumID1);
      if (crcObj1.sumCRC !== crcObj2.sumCRC) {
        result = await client.request(checkStringMismatch(nsTable1, nsTable2));
        logger.warn(`${msg} following (or more) rows have mismatches\n${result.body}\n`);
      }
    }
    // logger.inspectIt({ result });
  });

  it('scrolling-paging', async () => {
    // will do a scroll down till end of data set with page size 5 then reverse and scroll up with page size 6 till the beginning (position -1)
    let scrollVector;
    let pg = {};
    const scrollInstance = new PageScrollExample();
    const counter = {}; // a dirty counter
    let where;
    let pageNumber = 0;
    scrollVector = 5; // records per page;
    pg = {};
    do {
      result = await req(scrollSelect(where, scrollVector));
      data = result.body.data;
      pg = await scrollInstance.getPageObj(data, pg, scrollVector);
      logger.inspectIt({ pageNumber, data, pg }, 'scrolling ');
      data.forEach((record) => { counter[record.id] = counter[record.id] ? counter[record.id] + 1 : 1; }); // just for checking
      if (data.length > 0 && Math.sign(scrollVector === 1)) { // check only while forward
        expect(data[0].pageNumber).toBe(pageNumber);
      }
      if (pg.position === 1) { scrollVector = -6; }  // reached the end lets reverse course with page size 6 this time
      pageNumber += Math.sign(scrollVector);
      where = (Math.sign(scrollVector) === 1) ? pg.next : pg.prev;
    } while (pg.position !== -1); // till we reach start approaching from end
    expect(Object.keys(counter).length).toBe(20); // we visited all records at least once (actually 2 except last page forward)
  });

  it('mutations', async () => {
    const recCnt = 1000000;
    const tbName = 'mutations';
    await req(DROP_TABLE(testDb, tbName));
    sqlStr = CREATE_TABLE_fromSelect(testDb, tbName, '(id Int32, str String)', `number as id, 'foo' as str FROM numbers(1, ${recCnt})`, { ENGINE: engine });
    await req(sqlStr);
    result = await check(SELECT('COUNT(*)', { FROM: `${testDb}.${tbName}`, WHERE: 'id >= 1', FORMAT: formatStr.JSONCompact }), 1, recCnt.toString());
    sqlStr = ALTER_TABLE_DELETE(testDb, tbName, 'id > 100000'); // 900000 delete mutations left with 10000 records
    await req(sqlStr);
    await req(OPTIMIZE_TABLE(testDb, tbName, { FINAL: true }));
    result = await check(SELECT('COUNT(*)', { FROM: `${testDb}.${tbName}`, WHERE: 'id >= 1', FORMAT: formatStr.JSONCompact }), 1, '100000');
    sqlStr = ALTER_TABLE_UPDATE(testDb, tbName, { str: '\'bar\'' }, 'id > 50000'); // 50000 records
    await req(sqlStr);
    await setTimeoutAsync(500); // lets wait for async mutations instead of forcing OPTIMIZE table;
    result = await check(SELECT('COUNT(*)', { FROM: `${testDb}.${tbName}`, WHERE: 'str = \'bar\'', FORMAT: formatStr.JSONCompact }), 1, '50000');
  });

  it('test1', async () => { // typeConversions
    const allTypesNS = `${testDb}.allTypes`;
    let response;
    const coreTypesStruct = () => {
      // CH returns date strings according to timezone;
      const convertToUTC = (col) => {
        if (col === 'DateTime') { return `${col}('UTC')`; }
        if (col === 'DateTime64') { return `${col}(3, 'UTC')`; }
        return col;
      };
      const schema = Object.keys(coreTypes).map((k) => [k, `${convertToUTC(k)}`]);
      // schema = [['id', 'UInt32'], ...schema];
      return `(${schema.map(([k, v]) => `\ncol_${k} ${v}`)})`;
    };

    const dataFromCoreTypes = (maxOrMin = 'max', asObj = true) => {
      const rt = Object.entries(coreTypes).map(([k, v]) => [`col_${k}`, v[maxOrMin]]);
      if (asObj === true) { return Object.fromEntries(rt); }
      return rt.map(([, v]) => v); // values only
    };

    const roughlyEqual = (source, target) => {
      let eq = false;
      if (source === null || target === null) { eq = (source || 0) === (target || 0); }
      else if (Array.isArray(source)) { eq = source.every((val, idx) => roughlyEqual(val, target[idx])); }
      else if (Pythagoras.isDate(source)) { eq = (source.getTime() === target.getTime()); }
      else if (Pythagoras.isNumberFloat(source)) { eq = (source.toPrecision(4) === target.toPrecision(4)); } // allow for rounding errors
      else if (typeof source === 'bigint') { eq = (source === target); }
      else { eq = (source === target); }
      if (eq !== true) { logger.inspectIt({ eq, source, target }); }
      return eq;
    };

    const roughlyEqualObj = (sourceArr, targetArr) => {
      const eqArr = sourceArr.map((val, idx) => roughlyEqual(val, targetArr[idx]));
      return eqArr.every((el) => el === true);
    };
    // -----------------------------------------------------------------------------------------types DESCRIBE TABLE
    const context = createContext(client, { chOpts: {}, flags: flagsCH.mapFlgFI.resolve });
    await context.DROP_TABLE(testDb, 'allTypes');
    result = await context.CREATE_TABLE_fromSchema(testDb, 'allTypes', coreTypesStruct(), { ENGINE: 'MergeTree ORDER BY col_Int8' });
    expect(result.statusCode).toBe(200);
    let maxMin;
    const maxMinArr = ['min', 'max'];
    const supportedFormats = ['JSON', 'JSONCompact', 'JSONCompactEachRow'];
    // ------------------------------------------------------check edge cases (min max values for each type)
    for (const key in maxMinArr) {
      maxMin = maxMinArr[key];
      result = await client.request(`DESCRIBE TABLE (select * FROM ${allTypesNS} ) FORMAT JSON`);
      const contextTrx = castTransform(result);
      await context.TRUNCATE_TABLE(testDb, 'allTypes');
      const dataOriginal = dataFromCoreTypes(maxMin);
      castData(dataOriginal, contextTrx, 'fromJS');
      const dataJSON = JSON.stringify(dataOriginal);
      result = await context.INSERT_INTO(testDb, 'allTypes', dataJSON, { FORMAT: formatStr.JSONEachRow });
      if (result.statusCode !== 200) { logger.inspectIt(result, dataJSON, 'INSERT_INTO'); }
      expect(result.statusCode).toBe(200);
      for (const formatIdx in supportedFormats) {
        const FORMAT = supportedFormats[formatIdx];
        logger.debug(`checking ${maxMin} with format ${FORMAT}`);
        response = await context.SELECT('*', { FROM: `${allTypesNS}`, WHERE: undefined, LIMIT: '1', FORMAT });
        const ctx = (FORMAT === 'JSONCompactEachRow') ? contextTrx : undefined; // JSONCompactEachRow needs context
        castResponse(response, ctx);
        if (FORMAT === 'JSON') { data = Object.values(response.body.data[0]); }
        else if (FORMAT === 'JSONCompact') { [data] = response.body.data; }
        else if (FORMAT === 'JSONCompactEachRow') { data = response.body; }
        else throw Error(`unsupported format ${FORMAT}`);
        const source = dataFromCoreTypes(maxMin, false);
        const rEqual = roughlyEqualObj(source, data);
        if (!rEqual) { logger.inspectIt({ source, data, rEqual }, 'not roughly Equal'); }
        expect(rEqual).toBe(true);
      }
    }

    // ------------------------------------------------------check random;
    // we create random data cast it to js and back save it to db, retrieve it and compare with original
    // before Date32 support await req(`ALTER TABLE ${testDb}.allTypes DROP COLUMN col_Date32`); // Date32 doesn't support Random table  
    // prior to native bool support by CH await req(`ALTER TABLE ${testDb}.allTypes DROP COLUMN col_Bool`);  // can't compare with original
    await req(`ALTER TABLE ${testDb}.allTypes ADD COLUMN col_ArrInt6432 Array(Int64)`); // add an array column coz we don't have one
    await req(`ALTER TABLE ${testDb}.allTypes ADD COLUMN col_counter Int32`);
    await req(`DROP TABLE IF EXISTS ${testDb}.allTypesRand`);
    result = await context.CREATE_TABLE_fromTb(`${testDb}`, 'allTypesRand', `${testDb}`, 'allTypes', { ENGINE: 'GenerateRandom(4096)' });
    result = await req(`DESCRIBE TABLE (SELECT * FROM ${testDb}.allTypesRand) FORMAT ${formatStr.JSON}`);
    const contextTrxDes = castTransform(result);

    await context.TRUNCATE_TABLE(testDb, 'allTypes');
    response = await context.SELECT('*', { FROM: 'testCH.allTypesRand', WHERE: undefined, LIMIT: '100', FORMAT: formatStr.JSONCompact }); // max 127
    expect(response.statusCode).toBe(200);
    const originalData = response.body.data;
    // logger.inspectIt({ originalData }, 'originalData');
    originalData.forEach((x, idx) => { x[x.length - 1] = idx; }); // so we can use it as ORDER BY in select
    const originalDataCopy = JSON.parse(JSON.stringify(originalData)); // copy because cast will mutate in place
    logger.time('castResponse');
    castResponse(response);  // cast to JS
    castData(response.body.data, contextTrxDes, 'fromJS', formatStr.JSONCompact); // cast back to CH
    logger.timeEnd('castResponse');
    // logger.inspectIt({ response }, 'originalData');
    for (const idx in originalData) {
      result = await context.INSERT_INTO(testDb, 'allTypes', JSON.stringify(originalData[idx]), { FORMAT: formatStr.JSONCompactEachRow });
      if (result.statusCode !== 200) { logger.inspectIt(result, 'INSERT_INTO'); }
      expect(result.statusCode).toBe(200);
    }
    response = await context.SELECT('*', { FROM: `${allTypesNS}`, ORDER_BY: 'col_counter ASC', FORMAT: formatStr.JSONCompact });
    data = response.body.data;
    // logger.inspectIt({ originalDataCopy, data }, 'SELECT * after cast back fromJS ', 'trace');
    originalDataCopy.forEach((source, idx) => {
      const rEqual = roughlyEqualObj(source, data[idx]);
      if (!rEqual) { logger.inspectIt({ source, dataRecord: data[idx], rEqual }, 'not roughly Equal'); }
      expect(rEqual).toBe(true);
    });
  });

  it('CreateAndDrops', async () => {
    const dbTstCreate = 'dbTstCreate';
    const tb = 'ct_test1';
    const tb2 = 'ct_test2';
    const schema = `
    (
      id UInt32, 
      str  String
    )`;
    const engine1 = `
    MergeTree()
    ORDER BY (id, str)
    SETTINGS index_granularity = 8192 
    `;
    const engine2 = `
    MergeTree()
    ORDER BY (str, id)
    SETTINGS index_granularity = 4192 
    `;
    const schema2 = '(str String)';

    await req(DROP_DATABASE(dbTstCreate, { IF_EXISTS: true }), '', 200);
    await req(CREATE_DATABASE(dbTstCreate, { IF_EXISTS: false }), '', 200);
    await req(DROP_TABLE(dbTstCreate, tb, { TEMPORARY: false, IF_EXISTS: true }), '', 200);
    await req(DROP_TABLE(dbTstCreate, tb2, { TEMPORARY: false, IF_EXISTS: true }), '', 200);
    await req(DROP_TABLE(dbTstCreate, tb, { TEMPORARY: true, IF_EXISTS: false }), '', 404); // "Code: 60. DB::Exception: Temporary table xxx doesn't exist
    await req(CREATE_TABLE_fromSchema(dbTstCreate, tb, schema, ''), '', 400);  // empty engine 'Code: 62. DB::Exception: Syntax error
    await req(CREATE_TABLE_fromSchema(dbTstCreate, tb, schema, { ENGINE: engine1 }), '', 200);
    await req(CREATE_TABLE_fromTb(dbTstCreate, tb2, dbTstCreate, tb, { IF_NOT_EXISTS: true, ENGINE: engine1 }), '', 200);

    await req(CREATE_TABLE_fromTb(dbTstCreate, tb2, dbTstCreate, tb, { IF_NOT_EXISTS: false, ENGINE: engine1 }), '', 500); // 'Code: 57. table already exists

    await req(DROP_TABLE(dbTstCreate, tb2, { TEMPORARY: false, IF_EXISTS: false }), '', 200);
    await req(CREATE_TABLE_fromTb(dbTstCreate, tb2, dbTstCreate, tb, { IF_NOT_EXISTS: false, ENGINE: engine2 }), '', 200); // different engine

    // // CREATE TABLE t1 (x String) ENGINE = Memory AS SELECT 1
    // export const CREATE_TABLE_fromSelect = (dbName, tableName, schema, engine, SELECT, { IF_NOT_EXISTS = true } = {})
    await req(DROP_TABLE(undefined, 't11', { IF_EXISTS: true }), '', 200);
    sqlStr = CREATE_TABLE_fromSelect(undefined, 't11', schema2, 1, { ENGINE: 'Memory', IF_NOT_EXISTS: true });
    // console.dir({sqlStr})
    await req(CREATE_TABLE_fromSelect(undefined, 't11', schema2, 1, { ENGINE: 'Memory', IF_NOT_EXISTS: true }), '', 200);
    await req(CREATE_TABLE_fromSelect(undefined, 't11', schema2, 1, { ENGINE: 'Memory', IF_NOT_EXISTS: false }), '', 500); // 'Code: 57. DB: exists

    await req(DROP_TABLE(undefined, 't11', { IF_EXISTS: false }), '', 200);
    await req(DROP_DATABASE(dbTstCreate, { IF_EXISTS: 0 }), '', 200); // clean up
  });

  it('direct select-limitOffset', async () => {
    await check(`SELECT toUInt32(number) AS n FROM numbers(1, 100) ${limitOffset(10, 10)} FORMAT ${formatStr.JSONCompact}`, 10, 11);
    await check(`SELECT toUInt32(number) AS n FROM numbers(1, 100) ${limitOffset('10')} FORMAT ${formatStr.JSONCompact}`, 100, 1);
    await check(`SELECT toUInt32(number) AS n FROM numbers(1, 100) ${limitOffset(undefined, 50)} FORMAT ${formatStr.JSONCompact}`, 50, 51);
    await check(`SELECT * FROM (SELECT number%50 AS n FROM numbers(100)) ORDER BY n ${limitOffset(5, 0)} FORMAT JSONCompact`, 5, 0);
    await check(`SELECT * FROM (SELECT number%50 AS n FROM numbers(100)) ORDER BY n ${limitOffset(5, 0, true)} FORMAT JSONCompact`, 6, 0);
    await check(`SELECT * FROM (SELECT number%50 AS n FROM numbers(100)) ORDER BY n ${limitOffset(false, false, true)} FORMAT JSONCompact`, 100, 0);
    await check(SELECTraw(`* FROM (SELECT number%50 AS n FROM numbers(100)) ORDER BY n ${limitOffset(false, false, true)} FORMAT JSONCompact`), 100, 0);

    await req(DROP_TABLE(undefined, 'testSelect', { IF_EXISTS: true }));
    await req(CREATE_TABLE_fromSelect(undefined, 'testSelect', '(n UInt32)', 'toUInt32(number) AS n FROM numbers(1, 10)', { ENGINE: 'Memory' }));
    await check(SELECT('*', { FROM: 'testSelect', FORMAT: formatStr.JSONCompact }), 10);
    await req(TRUNCATE_TABLE(undefined, 'testSelect'));
    await check(SELECT('*', { FROM: 'testSelect', FORMAT: formatStr.JSONCompact }), 0);
    result = await req(EXISTS('TABLE', undefined, 'testSelect', { FORMAT: formatStr.JSONCompactEachRow })); expect(result.body[0]).toBe(1);
    logger.inspectIt({ result });
    result = await req(EXISTS('TABLE', undefined, 'testSelectXXXX123')); expect(result.body).toBe('0\n');
    result = await req(SHOW_CREATE('TABLE', undefined, 'testSelect', { FORMAT: undefined }));
    await req(DROP_TABLE(undefined, 'testSelect'));
  });

  it('Errors', async () => {
    result = await req('SELECT * FROM numbers(1, 2) FORMAT JSONEachRow', '', 555); // JSONEachRow can't be used with multi record result set
    result = await req('SELECT * FROM numbers(1, 1) FORMAT JSONEachRow', '', 200); // this is OK coz one record only
    // result = await client.post('', sqlStr, {}, flagsCH.flagsToNum(['resolve'])); // logger.inspectIt({result, sqlStr}); throwClient
  });

  it('context', async () => {
    let context;
    const tbContext1 = 'tbContext1';

    context = createContext(); // no client specified all calls will return sql statements
    sqlStr = await context.EXISTS('TABLE', testDb, tbContext1);
    expect(sqlStr.includes('EXIST')).toBeTruthy();  // it is a a string;
    result = await req(sqlStr);  // should return status 200;

    context = createContext(client, { chOpts: {}, flags: flagsCH.mapFlgFI.resolve });  // client and flags specified;
    result = await context.EXISTS('TABLE', testDb, tbContext1); // now we can call it to execute against the client
    expect(result.statusCode).toBe(200);
    result = await context.TRUNCATE_TABLE(testDb, tbContext1); // or execute directly from context
    expect(result.statusCode).toBe(200);
    expect(typeof result.body).toBe('string');  // we resolved body by specifying flag resolve;

    context = createContext(client, { chOpts: {}, flags: 0 }); // no flags
    result = await context.EXISTS('TABLE', testDb, tbContext1);
    expect(typeof result.body).toBe('object'); // coz we din't resolve;
    // const tbContext1 = 'tbContext1';
    context = createContext(client, { callback: (results) => results.statusCode, flags: flagsCH.flagsToNum(['resolve']) });
    result = await context.EXISTS('TABLE', testDb, tbContext1);
    expect(result).toBe(200); // callback returns only statusCode
  });

  /**
  * here we create a table and a live View from it, insert some stuff through an interval while monitoring data
  * as are coming from live view
  */
  it('Live View watcher', async () => {
    const lvSel = `${testDb}.lvSel`;
    const lv = `${testDb}.liveView`;
    await Promise.all([req(DROP_TABLE(undefined, lvSel)), req(`DROP VIEW IF EXISTS ${lv}`)]);
    await req(`CREATE TABLE  ${lvSel} (num UInt8, dtCreated DateTime) ENGINE = MergeTree ORDER BY dtCreated`);
    await req(`CREATE LIVE VIEW ${lv} AS SELECT num, toUInt32(sum(num)) as sum FROM  ${lvSel} GROUP by num`);

    const request = () => { client.request(`INSERT INTO ${lvSel} SELECT 1 as num, ${new Date().getTime() / 1000}`); };
    const interval = setInterval(request, 300);
    const flags = flagsCH.flagsToNum(['throwNon200', 'throwNon200']);
    const query_id = new Date().toISOString();
    const watcher = await client.post(`WATCH ${lv} FORMAT JSONEachRow`, undefined, { flags, chOpts: { query_id } }); // don't resolve

    // @important need to handle on error if we abort otherwise it will throw an Unhandled error as we hav already exited the request
    watcher.body.on('error', async (err) => {
      if (err.code === 'UND_ERR_ABORTED') { return; }
      throw err;
    });

    watcher.body.on('data', async (dataIn) => {
      const dataObj = JSON.parse(dataIn);
      logger.inspectIt({ dataObj }, 'live View data');
      if (dataObj.sum === 5) {
        clearInterval(interval);
        watcher.body.destroy(); // if we don't consume the full stream we MUST destroy it
        await client.request(`KILL QUERY WHERE query_id = '${query_id}'`, null, { flags }); // kill watch query;
      }
    });
    await setTimeoutAsync(3000); // give it some time
  });

  it('test111 ', async () => {
    return;
    // const abortController = new AbortController();
    const lvRnd = `${testDb}.lvRnd`;
    const lvSel = `${testDb}.lvSel`;
    const lv = `${testDb}.liveView`;
    const lvRndCrSql = `CREATE TABLE  ${lvRnd} (num UInt8, date Date) ENGINE = GenerateRandom()`;
    const lvSelCrSql = `CREATE TABLE  ${lvSel} (num UInt8, date Date) ENGINE = MergeTree ORDER BY num`;

    await Promise.all([req(DROP_TABLE(undefined, lvRnd)), req(DROP_TABLE(undefined, lvSel))]);
    await req(`DROP VIEW IF EXISTS ${lv}`);
    await Promise.all([req(lvRndCrSql), req(lvSelCrSql)]); // create 2 tables
    await req(`CREATE LIVE VIEW ${lv} AS SELECT num, count(num) as cnt FROM  ${lvSel} GROUP by num`);

    // result = await req(`WATCH ${lv}`);
    // JSONEachRow 'throwClient' flagsCH.flagsToNum(['throwNon200']);
    sqlStr = `INSERT INTO ${lvSel} SELECT * FROM ${lvRnd} LIMIT 1`;
    const request = () => { client.request(sqlStr); };
    const interval = setInterval(request, 300);
    const flags = flagsCH.flagsToNum(['throwNon200', 'throwNon200']);
    const watcher = await client.post(`WATCH ${lv} FORMAT JSONEachRow`, undefined, { flags }); // don't resolve
 
    await setTimeoutAsync(6000);
  });


});
