/* eslint-disable object-curly-newline */
/* eslint-disable camelcase */
/* eslint-disable function-paren-newline */
/* eslint-disable comma-dangle */
/* eslint-disable no-await-in-loop */
/* eslint-disable no-bitwise */
/* eslint-disable no-underscore-dangle */
/* eslint-disable no-undef */

import { setTimeout as setTimeoutAsync } from 'timers/promises';
import { finished, pipeline } from 'stream/promises';
import { ConLog, consolDummy } from '../../acropolis-nd/lib/scripts/nodeOnly.js';
import { UndiciCH, flagsCH } from '../lib/client.js';
import { confCH, runOptions } from '../config.js';
import { sqlPrettify } from '../lib/sql/fragments.js';
import { limitOffset, SELECT, SELECTraw } from '../lib/sql/select.js';
import { createContext } from '../lib/context.js';
import { formatStr } from '../lib/sql/varsCH/formats.js';
import { JSONstringifyCustom, toValuesStr, toColumnNamesStr, createParserFromMeta, resultsParse, columnsFromStructStr, rxDefaultRowMach } from '../lib/helpers/transforms.js';
import { PageScrollExample, scrollSelect } from '../lib/solutions/pagination.js';
import { ReadableArrOrFn, TransformParseRaw } from '../lib/helpers/streams.js';
import * as auxillary from '../lib/sql/auxillary.js';
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

import { sqlTests, engine, simpleStructAll, simpleStructAllNullable, mostTypesStruct } from '../lib/structures/tests.js';
import { on } from 'events';
 
 



const logLevel = runOptions?.tests?.logLevel || 'log';

const logger = (logLevel === 'silent') ? consolDummy : new ConLog('debug', { inclTS: true });

// eslint-disable-next-line no-console
console.log(`set logLevel variable in config.js in one of available Levels: ${ConLog.availableLevelsStr()}`);

describe('sql statements', () => {
  let client;
  let result;
  let sqlStr;
  let data;
  const testDb = 'testCH';
  const NSnumbers = `${testDb}.numbers`;
  const NSobjRndFlat = `${testDb}.objRndFlat`;
  const sumOfNaturalNumbers = (n) => (n * (n + 1)) / 2;
  const sqlExec = async (fn) => {
    result = await fn;
    // inspectIt({ result }, logger, sql, { breakLength: 140 });
    expect(result.statusCode).toBe(200);
    return result;
  };
  // client.post(query, sqlStr, expStatus = 200);
  const sqlLog = (str) => `-----\n[${sqlPrettify(str)}]\n----`;
  const req = async (sql, bodyData = '', statusCodeExpected = 200) => {
    logger.log(sqlLog(sql));
    result = await client.request(sql, bodyData, { flags: flagsCH.flagsToNum(['resolve']) });
    if (result.statusCode !== statusCodeExpected) {
      logger.inspectIt({ sql, bodyData, result, statusCodeExpected }, 'req');
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
    client = new UndiciCH(confCH.uri, confCH.credentials, { connections: 10 });
    await req(CREATE_DATABASE(testDb));
  });

  afterAll(async () => {
    // await req(DROP_DATABASE(testDb));
    await setTimeoutAsync(100);
    await client.close();
    await setTimeoutAsync(100); // give it some time if prints are pending
  });

  it('test1', async () => {
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
      if (resultsOut.statusCode !== 200) { logger.dir({ resultsOut })}
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
    // exclude JSONStringsEachRow JSONCompactStringsEachRow  as don't work with randomStrings
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
    sqlStr = CREATE_TABLE_fromSelect(testDb, tbName, '(id Int32, str String)', engine, `number as id, 'foo' as str FROM numbers(1, ${recCnt})`);
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

  it('typeConversions', async () => {
    let context;
    let values = 1;
    const aDate = new Date('2021-11-01T02:03:04.200Z');
    const dataJS = {
      // id: 1, UInt8: 2 ** 7, UInt64: BigInt(2n ** 63n).toString()
      id: 1,
      Bool: true,
      UInt8: (2 ** 8) - 1,                            // [0 : 255]
      UInt16: (2 ** 16) - 1,                          // [0 : 65535]
      UInt32: (2 ** 32) - 1,                          // [0 : 4294967295]
      UInt64: (BigInt(2n ** 64n) - 1n),               // [0 : 18446744073709551615]
      UInt128: (BigInt(2n ** 128n) - 1n),             // [0 : 340282366920938463463374607431768211455]
      UInt256: (BigInt(2n ** 256n) - 1n),             // [0 : 115792089237316195423570985008687907853269984665640564039457584007913129639935]
      Int8: -1 * (2 ** 7),                            // [-128 : 127]
      Int16: -1 * (2 ** 15),                          // [-32768 : 32767]
      Int32: -1 * (2 ** 31),                          // [-2147483648 : 2147483647]
      Int64: (-1n * BigInt(2n ** 63n)),               // [-9223372036854775808 : 9223372036854775807]
      Int128: (BigInt(2n ** 127n) - 1n),              // [-170141183460469231731687303715884105728 : 170141183460469231731687303715884105727]
      Int256: (BigInt(2n ** 255n) - 1n),              // [-57896044618658097711785492504343953926634992332820282019728792003956564819968 ]
      Float32: Math.PI,
      Float64: Math.PI,
      String: 'foo',
      UUID: '61f0c404-5cb3-11e7-907b-a6006ad3dba0',
      // Date: aDate,    // will not work with Json
      // Date32: aDate,  // will not work with values and Json
      DateTime: aDate,
      DateTime64: aDate,
      IPv4: '192.168.0.255',
      IPv6: '2a02:e980:1e::1',
    };
    // ------------------------------------------------------------------------------------------------------ types DESCRIBE TABLE
    context = createContext(client, { chOpts: {}, flags: flagsCH.mapFlgFI.resolve });
    // result = await context.tableStructure(testDbName, 'allTypes');
    await context.DROP_TABLE(testDb, 'allTypes');
    result = await context.CREATE_TABLE_fromSchema(testDb, 'allTypes', mostTypesStruct, engine);
    expect(result.statusCode).toBe(200);
    // result = await context.DESC(testDbName, 'allTypes', { FORMAT: 'JSON' });
    // -----------------------------------------------------------json
    data = JSONstringifyCustom(dataJS);
    logger.inspectIt({ data }, 'data json');
    result = await context.INSERT_INTO(testDb, 'allTypes', data, { FORMAT: formatStr.JSONEachRow });
    logger.inspectIt({ result }, 'result insert JSONEachRow');
    expect(result.statusCode).toBe(200);
    result = await context.SELECT('*', { FROM: 'testCH.allTypes', WHERE: 'id = 1', LIMIT: '10', FORMAT: formatStr.JSON });
    logger.inspectIt({ result }, 'SELECT, JSONEachRow');
    expect(result.body.data[0].Bool).toBe(dataJS.Bool | 0);
    // ----------------------------------------------  Values
    result = await context.TRUNCATE_TABLE(testDb, 'allTypes');
    const columns = toColumnNamesStr(dataJS);
    values = toValuesStr(dataJS);
    values = `${values}\n`.repeat(1);
    logger.inspectIt({ columns, values }, ' columns - values');
    result = await context.INSERT_INTO(testDb, 'allTypes', `${values}`, { columns });
    logger.inspectIt({ result }, 'INSERT_INTO Values');
    result = await context.SELECT('*', { FROM: 'testCH.allTypes', WHERE: 'id = 1', LIMIT: '10', FORMAT: formatStr.JSONCompact });
    const parser = createParserFromMeta(result.body.meta);
    result = parser(result.body.data);
    expect(result[0].UInt256).toBe(dataJS.UInt256); // minimal check
    logger.inspectIt({ result }, 'parse');
    result = await context.SELECT('*', { FROM: 'testCH.allTypes', WHERE: 'id = 1', LIMIT: '10', FORMAT: formatStr.JSON });
    await resultsParse(result);
    expect(result.body.data[0].UInt128).toBe(dataJS.UInt128); // minimal check
    logger.inspectIt({ result }, 'parse Values');
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
    ENGINE = MergeTree()
    ORDER BY (id, str)
    SETTINGS index_granularity = 8192 
    `;
    const engine2 = `
    ENGINE = MergeTree()
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
    await req(CREATE_TABLE_fromSchema(dbTstCreate, tb, schema, engine1), '', 200);
    await req(CREATE_TABLE_fromTb(dbTstCreate, tb2, dbTstCreate, tb, { IF_NOT_EXISTS: true, engine: engine1 }), '', 200);

    await req(CREATE_TABLE_fromTb(dbTstCreate, tb2, dbTstCreate, tb, { IF_NOT_EXISTS: false, engine: engine1 }), '', 500); // 'Code: 57. table already exists

    await req(DROP_TABLE(dbTstCreate, tb2, { TEMPORARY: false, IF_EXISTS: false }), '', 200);
    await req(CREATE_TABLE_fromTb(dbTstCreate, tb2, dbTstCreate, tb, { IF_NOT_EXISTS: false, engine: engine2 }), '', 200); // different engine
    // // CREATE TABLE t1 (x String) ENGINE = Memory AS SELECT 1
    // export const CREATE_TABLE_fromSelect = (dbName, tableName, schema, engine, SELECT, { IF_NOT_EXISTS = true } = {})
    await req(DROP_TABLE(undefined, 't11', { IF_EXISTS: true }), '', 200);
    await req(CREATE_TABLE_fromSelect(undefined, 't11', schema2, 'ENGINE = Memory', 1, { IF_NOT_EXISTS: true }), '', 200);
    await req(CREATE_TABLE_fromSelect(undefined, 't11', schema2, 'ENGINE = Memory', 1, { IF_NOT_EXISTS: false }), '', 500); // 'Code: 57. DB: exists

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
    await req(CREATE_TABLE_fromSelect(undefined, 'testSelect', '(n UInt32)', 'ENGINE = Memory', 'toUInt32(number) AS n FROM numbers(1, 10)'));
    await check(SELECT('*', { FROM: 'testSelect', FORMAT: formatStr.JSONCompact }), 10);
    await req(TRUNCATE_TABLE(undefined, 'testSelect'));
    await check(SELECT('*', { FROM: 'testSelect', FORMAT: formatStr.JSONCompact }), 0);
    result = await req(EXISTS('TABLE', undefined, 'testSelect', { FORMAT: formatStr.JSONCompactEachRow })); expect(result.body[0]).toBe(1);
    logger.inspectIt({ result });
    result = await req(EXISTS('TABLE', undefined, 'testSelectXXXX123')); expect(result.body).toBe('0\n');
    result = await req(SHOW_CREATE('TABLE', undefined, 'testSelect', { FORMAT: undefined}));
    // logger.log(`\n${result.body}`)
    // result = await req(select('*', { FROM: 'testSelect', FORMAT: formatStr.JSONCompact }));
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
  
  it('loop', () => {
    return
    // const tstObj = { SELECT: 1, f1: 1, f2: 2, f3: 3};
    const tstObj = { SELECT: 'foo'};
    const loops = 10000000;
    const IfValueK = (obj) => Object.entries(obj).map(([k, v]) => ((v) ? k.replaceAll('_', ' ') : '')).join(' ');
    const IfValueK1 = (obj) => { const [k, v] = Object.entries(obj)[0]; return ((v) ? k.replaceAll('_', ' ') : ''); };
    logger.log([IfValueK(tstObj), IfValueK1(tstObj)]);
     
    //
    logger.time('k1'); //-----------------------------------------------
    for (let idx = 0; idx < loops; idx += 1) {
      IfValueK(tstObj);
    }
    logger.timeEnd('k1'); //--------------------------------------------
  });
});
