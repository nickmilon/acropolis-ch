/* eslint-disable object-curly-newline */
/* eslint-disable camelcase */
/* eslint-disable function-paren-newline */
/* eslint-disable comma-dangle */
/* eslint-disable no-await-in-loop */
/* eslint-disable no-bitwise */
/* eslint-disable no-underscore-dangle */
/* eslint-disable no-undef */

import { describe, it, before, after } from 'node:test';
import { strict as assert } from 'node:assert';

import { setTimeout as setTimeoutAsync } from 'timers/promises';
import { PageScroll } from 'acropolis-nd/lib/Euclid.js';
import { ConLog, consolDummy } from 'acropolis-nd/lib/scripts/nodeOnly.js';
import { Graph } from '../lib/solutions/graph.js';
import { CHclient, flagsCH } from '../lib/client.js';
import { confCH, runOptions } from '../acropolis-ch-conf.js';
import { sqlPrettify } from '../lib/sql/fragments.js';
import { formatStr } from '../lib/sql/varsCH/formats.js';



class ScrollOnColumn extends PageScroll {
  // expects data in JSONCompact format
  constructor(column, prefix = '',
    {
      fnNext = (row) => `${prefix} (${column} > ${row[0]})`,
      fnPrev = (row) => `${prefix} (${column} < ${row[0]})`,
      vectorDefault = 1000,
    } = {},
  ) {
    super({ fnNext, fnPrev, vectorDefault });
  }
}

const prettyLog = (data, comments) => `----- ${comments}-----\n${data}`;


const logLevel = runOptions?.tests?.logLevel || 'log';

const logger = (logLevel === 'silent') ? consolDummy : new ConLog('debug', { inclTS: true, inspectDefaults:{colors: false} });


// eslint-disable-next-line no-console
console.log(`set logLevel variable in config.js in one of available Levels: ${ConLog.availableLevelsStr()}`);
 
describe('sql statements', () => {
  let client;
  let graph;
  let result;
  let sqlStr;
  const sqlExec = async (fn) => {
    result = await fn;
    //del expect(result.statusCode).toBe(200);
    assert.equal(result.statusCode, 200 )
    return result;
  };
   
  const sqlLog = (str) => `-----\n[${sqlPrettify(str)}]\n----`;
  const req = async (sql, bodyData = '', statusCodeExpected = 200) => {
    logger.log(sqlLog(sql));
    result = await client.request(sql, bodyData, { flags: flagsCH.flagsToNum(['resolve']) });
    if (result.statusCode !== statusCodeExpected) {
      logger.inspectIt({ sql, bodyData, result, statusCodeExpected }, 'req');
    }
    //del expect(result.statusCode).toBe(statusCodeExpected);
    assert.equal(result.statusCode, statusCodeExpected )
    return result;
  };

  const check = async (sql, numOfRows, firstNum) => {
    result = await req(sql);
    const { body } = result;
    body.data = body.data.flat();
    if (numOfRows !== undefined) {
      //DEL expect(body.rows).toBe(numOfRows);
      assert.equal(body.rows, numOfRows)
      //del expect(body.data.length).toBe(numOfRows);
      assert.equal(body.data.length, numOfRows)
    }
    if (firstNum !== undefined) { assert.equal(body.data[0], firstNum); }
    return result;
  };

  before(async () => {
    client = new CHclient(confCH.uri, confCH.credentials, { connections: 10 });
    client.defaultFlags = ['resolve', 'throwClient', 'throwNon200'];
    graph = new Graph(client);
  });

  after(async () => {
    // await req(DROP_DATABASE(testDb));
    await setTimeoutAsync(100);
    await client.close();
    await setTimeoutAsync(100); // give it some time if prints are pending
  });
  


  it('test1', async () => {  // 'graph_and_pagination' 
    const scrollP = new ScrollOnColumn('parent', 'AND');
    // const scrollC = new ScrollOnColumn('child', 'AND');
    let pg = {};
    const child = 106186; // a child that exists on sample;
    result = await client.request('show databases FORMAT JSONCompactColumns');
    if (! result.body[0].includes('twitter') ) {
      logger.log('twitter sample db is missing - check documentation on how to install it - skipping test')
      return
    }
    result = await graph.parents(child, 2, '', { type: 1, FORMAT: formatStr.Pretty });
    // console.log(result)
    // expect(result.body.length).toBeGreaterThan(0);  // found child #
      
    assert.ok(result.body.length > 0, "messXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    // console.dir(result)
    
     
    
   //  logger.log(prettyLog(result.body, 'first 2 rows'));
    
    let vector = 10; // just a demo
    
    result = await graph.parents(child, vector, '', { type:1, FORMAT: formatStr.JSONCompact });
    // logger.dir({ body: result.body });
  
    pg = await scrollP.getPageObj(result.body.data, pg, vector);
    // logger.inspectIt({ body: result.body, pg }, 'scrolling ');
    
    const res10 = result.body.data.flat();
     
    vector = 1;
    pg = {};
    for (let index = 0; index < 10; index += 1) {  // scroll from beginning one row at a time
      result = await graph.parents(child, vector, pg.next, { type: 1, FORMAT: formatStr.Pretty })
      // logger.log(prettyLog(result.body,  `index:${index}`));
      result = await graph.parents(child, vector, pg.next, { type: 1, FORMAT: formatStr.JSONCompact }); // first row will default to undefined class will handle it
      // expect(result.body.data[0][0]).toBe(res10[index]);
      assert.equal(result.body.data[0][0], res10[index])
      pg = await scrollP.getPageObj(result.body.data, pg, vector);
    }
  });
 
});

 
