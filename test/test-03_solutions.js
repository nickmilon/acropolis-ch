/* eslint-disable object-curly-newline */
/* eslint-disable camelcase */
/* eslint-disable function-paren-newline */
/* eslint-disable comma-dangle */
/* eslint-disable no-await-in-loop */
/* eslint-disable no-bitwise */
/* eslint-disable no-underscore-dangle */
/* eslint-disable no-undef */

import { describe, it, before, after } from 'node:test';
import { strict } from 'node:assert';

import { setTimeout as setTimeoutAsync } from 'timers/promises';
import { PageScroll } from 'acropolis-nd/lib/Euclid.js';
import { ConLog, consolDummy, inspectIt } from 'acropolis-nd/lib/scripts/nodeOnly.js';
import { Graph } from '../lib/solutions/graph.js';
import { CHclient, flagsCH } from '../lib/client.js';
import { confCH } from '../acropolis-ch-conf.js';
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
 
const logger = (Object.keys(ConLog.levels).includes(process.argv.at(2) )) ? 
    new ConLog(process.argv[2], { inclTS: true, inspectDefaults: {colors: true} }) : consolDummy
 
describe('sql statements', { concurrency: true }, () => {
  let client;
  let graph;
  let result;
  let sqlStr;
  let data;
  const sqlExec = async (fn) => {
    result = await fn;
    strict.equal(result.statusCode, 200 )
    return result;
  };

  const sqlLog = (str) => `-----\n[${sqlPrettify(str)}]\n----`;
  const req = async (sql, bodyData = '', statusCodeExpected = 200) => {
    logger.log(sqlLog(sql));
    result = await client.request(sql, bodyData, { flags: flagsCH.flagsToNum(['resolve']) });
    if (result.statusCode !== statusCodeExpected) {
      logger.inspectIt({ sql, bodyData, result, statusCodeExpected }, 'req');
    }
    strict.equal(result.statusCode, statusCodeExpected )
    return result;
  };

  const check = async (sql, numOfRows, firstNum) => {
    result = await req(sql);
    const { body } = result;
    body.data = body.data.flat();
    if (numOfRows !== undefined) {
      strict.equal(body.rows, numOfRows)
      strict.equal(body.data.length, numOfRows)
    }
    if (firstNum !== undefined) { strict.equal(body.data[0], firstNum); }
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
   
  it('paging', async () => {  // 'graph_and_pagination' 
    const scrollP = new ScrollOnColumn('parent', 'AND');
    // const scrollC = new ScrollOnColumn('child', 'AND');
    let pg = {};
    const child = 106186; // a child that exists on sample;
    result = await client.request('show databases FORMAT JSONCompactColumns');
    if (! result.body[0].includes('twitter') ) {  // do we have twitter db ?
      logger.log('twitter sample db is missing - check documentation on how to install it - skipping test')
      return
    }
    result = await graph.parents(child, 2, '', { type: 1, FORMAT: formatStr.PrettyNoEscapes });      
    strict.ok(result.body.length > 0)  
    logger.log(prettyLog(result.body, 'first 2 rows'));
     
    let vector = 10; // just a demo
    result = await graph.parents(child, vector, '', { type:1, FORMAT: formatStr.JSONCompact });
    logger.dir({ body: result.body });
  
    pg = await scrollP.getPageObj(result.body.data, pg, vector);
    // logger.inspectIt({ body: result.body, pg }, 'scrolling ');
    
    const res10 = result.body.data.flat();
     
    vector = 1;
    pg = {};
    for (let index = 0; index < 10; index += 1) {  // scroll from beginning one row at a time
      result = await graph.parents(child, vector, pg.next, { type: 1, FORMAT: formatStr.PrettyNoEscapes })
      logger.log(prettyLog(result.body,  `index:${index}`));
      result = await graph.parents(child, vector, pg.next, { type: 1, FORMAT: formatStr.JSONCompact }); // first row will default to undefined class will handle it
      strict.equal(result.body.data[0][0], res10[index])
      pg = await scrollP.getPageObj(result.body.data, pg, vector);
    }
  });
 
});

 
