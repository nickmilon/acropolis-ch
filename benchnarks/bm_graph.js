/* eslint-disable no-param-reassign */
/* eslint-disable no-return-assign */
/* eslint-disable no-await-in-loop */

import { setTimeout as setTimeoutAsync } from 'timers/promises';
import { PageScroll } from 'acropolis-nd/lib/Euclid.js';
import { sumsCounter } from '../../acropolis-nd/lib/Solon.js';
import { objToSchema } from '../lib/helpers/transforms.js';
import { Graph } from '../lib/solutions/graph.js';
import { ConLog, consolDummy } from '../../acropolis-nd/lib/scripts/nodeOnly.js';
import { UndiciCH, flagsCH } from '../lib/client.js';
import { ClientExt } from '../lib/clientExt.js'; 
import { confCH, runOptions } from '../config.js';
import { sqlPrettify } from '../lib/sql/fragments.js';
import { limitOffset, SELECT, SELECTraw } from '../lib/sql/select.js';
import { createContext, contextStatementsSet, contextStatementsGet, contextStatementsAppend } from '../lib/context.js';
import { formatStr } from '../lib/sql/varsCH/formats.js';
import { JSONstringifyCustom, toValuesStr, toColumnNamesStr, createParserFromMeta, resultsParse, columnsFromStructStr, rxDefaultRowMach } from '../lib/helpers/transforms.js';
import { PageScrollExample, scrollSelect } from '../lib/solutions/pagination.js';
import { ReadableArrOrFn, TransformParseRaw } from '../lib/helpers/streams.js';
import * as auxillary from '../lib/sql/auxillary.js';
import { settingsCH } from '../lib/sql/varsCH/settings.js';
import {DROP_TABLE,
  TRUNCATE_TABLE,
  SHOW_CREATE,
  EXISTS,
  CREATE_DATABASE,
  DROP_DATABASE,
  CREATE_TABLE_fromSchema,
  ALTER_TABLE_DELETE,
  ALTER_TABLE_UPDATE,
  OPTIMIZE_TABLE,
  INSERT_INTO,
} from '../lib/sql/basic.js';

class ScrollOnColumn extends PageScroll {
  // expects data in JSONCompact format
  constructor(column, prefix = '',
    {
      fnNext = (row) => `${prefix} (${column} > ${row[0]})`,
      fnPrev = (row) => `${prefix} (${column} < ${row[0]})`,
      vectorDefault = 1000,
    } = {}) {
    super({ fnNext, fnPrev, vectorDefault });
  }
}

const prettyLog = (data, comments) => `----- ${comments}-----\n${data}`;



const createTableFromObj = (dbName, tbName, obj, engine) => {
 
  const CREATE_TABLE_fromSchema = (dbName, tbName, schema, engine = '', { IF_NOT_EXISTS = true, ON_CLUSTER } = {}) => `
  CREATE TABLE ${strIfK({ IF_NOT_EXISTS })} ${nameSpaceOrTb(dbName, tbName)} ${strIfKV({ ON_CLUSTER })}
  ${schema}
  ${engine}
`;

}

const logger = new ConLog('debug', { inclTS: true });
let result;
let sqlStr;

class GraphBM extends Graph {
  constructor(client, dbName = 'twitter', { tbEdges = 'edges', idType = 'UInt32', parentAlias = 'following', childAlias = 'followers' } = {}) {
    super(client, dbName, { idType, parentAlias, childAlias });
    this.ns.edges = `${dbName}.${tbEdges}`;
    this.client.defaultFlags = ['resolve', 'throwClient', 'throwNon200'];
  }

  async bmParents({ child = 23933989, vector = 100000 } = {}) {
    // BTW demonstrates scrolling usefulness as every new call has < elapsed time
    const scrollP = new ScrollOnColumn('parent', 'AND');
    const scrollC = new ScrollOnColumn('child', 'AND');
    let pg = {};
    let sumStats;
    logger.time('parents');
    // sqlStr = graph.gSql.parents(child, vector, pg.next);
    // console.log(sqlStr);
    do {
      // sqlStr = graph.gSql.parents(child, vector, pg.next);
      result = await this.parents(child, vector, pg.next, { FORMAT: formatStr.JSONCompact });
      const { data, rows, statistics } = result.body;
      sumStats = sumsCounter({ hops: 1, rows, ...statistics }, sumStats);
      // console.log(`hops:${hops} stats:${statistics.toString()}, sum${sumStats.toString()}`)
      pg = await scrollP.getPageObj(data, pg, vector);
      logger.info('stats', statistics);
    } while (pg.position === 0);
    logger.timeEnd('parents');
    return sumStats;
  }

  // eslint-disable-next-line class-methods-use-this
  

  async bmParallel(concurrency = 2, { child = 23933989, vector = 1000 } = {}) {
    const jobArr = Array(concurrency).fill(
      this.parents(child, vector, undefined, { FORMAT: formatStr.JSONCompact }),
    );
    logger.time('bmParallel1');
    result = await Promise.all(jobArr);
    logger.timeEnd('bmParallel1');

    result = result.map((r) => ({ ...{ rows: r.body.rows }, ...r.body.statistics }));
    // console.log(result)
    sqlStr = `
      SELECT
      sum(rows) as sumRows,
      min(elapsed) AS min,
      avg(elapsed) AS avg,
      max(elapsed) AS max,
      quantile(0.95)(elapsed) as q95,
      sum(rows_read) AS sumRows_read
      FROM del2
      FORMAT JSONEachRow 
    `;
    logger.time('memoryTableAndResults');
    result = await this.client.dirtyMemTable('del2', result, sqlStr);
    logger.timeEnd('memoryTableAndResults');
    return result.body;
  }

  async close() {
    // await req(DROP_DATABASE(testDb));
    await setTimeoutAsync(100);
    await this.client.close();
    await setTimeoutAsync(100);
  }
}

const graph = new GraphBM(new ClientExt(confCH.uri, confCH.credentials, { connections: 100 }), 'twitter', { tbEdges: 'edgesV6' });
export { graph };



/*
23934196
┌───parent─┬─children─┐
│ 23934132 │  2997469 │
│ 23934049 │  2679639 │
│ 23934048 │  2674874 │
│ 23934050 │  2450749 │
│ 23934199 │  1994926 │
│ 23934137 │  1959708 │
│ 23934177 │  1885782 │
│ 21513299 │  1882889 │
│ 23934172 │  1844499 │
│ 23932899 │  1843561 │
│ 23932071 │  1790771 │
│ 23934133 │  1691919 │
│ 23934051 │  1668193 │
│ 23932065 │  1657119 │
│ 23934060 │  1651207 │
│ 23934063 │  1524048 │
│ 23934079 │  1517067 │
│ 24893630 │  1477423 │
│ 23934131 │  1380160 │
│ 23934144 │  1377332 │
│ 23934900 │  1318909 │
│ 23932900 │  1318378 │
│ 23934025 │  1278103 │
│ 23934044 │  1277163 │
│ 24891383 │  1269341 │
│ 23932069 │  1241331 │
│ 23934142 │  1213787 │
│ 23934136 │  1210996 │
│ 23934180 │  1200472 │
│ 23934201 │  1195089 │
└──────────┴──────────┘
┌────child─┬─parents─┐
│ 21513299 │  770155 │
│ 23933989 │  505613 │
│ 23933986 │  498700 │
│ 23937213 │  407705 │
│ 23934048 │  406238 │
│ 23934131 │  369569 │
│ 23934073 │  283435 │
│ 23934123 │  263317 │
│ 23934033 │  143408 │
│ 21515005 │  140903 │
│ 21511313 │  138045 │
│ 23934069 │  134788 │
│ 21515742 │  123051 │
│ 23934128 │  119716 │
│ 21515941 │  119596 │
│ 21515707 │  119531 │
│ 23934127 │  119279 │
│ 21515805 │  115293 │
│ 21515809 │  110772 │
│ 21511314 │  109377 │
│ 21515803 │  109279 │
│ 21515782 │  108615 │
│ 21512481 │  107580 │
│ 21515771 │  106294 │
│ 21515997 │  105289 │
│ 21515804 │  102827 │
│ 21515985 │   99668 │
│ 21512480 │   99184 │
│ 21523985 │   99102 │
│ 21515684 │   97050 │
└──────────┴─────────┘

top Mutual
[
  23934048,
  21513299,
  23934131,
  23934128,
  23933986,
  23933989,
  23937213,
  23934127,
  23934073,
  23934123,
  23934069
]
{
  meta: [ { name: 'parent', type: 'UInt32' } ],
  dataX: [
    [ 322 ],  [ 854 ],
    [ 6314 ], [ 6591 ]
  ],
  rows: 10,
  rows_before_limit_at_least: 406238,
  statistics: { elapsed: 0.047172528, rows_read: 491520, bytes_read: 4423680 }
}
*/
