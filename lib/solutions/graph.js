/* eslint-disable camelcase */
/* eslint-disable no-await-in-loop */
/* eslint-disable no-bitwise */

/**
 * { @link https://snap.stanford.edu/data/twitter-2010.html twitter graph }
 */

import assert from 'assert';
import { homedir } from 'os';

import { setTimeout as setTimeoutAsync } from 'timers/promises';
import { finished, pipeline } from 'stream/promises';
import { PageScroll } from 'acropolis-nd/lib/Euclid.js';

import * as sql from '../sql/basic.js';
import { sqlPrettify } from '../sql/fragments.js';
import * as pyth from '/_code/node/acropolis-nd/lib/Pythagoras.js'
import { UndiciCH, flagsCH } from '../../lib/client.js';
import { confCH, runOptions } from '../../config.js';
import { DateRandom, rndNormal, rndWithin, rndWithinInt, rndExponentialInv } from '../../../acropolis-nd/lib/Pythagoras.js';
import { dtToUtc, dtOffsetMinutes } from '../../../acropolis-nd/lib/Chronos.js';
import { ConLog, consolDummy } from '../../../acropolis-nd/lib/scripts/nodeOnly.js';
import { ErrAcrCH } from '../helpers/errors.js';
import { rxDefaultRowMach, columnsFromStructStr } from '../helpers/transforms.js';
import { ReadableArrOrFn, TransformParseRaw } from '../helpers/streams.js';
import { graphSql } from './graphSql.js';
import { functionsUDF } from '../sql/functionsUDF.js';

const logLevel = runOptions?.tests?.logLevel || 'log';

const logger = (logLevel === 'silent') ? consolDummy : new ConLog('debug', { inclTS: true });

class ScrollOnColumn extends PageScroll {
  constructor(column,
    {
      fnNext = (row) => `(${column} > ${row})`,
      fnPrev = (row) => `(${column} < ${row})`,
      vectorDefault = 1000,
    } = {},
  ) {
    super({ fnNext, fnPrev, vectorDefault });
  }
}

/**
 *
 * ┌─parent─┬────child─┬─meaning in a social network ──────────────────────────────────────┐
 * │id_Nick │ id_Seth  │ Seth is following Nick or in other words Nick is followed by Seth │
 * └────────┴──────────┴───────────────────────────────────────────────────────────────────┘
 * @export
 * @class Graph
 */
export class Graph {
  constructor(client, dbName = 'twitter',
    { idType = 'UInt32', typeAlias = 'tenant', parentAlias = 'parent', childAlias = 'child', mutualAlias = 'friends' } = {}) {
    this._props = { dbName, idType, typeAlias, parentAlias, childAlias, mutualAlias, graphNs: `${dbName}.edges` };
    this.ns = { nodes: `${dbName}.nodes`, edges: `${dbName}.edges` };
    this.client = client;
    this.gSql = graphSql(this);
  }

  get graphNs() { return this._props.graphNs; }

  static inspectSql(sqlStr) {
    const sqlLog = (sqlOrFragment) => `-----[\n${sqlOrFragment}\n]----`;
    logger.log(sqlLog(sqlStr));
  }

  async parents(child, vector = 1000, scroll = '', { FORMAT } = {}) {
    const sqlStr = this.gSql.parents(child, vector, scroll, { FORMAT });
    const result = await this.client.request(sqlStr);
    return result;
  }
  // const parents = (child, vector, scroll = '', { FORMAT } = {})
  /*
    async mutual(id, { LIMIT, OFFSET, FORMAT } = {}) {
      return this._chClient.get(graphSql.mutual(id, { ...this._props, LIMIT, OFFSET, FORMAT }));
    }
    async children(idParent, { LIMIT, OFFSET, FORMAT } = {}) {
      return this._chClient.get(graphSql.children(idParent, { ...this._props, LIMIT, OFFSET, FORMAT }));
    }
    async parents(idChild, { LIMIT, OFFSET, FORMAT } = {}) {
      return this._chClient.get(graphSql.parents(idChild, { ...this._props, LIMIT, OFFSET, FORMAT }));
    }
    */
}

export class GraphSim extends Graph {
  constructor(client, dbName = 'del1', { idType = 'UInt32', parentAlias = 'following', childAlias = 'followers', accountsCount = 100000 } = {}) {
    super(client, dbName, { idType, parentAlias, childAlias });
    this._sim = { accountsCount, nsAccounts: `${dbName}.nodes`, nsSimAccounts: `${dbName}.accountsSim` };
    this.ns.nodesSim = `${dbName}.nodesSim`;
  }

  async prepare() {
    this.client.defaultFlags = ['resolve', 'throwClient', 'throwNon200'];
    await this.client.request(sql.DROP_DATABASE(this._props.dbName));
    await this.client.request(sql.CREATE_DATABASE(this._props.dbName));
    await Promise.all([
      // this.client.request(sql.DROP_DATABASE(this._props.dbName), null, { chOpts: { session_id } }), will not work bug ?
      this.client.request(this.gSql.structNodes()),
      this.client.request(this.gSql.structEdges()),
    ]);
    // sqlQuery, data, { callback = (results) => results, chOpts = {}, flags;

    /*
    await this.client.request(sql.DROP_DATABASE(this._props.dbName));
    await this.client.request(sql.CREATE_DATABASE(this._props.dbName));
    await this.client.request(sql.CREATE_TABLE_fromSchema(undefined, this._sim.nsAccounts, this.gSql.structNodes()));
    await this.client.request(sql.CREATE_TABLE_fromSchema(undefined, this._sim.nsSimAccounts, this.gSql.structNodesSim()));
    await this.client.request(sql.CREATE_TABLE_fromSchema(undefined, this.graphNs, this.gSql.structEdges()));
    */
  }

  async addFunctionsUDF() {
    // eslint-disable-next-line no-restricted-syntax
    for (const fnUDF of Object.values(functionsUDF)) { await this.client.request(fnUDF); }
  }


  async buildTiny() {
    const tbTiny = 'twitter.tiny';
    let dt = new Date(); dt.setMilliseconds(0); dt = dt.getTime() / 1000;
    let data = [
      [1, 2, 1, dt],
      [1, 3, 1, dt],
      [1, 3, -1, dt],   // cancel
      [3, 1, 1, dt],    // canceled mutual
      [1, 4, 1, dt],
      [4, 1, 1, dt],    // mutual
      [2, 3, 1, dt],
      [3, 4, 1, dt],
    ];
    data = data.map((el) => `(${el})`).join('');
    await this.client.request(sql.DROP_TABLE(undefined, tbTiny));
    await this.client.request(sql.CREATE_TABLE_fromSchema(undefined, tbTiny, this.gSql.structEdges()));
    const sqlStr = sql.INSERT_INTO(undefined, tbTiny, data, { columns: '', FORMAT: 'Values' });
    const result = await this.client.request(sqlStr);
    logger.inspectIt(result);
  }

  async buildGraphTwitter2010(path = `${homedir()}/twitter-2010.txt.gz`) {
    /**
     * { @link https://snap.stanford.edu/data/twitter-2010.html  twitter-2010.txt.gz}
     * Nodes: 41 652 230 Edges: 1468 364 884?
     *        40 050 007
     * ┌─uniqExact(id)─┬──max(id)─┬──count()─┐  ┌─uniqExact(child)─┬─uniqExact(parent)─┐
     * │      41652230 │ 41652229 │ 41652230 │  │         35689148 │          40103281 │
     * └───────────────┴──────────┴──────────┘  └──────────────────┴───────────────────┘
     * {@link http://twitter.mpi-sws.org/ other twitter datasets}
     *
     * ┌─parent─┬────child─┬─meaning in a social network ──────────────────────────────────────┐
     * │id_Nick │ id_Seth  │ Seth is following Nick or in other words Nick is followed by Seth │
     * └────────┴──────────┴───────────────────────────────────────────────────────────────────┘
     *
    /*

    */
    // https://snap.stanford.edu/data/twitter-2010.html Nodes: 41 652 230 Edges: 1468 364 884?
    //  rows 1468365182  children 35689148 parents 40103281  distinct accounts 41652230
    const FORMAT = 'Values';
    const streamFile = createReadStream(path);
    const steamUnzip = createGunzip();
    logger.time('buildGraphTwitter2010');
    logger.time('batch');
    let counterTrx = 0;
    const dateRandom = new DateRandom(new Date('2007-01-01'), new Date('2009-12-30'), rndExponentialInv, 18, 1);
    const funcTrx = async (row) => {
      // logger.inspectIt({ row });
      const dtCr = ~~(dateRandom.randomDt().getTime() / 1000);
      counterTrx += 1;

      const [parent, child] = row.trim().split(' ');
      // const rt = JSON.stringify({ parent, child, sign: 1, dtCr });
      const rt = `(${parent}, ${child}, 1, ${dtCr})`;
      if (counterTrx % 1000000 === 0) { logger.timeEnd('batch'); logger.dir({ counterTrx, rt }); logger.time('batch'); }
      return rt;
    };
    const transformStream = new TransformParseRaw('CSV', { funcTrx });
    const sqlArr = sql.INSERT_INTO(undefined, `${this.graphNs}`, transformStream, { columns: '', FORMAT });
    // console.dir({ sqlArr, FORMAT})
    let resultsOut = this.client.request(...sqlArr, { flags: flagsCH.flagsToNum(['throwClient', 'throwNon200']) });
    await pipeline(streamFile, steamUnzip, transformStream);
    // streamFile.pipe(unzip).pipe(writeStream);
    resultsOut = await resultsOut; // if we want to check results do an await here after stream ends;
    // logger.dir({ resultsOut });
    assert.equal(resultsOut.statusCode, 200);
    logger.timeEnd('buildGraphTwitter2010');
  }

  async simInsertAccounts(count = this._sim.accountsCount) {
    // rndWithin = (min, max, rndFun = Math.random, ...args)
    // rndWithin = (min, max, rndFun = Math.random, ...args)
    // const columns = columnsFromStructStr(this.graphSql.structAccountsSim());
    const dateRandom = new DateRandom(new Date('2021-01-01'), new Date('2021-12-30'), rndExponentialInv, 18, 1);
    let cntCurrent = 0;
    const dataFn = () => {
      // eslint-disable-next-line no-cond-assign
      if ((cntCurrent += 1) <= count) {
        const dt = ~~(dateRandom.randomDt().getTime() / 1000);
        const expectC = rndWithin(0, 0.3, rndNormal).toFixed(2);
        const expectP = rndWithin(0, 0.3, rndNormal).toFixed(2);
        const values = `${cntCurrent}, ${dt}, ${expectC}, ${expectP}\n`;
        // console.log('val', values);
        return values;
      }
      return null;
    };
    const readStream = new ReadableArrOrFn(dataFn);
    const sqlArr = sql.INSERT_INTO(undefined, this._sim.nsSimAccounts, readStream, { FORMAT: 'CSV' });
    logger.time('insert');
    const results = await this.client.request(...sqlArr);
    logger.timeEnd('insert');
    return results;
  }

  async buildGraphRand() {
    // const ns = `${this._props.dbName}.${this._props.accountsSim}`;
    const FORMAT = 'JSONEachRow'; //
    logger.time('scanStream');
    logger.time('build100000');
    const results = await this.client.request(this.gSql.structNodesSim());
    assert.equal(results.statusCode, 200);
    const counterTrx = { parents: 0, children: 0 };
    const funcTrx = async (row) => {
      // logger.inspectIt({ row });
      counterTrx.parents += 1;
      if (counterTrx.parents % 100000 === 0) { logger.timeEnd('build100000'); logger.dir({ counterTrx }); logger.time('build100000'); }
      const parent = JSON.parse(row);
      const parentDtCr = dtToUtc(new Date(parent.dtCr));
      const limit = ~~(Math.log(this._sim.accountsCount * parent.expectC) ** 3);   //  Math.round((this._sim.accountsCount * parent.expectC)  );
      const sqlStr = `SELECT * FROM ${this._sim.nsSimAccounts} WHERE id != ${parent.id} ORDER BY rand() ASC LIMIT ${limit} FORMAT JSON`;
      const result = await this.client.request(sqlStr);
      const rt = result.body.data.map((child) => {
        const childDtCr = dtToUtc(new Date(child.dtCr));
        let dt = new Date((parentDtCr > childDtCr) ? parentDtCr : childDtCr);
        dt = dtOffsetMinutes(dt, rndWithinInt(10, 1440)); // give it a random offset of < 1 day
        dt.setMilliseconds(0);
        const rtArr = [{ parent: parent.id, child: child.id, sign: 1, dtCr: dt / 1000 }];
        if (Math.random() < 0.1) {
          rtArr.push({ parent: child.id, child: parent.id, sign: 1, dtCr: dt / 1000 });  // make sure we have some mutual
        }
        if (Math.random() < 0.1) {
          dt = dtOffsetMinutes(dt, 1440);
          rtArr.push({ parent: parent.id, child: child.id, sign: -1, dtCr: dt / 1000 }); // unregister 10%
          if (Math.random() < 0.2) {
            dt = dtOffsetMinutes(dt, 1440);
            rtArr.push({ parent: parent.id, child: child.id, sign: 1, dtCr: dt / 1000 }); // re-register 20% of unregistered
          }
        }
        counterTrx.children += rtArr.length;
        return rtArr.map((el) => JSON.stringify(el));
      });
      return rt.flat();
      // logger.inspectIt({ input, result });
    };

    logger.time('buildGraph');
    const transformStream = new TransformParseRaw(FORMAT, { funcTrx });
    // ------------
    // const columns = columnsFromStructStr(structAccountsSim);
    const sqlArr = sql.INSERT_INTO(undefined, `${this.graphNs}`, transformStream, { columns: '', FORMAT });
    // console.dir({ sqlArr, FORMAT})
    let resultsOut = this.client.request(...sqlArr);
    // ------------
    const sqlStr = `SELECT * FROM ${this._sim.nsSimAccounts} ORDER BY id ASC FORMAT ${FORMAT}`;
    const resultReader = await this.client.request(sqlStr, '', { flags: flagsCH.flagsToNum(['throwClient', 'throwNon200']) }); // don't resolve;
    await pipeline(resultReader.body, transformStream); // process.stdout)
    resultsOut = await resultsOut; // if we want to check results do an await here after stream ends;
    // logger.dir({ resultsOut });
    assert.equal(resultsOut.statusCode, 200);
    logger.timeEnd('buildGraph');
    // logger.inspectIt({ bodyStr }, 'bodySTr ------------------')
    return { counterStream: transformStream.counters, counterTrx };
  }

  async doAll() {
    await this.prepare();
    await this.simInsertAccounts();
    return this.buildGraph();
  }

  async truncateAndBuild() {
    await this.client.request(sql.TRUNCATE_TABLE(undefined, this.graphNs));
    return this.buildGraph();
  }
}


const client = new UndiciCH(confCH.uri, confCH.credentials, { connections: 10 });
export const graphSim = new GraphSim(client);

