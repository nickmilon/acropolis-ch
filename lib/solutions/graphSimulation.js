/* eslint-disable camelcase */
/* eslint-disable no-await-in-loop */
/* eslint-disable no-bitwise */

/**
 * { @link https://snap.stanford.edu/data/twitter-2010.html twitter graph }
 */

/**
 * example solution
 * @module
 */

import assert from 'assert';
import { homedir } from 'os';
import { createReadStream } from 'fs';
import { createGunzip } from 'node:zlib';
import { pipeline } from 'stream/promises';
import { DateRandom, rndNormal, rndWithin, rndWithinInt, rndExponentialInv } from 'acropolis-nd/lib/Pythagoras.js';
import { dtToUtc, dtOffsetMinutes, dtNowUtc, convertMS } from 'acropolis-nd/lib/Chronos.js';
import { ConLog, consolDummy } from 'acropolis-nd/lib/scripts/nodeOnly.js';
import * as sql from '../sql/basic.js';
import { Graph } from './graph.js';
import { CHclient, flagsCH } from '../client.js';
import { confCH, runOptions } from '../../acropolis-ch-conf.js ';
import { ReadableArrOrFn, TransformParseRaw } from '../helpers/streams.js';
import { functionsUDF } from '../sql/functionsUDF.js';

const logLevel = runOptions?.tests?.logLevel || 'log';

const logger = (logLevel === 'silent') ? consolDummy : new ConLog('debug', { inclTS: true });

export class GraphSim extends Graph {
  constructor(client, dbName = 'twitter', { idType = 'UInt32', parentAlias = 'following', childAlias = 'followers', accountsCount = 100000 } = {}) {
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
  }

  async addFunctionsUDF() {
    // eslint-disable-next-line no-restricted-syntax
    for (const fnUDF of Object.values(functionsUDF)) { await this.client.request(fnUDF); }
  }


  async buildGraphTwitter2010(path = `${homedir()}/twitter-2010.txt.gz`) {
    /**
     * wget https://snap.stanford.edu/data/twitter-2010.txt.gz
     * curl https://snap.stanford.edu/data/twitter-2010.txt.gz | gunzip -c | head -100000 |  gzip  > del_100000.gz
     * https://snap.stanford.edu/data/twitter-2010.html Nodes: 41 652 230 Edges: 146 836 488 4?
     * at twitter-2010.txt | wc -l => 1 468 365 182
     * { @link https://snap.stanford.edu/data/twitter-2010.html  twitter-2010.txt.gz}
     * Nodes: 41 652 230 Edges: 1468 364 882 
     * rows 1468 365 182  children 35689148 parents 40103281  distinct accounts 41652230
     * 
     * ┌─uniqExact(id)─┬──max(id)─┬──count()─┐  ┌─uniqExact(child)─┬─uniqExact(parent)─┐
     * │      41652230 │ 41652229 │ 41652230 │  │         35689148 │          40103281 │    
     * └───────────────┴──────────┴──────────┘  └──────────────────┴───────────────────┘
     * {@link http://twitter.mpi-sws.org/ other twitter datasets}
     *
     * ┌─parent─┬────child─┬─meaning in a social network ──────────────────────────────────────┐
     * │id_Nick │ id_Seth  │ Seth is following Nick or in other words Nick is followed by Seth │
     * └────────┴──────────┴───────────────────────────────────────────────────────────────────┘
     *  time zcat ~/twitter-2010.txt.gz | awk -v min=1230760800 -v max=1262296800 'BEGIN{OFS=", ";srand()}{print 0, $2, $1, 1, (int(min+rand()*(max-min+1)))}' \
     *  |  clickhouse-client --password nickmilon -d twitter --query="INSERT INTO edgesDel1  FORMAT CSV"
    /*
    */
    const FORMAT = 'Values';
    
    const streamFile = createReadStream(path);
    // const streamFile = createReadStream('/mnt/sda3/twitter-2010-lines.txt.gz')
    const steamUnzip = createGunzip();
    logger.time('buildGraphTwitter2010');
    logger.time('batch');
    let counterTrx = 0;
    const info = {currentBatch: 0, batchSize: 1000000, totalSize: 1468365182, dtStart: Date.now() }
    const dateRandom = new DateRandom(new Date('2007-01-01'), new Date('2009-12-30'), rndExponentialInv, 18, 1);
    const funcTrx = async (row) => {
      const pad = (v, n = 2) => v.toString().padStart(n, '0');
      const dtCr = ~~(dateRandom.randomDt().getTime() / 1000);
      counterTrx += 1;

      const [parent, child] = row.trim().split(' ');
      // const rt = JSON.stringify({ parent, child, sign: 1, dtCr });
      const rt = `(1, ${parent}, ${child}, 1, ${dtCr})`;
      // console.log(rt)
      if (counterTrx % info.batchSize === 0) {
        info.currentBatch +=1;
        info.dtNow = Date.now();
        let dtDelta = info.dtNow - info.dtStart 
        info.opsSec = counterTrx / dtDelta
        info.ETA = convertMS(Math.round((info.totalSize - counterTrx)  / info.opsSec)) 
        const logStr = `batch:${pad(info.currentBatch, 6)}, ETA:${info.ETA} opsSec:${info.opsSec.toFixed(2)} cnt:${counterTrx.toLocaleString()})`
        // logger.dir(info)
        logger.log(logStr)
        // logger.timeEnd('batch'); logger.dir({ counterTrx, rt }); logger.time('batch'); 
      }
      return rt;
    };
    const transformStream = new TransformParseRaw('CSV', {highWaterMark: 2048, funcTrx });
    const sqlArr = sql.INSERT_INTO(undefined, `${this.ns.edges}`, transformStream, { columns: '', FORMAT });
    // console.dir({ sqlArr})
    let resultsOut = this.client.request(...sqlArr, { flags: flagsCH.flagsToNum(['throwClient', 'throwNon200']) });
    const ppl =  pipeline(streamFile, steamUnzip, transformStream);
     await ppl
    // await pipeline(streamFile, steamUnzip, transformStream);
    // streamFile.pipe(unzip).pipe(writeStream);
    resultsOut = await resultsOut; // if we want to check results do an await here after stream ends;
    // logger.dir({ resultsOut });
    assert.equal(resultsOut.statusCode, 200);
    logger.timeEnd('buildGraphTwitter2010');
  }


  async buildNodes() {
    const selectSql = `
    SELECT node as id, '2006-07-19 01:03:44' as dtCr, '-' as name 
    FROM
      (
        SELECT DISTINCT parent AS node
        FROM ${this.ns.edges}
        UNION DISTINCT
        SELECT DISTINCT child AS node
        FROM ${this.ns.edges}
      )`
  // sqlIns = sql.INSERT_INTO(undefined, `${this.ns.nodes}`,  select)
  const sqlIns = `INSERT INTO ${this.ns.nodes}\n ${selectSql}`
  const result = await this.client.request(sqlIns)
  assert.equal(result.statusCode, 200);
  }

  async doAll(twtpath = process.env.npm_config_twtpath) {
    await this.prepare();
    await this.buildGraphTwitter2010(twtpath);
    // await this.simInsertAccounts();
    await this.buildNodes();
    // return this.buildGraph();
  }

  async truncateAndBuild() {
    await this.client.request(sql.TRUNCATE_TABLE(undefined, this.ns.edges));
    return this.buildGraph();
  }
}

const client = new CHclient(confCH.uri, confCH.credentials, { connections: 10 });
export const graphSim = new GraphSim(client);

/**
 * (SELECT name FROM generateRandom('name String(10)') LIMIT 1) as name
  select * from users join widgets on widgets.id = (
    select id from widgets
    where widgets.user_id = users.id
    order by created_at desc
    limit 1
   )

   select * from twitter.edges100
    where id in (
        select min(dtCr) from twitter.edges group by dtCr
    )


    create table twitter.edges100  as select * from twitter.edges LIMIT 100

    select * from twitter.edges LIMIT 20 
  SELECT *
  FROM
  (
      SELECT DISTINCT parent AS node
      FROM twitter.edges
      LIMIT 10
      UNION DISTINCT
      SELECT DISTINCT child AS node
      FROM twitter.edges
      LIMIT 10
  )

  SELECT parent, child, dtCr
  FROM twitter.edges
  ORDER BY dtCr ASC
  LIMIT 10


  INSERT INTO twitter.nodes 
    SELECT node as id, '2019-01-01 00:00:00' as dtCr, '' as name
    FROM
      (
        SELECT DISTINCT parent AS node
        FROM twitter.edges100
        UNION DISTINCT
        SELECT DISTINCT child AS node
        FROM twitter.edges100
  )
 */

