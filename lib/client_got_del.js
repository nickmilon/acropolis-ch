

/**
 * ns = nameSpace string i.e  db.table
  * @todo add post for long queries
  * while a cursor.stream().pipe(mgTrxStream).pipe would allow transformations on the fly it can't reject objects
  * so we have to use a custom transform stream to implement this feature
  * function apiResponse(){
    this.success = true;
    this.message = "OK";
    this.code = 0;
    data = {}
}
*/ 

// import { pipeline as pipelineAsync } from 'stream/promises';
import { pipeline as pipelineAsync} from 'node:stream/promises';

import { ReadableStream } from 'node:stream/web';

import { promisify } from 'util';
import { createReadStream } from 'fs';
// import { pipeline, Transform } from 'stream';
import { setTimeout } from 'timers/promises';
// import { promises as streamPromises } from 'stream';
import { promises as streamPromises } from 'stream/promises';

import { inspectIt } from 'acropolis-nd/lib/scripts/nodeOnly.js';
// import { sleepMs } from 'acropolis-nd/lib/Plato.js'; // @todo use Timers Promises API (v16)
import got from 'got';

// import { TransfAndStringify } from '../../modules/temp/streams.js';
import { ResultsBase, ResultsCH, PipelineHandler, addToMatch, TransformJson, isObject } from '../../modules/code_to_move.js';
import { Graph } from './graph.js';
// import { insertJson, truncateTable } from './sql/basic.js'

import * as sqlBasic from './sql/basic.js';

// import { delos } from '../../../acropolis-nd/index.js'  // @rebase from 'acropolis-nd';

const pipelineAsync = promisify(pipeline);


/**
 * funcTrxDoc function for any additional transforms can NOT by async since a promise will create race conditions
 * funcTrxDoc if returns null nothing is pushed
 */
// @malakia δεν χρειάζεται το funcTrxDoc  also needs _flush(callback)
class TransformExty extends Transform {
  constructor({ name = 'TransfAndStringify', highWaterMark = 2048, funcTrxDoc = (doc) => JSON.stringify(dpc), logger } = {}) {
    super({ objectMode: true, encoding: 'utf8', highWaterMark });
    this._props = { name, funcTrxDoc, logger, docLast: null, dt: { Start: new Date(), End: new Date() } };
  }

  get docFirst() { return this._props.docFirst}

  get docLast() { return this._props.docLast}

  async _transform(data, encoding, callback) {
    try {
      this._props.docCount += 1;
      const doc = this._props.funcTrxDoc(data);
      if (this._props.docCount === 1) { this._props.docFirst = doc}
      // if (this._metadata.docCount === 1000) { console.log({"this": this._readableState}) }
      this.push(this._props.funcTrxDoc(data));
      this._props.docLast = doc;
      return callback();
    } catch (err) {
      this._metadata.status = 500;
      this._metadata.message = `error:${err.message}`;
      await this.log(err, 'error');
      await this.end();
      await setTimeout(100);    // give it a chance to end stream with what we have up to now otherwise stream closes with transfer closed with outstanding data
      return callback(err);    // signal the error anyway
    }
  }

  _flush(callback) {
    this._props.logger.info(`${this._props.name}: finished`);
    this.push(callback);
  }

  async _destroy(err, callback) {
    this.log(`destroy ${err}`);
    return callback(err)
  }

}



/**
 * a got (https://github.com/sindresorhus/got) plug-in for clickhouse https://clickhouse.tech/)
 * see https://github.com/sindresorhus/got
 * @todo "http://127.0.0.1:8124/replicas_status"
 * @todo
 *        - loop on basucSql to auto create functions  as async execSql(sql, ...args) { return this.post(sql(...args)) }
 *        - allow for query options  session_id, session_timeout, wait_end_of_query,  query_id, input_format_skip_unknown_fields etc
 *        - use CH buffering option https://altinity.com/blog/2018/9/28/progress-reports-for-long-running-queries-via-http-protocol
 */
class ClickHouseClient {
  constructor(connConf, { name = 'acropolis-ch', logger, verifyServer = false } = {}) {
    this._props = { connConf, name, logger };
    this._dict = { connectionStr: `connection clickhouse http server: ${this.uri}`};
    this._setAgent();
    if (verifyServer === true) {
      this._verifyServerAndCreateDbs()
        .then((resp) => {
          this._props.server_verified = resp;
        }).catch((err) => {
          throw new Error(`${this._dict.connectionStr} is not responding error(${err.message}`);
        });
    }
  }

  _setAgent() {
    this.agentHttp = got.extend({
      headers: {
        'user-agent': this._props.name,         // useful for tracing ch logs
        'Accept-Encoding': '*',                        // gzip, identity decompress is doing it anyway
      },
      prefixUrl: this.uri,
      decompress: true,

      hooks: {
        // beforeRequest  @todo afterResponse is not catching stream
        afterResponse: [
          (resp) => {
            // @todo this is no good although may be we log nothing we still calculate this stuff vvv
            this.log(`hook|${resp.req.method}|statusCode:${resp.statusCode}|ms: ${resp.timings.phases.total}}|${unescape(resp.url)}`, 'silly');
            return resp;
          }],
        beforeError: [
          (error) => {
            if (error) {
              this.log(`hook|statusCode: ${error.statusCode}|statusMessage:${error.statusMessage}|url:${unescape(error.url)}|body:${error.body}|`, 'error');
              return error; // _note if we return response we override throwHttpErrors: false
            }
            this.log(`hook|got error-without response:${JSON.stringify(error)}`, 'error');
            return error;
          }],
      },
    });
  }

  async _verifyServerAndCreateDbs() {
    try {
      Promise.all([
        await this.post(sqlBasic.createDatabase(this.workDbName, true)),
        await this.post(sqlBasic.createDatabase(this.altDbName, true)),
      ]);
      const resp = await this.ping();
      this.log(`${this._dict.connectionStr} workDbName:${this.workDbName} altDbName:${this.altDbName}|ok [ping-ms: ${resp.timings.phases.total}]`);
      return true;
    } catch (err) { this.log(`_verifyServerAndCreateDbs error:${err}`, 'error'); }
    return false;
  }

  static trimSqlEncoded(str) { return encodeURIComponent(str.trim().replace(/\n/g, ' ').replace(/  +/g, ' ')); }

  static getNS(dbName = 'default', tableName = 'test') { return `${dbName}.${tableName}`;}

  get uri() { return this._props.connConf.uri; }

  get workDbName() { return this._props.connConf.workDbName; }

  get altDbName() { return this._props.connConf.altDbName; }

  get name() { return this._props.name; }

  getWorkTbNS(tableName) { return this.constructor.getNS(this.workDbName, tableName); } // use this so we can switch dbs from configuration

  getAltTbNS(tableName) { return this.constructor.getNS(this.altDbName, tableName); } // use this so we can switch dbs from configuration

  /// db(dbName) { return new DB(dbName, this);}

  // eslint-disable-next-line class-methods-use-this
  chUri(query, { session_id, query_id, input_format_skip_unknown_fields } = {}) {
    const session_id_arg = (session_id === undefined) ? '' : `&session_id=${session_id}`;
    const query_id_arg = (query_id === undefined) ? '' : `&query_id=${query_id}`;
    const input_format_skip_unknown_fields_arg = (input_format_skip_unknown_fields) ? '&input_format_skip_unknown_fields=1' : '';
    const opts = `${session_id_arg}${query_id_arg}${input_format_skip_unknown_fields_arg}`;
    if (query) {
      return `?query=${this.constructor.trimSqlEncoded(query)}${opts}`;
    } if (opts === '') {
      return '';
    }
    return encodeURIComponent(`?${opts.substring(1)}`); // skip first '$'
  }

  async ping() { return this.agentHttp.get('ping'); }

  /**
   * when we need excessive transformations or need to selectively exclude objects from input stream
   * @param {string} sql sql
   * @param {*} inputStream a stream
   * @param {*} param2 f
   * @return {*} rt
   */
  async chInsertStreamExt(sql, inputStream, { funcTrxDoc = (doc) => doc, logSec = 60 }) {
    const trsStream = new TransfAndStringify({ name: 'TrfStm', highWaterMark: 1024, funcTrxDoc, logger: this._props.logger, logSec });
    try {
      const url = this.chUri(sql, { input_format_skip_unknown_fields: 1 });
      this.log(`chStream to uri:${url}`, 'debug');
      const gotStream = this.agentHttp.stream.post(url);
      // gotStream.on('response', (evt) => { console.log({evt}) })
      // { evt: { percent: 0, transferred: 460000, total: undefined }  { evt: { percent: 1, transferred: 460000, total: 460000 } } .on('response', response)
      const rt = await pipelineAsync(inputStream, trsStream, gotStream);
      this.log('pipelineAsync END', 'debug');
      return rt;
    } catch (err) { this.log(`chStream error:${err}|on object: ${trsStream.docLast}`, 'error'); }
    return true;
  }

  async insertStreamJsonExt(ns, inputStream, argsDict) {
    await this.chInsertStreamExt(sqlBasic.insertJson(ns), inputStream, argsDict);
  }

  /**
   * simpler method
   * @param {*} inputStream must be a JSON.stringified stream
   * @param {*} nsOutput
   */
  async chInsertStream(inputStream, nsOutput, throwOnErr = true) {
    const msg = `${this.name}:chInsertStream:${nsOutput}`;
    const results = new ResultsBase(msg);
    try {
      const url = this.chUri(sqlBasic.insertJson(nsOutput), { input_format_skip_unknown_fields: 1 });
      const gotStream = this.agentHttp.stream.post(url);
      const pipelineHandler = new PipelineHandler([inputStream, gotStream]);
      this.log(`START :${msg}`, 'debug');
      await pipelineAsync(pipelineHandler.pipelinesArr);
      // await sleepMs(400); // give it some time to catch on.errors that's the only way we found to catch
      this.log(`END   :${msg}`, 'debug');
      const plResults = await pipelineHandler.results();
      // console.log({plResults, a1: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'})
      if (plResults.success !== true) {
        if (plResults.success !== false && plResults.success.response !== undefined) { // got error since PipelineHandler returns only true | false
          const { statusCode, statusMessage, body } = plResults.success.response;
          const rt = results.error(plResults.success, {statusCode, statusMessage, data: body });
          return rt;
        }
        return results.error(plResults.error, { statusMessage: plResults.statusMessage, statusCode: plResults.statusCode}); // pipeLine Error
      }
      return results.success('done');
    } catch (error) {
      this.log(`${this.name}: ERROR ${error.message}`, 'error'); // mongo errors end up here
      if (throwOnErr === true) {
        throw new Error(`${this.name}: ${error.message}`);
      } else { return results.error(error, { statusMessage: error.message, statusCode: 592}); }
    }
  }
  
  /**
   * 
   * @param {*} query
   * @param {*} getOptions { responseType: text || json } if json driver does the parsing
   * @param {*} queryOptions
   * @param {*} modeDebug
   */
  async get(query, getOptions = {}, queryOptions = {}, modeDebug = '3') {
    try {
      // const responseType = (sql.includes(' JSON')) ? 'json' : 'text'
      const getOpts = { throwHttpErrors: false, responseType: 'text', ...getOptions};  // default i 'text' change to 'json' if you want driver to parse it;
      // const resp = await this.agentHttp.get(this.chUri(sql, queryOptions), getOpts); // {responseType: 'json'}
      // const resp = await this.agentHttp.get(this.chUri(sql));
      // const resp = await this.agentHttp.get(url, );
      const resp = await this.agentHttp.get(this.chUri(query, queryOptions), getOpts);
      return ResultsCH.results(resp, modeDebug);
    } catch (error) {
      return ResultsCH.resultsError(error, modeDebug);
    }
  }

  async getTest(query, getOptions = {}, queryOptions = {}, modeDebug = '3') {
    try {
      // const responseType = (sql.includes(' JSON')) ? 'json' : 'text'
      const getOpts = { throwHttpErrors: false, responseType: 'text', ...getOptions};  // default i 'text' change to 'json' if you want driver to parse it;
      // const resp = await this.agentHttp.get(this.chUri(sql, queryOptions), getOpts); // {responseType: 'json'}
      // const resp = await this.agentHttp.get(this.chUri(sql));
      // const resp = await this.agentHttp.get(url, );
      inspecIt({foo: 12345, query, getOptions, logger });
      const resp = await this.agentHttp.get(this.chUri(query, queryOptions), getOpts);
      inspectIt({foo: 123456, body: resp.body}, logger);
      // return ResultsCH.results(resp, modeDebug)
    } catch (error) {
      console.log({error});
      return ResultsCH.resultsError(error, modeDebug);
    }
  }

  /**
   * bypasses query escaping and results processing better used with FORMAT Native in sql
   * @param {string} query
   */
  async getQuick(query) {
    try {
      return (await this.agentHttp.get(`?query=${query}`, { throwHttpErrors: false })).body;
    } catch (error) {
      return error.message;
    }
  }

  /**
   * if a simple select sql we can put it in query or body, body is preferable coz it dosn't have to be trimmed and escaped by chUri
   * @param {*} query
   * @param {*} body
   * @param {*} postOptions
   * @param {*} queryOptions { session_id, query_id, input_format_skip_unknown_fields }
   * @param {*} modeDebug
   */
  async post(query = '', body = '', postOptions = {}, queryOptions = {}, modeDebug = '1') {
    try {
      // const responseType = (query.includes(' JSON') || body.includes(' JSON')) ? 'json' : 'text'
      const postOpts = { body, throwHttpErrors: false, responseType: 'text', ...postOptions};
      const resp = await this.agentHttp.post(this.chUri(query, queryOptions), postOpts);
      return ResultsCH.results(resp, modeDebug);
    } catch (error) {
      return ResultsCH.resultsError(error, modeDebug);
      // inspectObj(error.response, 'post Error resp')
      // throw new Error(`${this.name}: ${error.message}`)
    }
  }

  /**
   *
   * @param {*} obj object or array of objects || an already JSON.stringified object or Array of objects
   * @param {*} ns
   * @param {*} postOptions
   * @param {*} queryOptions
   * @param {*} modeDebug
   * @returns  a results or resultsError object (NEVER throws) callers should check success === true
   */
  async insertJson(obj, ns, postOptions = {}, queryOptions = {}, modeDebug = '3') {
    const queryOpts = { input_format_skip_unknown_fields: true, ...queryOptions}; // ignores extra fields by default override it if you wish
    const body = (typeof obj === 'object' && obj !== null) ? JSON.stringify(obj) : obj;  // assume already stringified
    const postOpts = { body, throwHttpErrors: false, responseType: 'text', ...postOptions};
    return this.post(sqlBasic.insertJson(ns), body, postOpts, queryOpts);
  }

  /**
   * when CSV format CH string results to boolean
   * @param {string} value
   */
  static chStrBool(value) {return (value === '1\n') ? true : (value === '0\n') ? false : undefined;}

  /**
   * most efficient way for boolean values
   * @param {*} queryRes
   */
  static chCSVToBool(queryRes) { return (queryRes.success === true) ? ClickHouseClient.chStrBool(queryRes.data) : queryRes;}


  async truncateTable(ns, ifExists = true) {return this.post(sqlBasic.truncateTable(ns, ifExists));}

  async existsTable(ns, format) {return this.get(sqlBasic.existsTable(ns, format));}

  async existsTableBool(ns) { return this.constructor.chCSVToBool(await this.existsTable(ns, 'CSV'));}

  /**
   *
   * @param {string} ns name-space
   * @param {*} where clause excluding word 'WHERE'
   * @returns {bool || string } true if found false if not otherwise error string
   * if found will contain 1 as defined in Select by my convention if not Found will be empty string else will be a string with a ch error usually (404)
   * if ns or where is invalid
   */
  async existsRecord(ns, where) {
    const rt = await this.getQuick(sqlBasic.existsRecord(ns, where));
    return (rt === '') ? ResultsBase.simpleHttp(404, 'not found') : (rt === '"TRUE"\n') ? ResultsBase.simpleHttp(200, 'exists') : ResultsBase.simpleHttp(501, rt);
  }

  async createTable(ns, createStruct) {return this.post(sqlBasic.createTable(ns, createStruct, true));}

  async countSimple(ns, where = '') {
    const queryRes = await this.get(sqlBasic.countSimple(ns, where, 'CSV'));
    return (queryRes.success === true) ? parseInt(queryRes.data, 10) : NaN;
  }

  async execSql(sql, ...args) { return this.post(sql(...args)); }


  async showCreateTable(ns) {
    const rt = await this.get(sqlBasic.showCreateTable(ns));
    if (rt.success === true) { rt.data = rt.data.replace(/"/g, ' ');}
    return rt;
  }

  async copyTableStructure(...args) {return this.post(sqlBasic.copyTableStructure(...args));}

  async streamGet(sql) {
    const url = this.chUri(sql);
    this.log(`get to uri:${unescape(url)}`, 'debug');
    try {
      return await this.agentHttp.stream.get(url);
    } catch (error) {
      return error;
    }
  }

  async log(msg, level = 'info') {
    if (this._props.logger) { this._props.logger[level](`|${this._props.name}|${msg}`); }
  }

  async insertFileJson(dbDotTable, pathname) {
    try {
      const url = this.chUri(sqlBasic.insertJson(dbDotTable));
      this.log(`insertFileJson:${url}`, 'debug');
      const gotStream = this.agentHttp.stream.post(url);
      await pipelineAsync(createReadStream(pathname), gotStream);
    } catch (err) { this.log(`insertFileJson error:${err}`, 'error'); }
    return true;
  }
}

// mongo CH interface ----------------------------------------------------------------------------------------

class ClickHouseClientMG extends ClickHouseClient {
  constructor(connConf, mgClientInst, { name = 'chClientMG', logger, verifyServer = true } = {}) {
    super(connConf, { name, logger, verifyServer });
    this._props.mgClientInst = mgClientInst;
  }

  get mgClientInst() { return this._props.mgClientInst;}

  async translateAppend(ns, appendObj) {
    // getLast = (ns, byField = '__createdAt', order = 'DESC')
    const rt = await this.execSql(sqlBasic.fetchLast, ns, appendObj.fieldName, appendObj.order);
    rt.meta.operation = 'translateAppend';
    if (rt.success === true) {
      if (rt.data.length === 1) {
        await ResultsCH.castToJsTypes(rt);
        const val = rt.data[0][appendObj.fieldName];
        let mgQuery = (appendObj.order === 'DESC') ? '$gt' : '$lt';
        mgQuery = { [appendObj.fieldName]: {[mgQuery]: val}};
        rt.meta.mgQuery = mgQuery;
        return rt;
      }
      rt.meta.mgQuery = null;
      return rt;
    }
    return rt;
  }

  /**
   *
   * @param {*} collNS
   * @param {*} chNS
   * @param {*} pl
   * @param {*} param3
   * @param {object} appendObj format {fieldName: '__createdAt', order: 'DESC'}
   */
  async mgAggrToCHTb(collNS, chNS = undefined, pl = [], { truncate = false, throwOnErr = false, appendObj, createStruct, transformer } = {}) {
    const nsOutput = (chNS === null) ? collNS : chNS;  // defaults to mongo NS
    const tableExists = await this.existsTableBool(nsOutput);
    // inspectObj({tableExists, pl, truncate, appendObj})
    if (tableExists === true) {
      if (truncate) {
        const rt = await this.truncateTable(nsOutput, true);
        if (rt.success === false) { return rt;}
      } else if (isObject(appendObj)) { // append only if not truncate
        const rt = await this.translateAppend(nsOutput, appendObj);
        if (rt.success !== true) { return rt; }
        if (rt.meta.mgQuery !== null) { addToMatch(pl, rt.meta.mgQuery);}
      }

    } else { // table doesn't exist create it if possible
      if (createStruct === undefined) { return ResultsBase.error(`${nsOutput} table doesn't exists amd no createStruct provided`); }
      const sql = sqlBasic.createTable(chNS, createStruct, true);  // create table
      const rt = await this.createTable(chNS, createStruct);
      if (rt.success === false) {return rt;} // failed to create table
    }

    // inspectObj({pl})

    /*
    if (tableExists !== true) {
      if (createStruct === undefined) { return ResultsBase.error(`${nsOutput} table doesn't exists amd no createStruct`) }
      const sql = sqlBasic.createTable(chNS, createStruct, true)
      const rt = await this.post(sql)
      if (rt.success === false) {return rt} // failed to create table
    }
    if (tableExists && truncate) { // failed to empty table
      const rt = await this.truncateTable(nsOutput, true)
      if (rt.success === false) { return rt}
    }
    */

    const transformInst = (transformer === undefined) ? new TransformJson() : transformer;
    const transform = transformInst.transform.bind(transformInst);
    const coll = this.mgClientInst.getCollectionFromNS(collNS);
    const cursor = coll.aggregate(pl, {});
    const rt = await this.chInsertStream(cursor.stream({transform}), nsOutput, throwOnErr);
    rt.meta.stats = transformInst.stats();
    return rt;
  }

}

/**
 *
 */

import * as structures from '../../sql/structures.js';
import * as pipelines from '../../modules/pipelines/sync.js';

// @todo move it, add date truncation which calls it
const dtTimePrecision = (dt, precision = 'm') => {
  const jumpTable = {
    h: () => dt.setHours(0, 0, 0, 0),
    M: () => dt.setMinutes(0, 0, 0),
    s: () => dt.setSeconds(0, 0),
    m: () => dt.setMilliseconds(0),
  };
  return jumpTable[precision](); // returns integer date is changed in place
};


class CHmgRapchat extends ClickHouseClientMG {
  constructor(connConf, mgClientInst, { name = 'chMgRapchat', logger, verifyServer = true } = {}) {
    super(connConf, mgClientInst, { name, logger, verifyServer });
    this._props.mgClientInst = mgClientInst;
    // this._collections = ['raps', 'accounts', 'events']
    this._mgConnectedOnce = false;
    this.tablesArr = ['accounts', 'accounts_follow', 'events', 'raps', 'ext_accountsLastSeen', 'ext_accounts_follow'];
    this.graphFollow = new Graph(this, this.accounts_followNS, { parentAlias: 'Following', childAlias: 'Followers'});
    mgClientInst.eventsEmitter.once('connect', () => {
      if (this._mgConnectedOnce === false) {
        this.workCollections = mgClientInst.workCollections;
        this.createAltDbTables();
        this._mgConnectedOnce = true; // no need since we use once but anyway
      }
    });
    mgClientInst.eventsEmitter.on('close', () => {this.log(`${this.name} mongo connection closed`);});
  }

  _defaultAltChNS(chNS, tbName) { return (chNS === null) ? this.getAltTbNS(tbName) : chNS; }

  static mgValuesToCh(val, precision = 'm') {
    if (val === true) {return 1;}
    if (val === false) {return -1;}
    if (val === null) {return 0;}
    if (typeof val.getMonth === 'function') { dtTimePrecision(val, precision); return val.toISOString().substring(0, 19); }
    return val;
  }

  static mgGraphIdToCh(mongo_id) { return {parent: mongo_id.p, child: mongo_id.c };}

  static docWhereUnique(mongo_id, collName) {
    return (collName === 'accounts_follow') ? `(parent = '${mongo_id.p}' AND child = '${mongo_id.c}')` : `(_id = '${mongo_id}')`;
  }

  get rapsNS() {return this.getWorkTbNS('raps');}

  get accountsNS() {return this.getWorkTbNS('accounts');}

  get accounts_followNS() {return this.getWorkTbNS('accounts_follow');}

  defaultCollection(nameStr) { return this.mgClientInst.getDefaultCollection(nameStr); }

  async fetchChBy_Id(ns, _id) { return this.get(sqlBasic.fetch(ns, _id));}

  async tablePopulate(collName, chNS, pl, { truncate = false, appendObj, transformer } = {}) {
    // collNS, chNS = undefined, pl = [], { truncate = false, throwOnErr = false, appendObj, createStruct } = {}) {
    const collNS = this.mgClientInst.defaultCollectionNS(collName);
    inspectObj({collNS});
    return this.mgAggrToCHTb(collNS, chNS, pl, { truncate, appendObj, transformer, createStruct: structures[collName] });
  }

  async accountsPopulate(chNS = null, { limit, truncate = false, _tbName = 'accounts'} = {}) {
    const appendObj = false; // {fieldName: '__createdAt', order: 'DESC'}
    const collNS = this.mgClientInst.defaultCollectionNS(_tbName);
    const pl = pipelines.accountsPopulate(limit);
    return this.mgAggrToCHTb(collNS, this._defaultAltChNS(chNS, _tbName), pl, { truncate, appendObj, createStruct: structures[_tbName] });
  }

  async accounts_followPopulate(chNS = null, { limit, truncate = false, _tbName = 'accounts_follow'} = {}) {
    const appendObj = false; // {fieldName: '__createdAt', order: 'DESC'}
    const collNS = this.mgClientInst.defaultCollectionNS(_tbName);
    const pl = pipelines.accounts_followPopulate(limit);
    return this.mgAggrToCHTb(collNS, this._defaultAltChNS(chNS, _tbName), pl, { truncate, appendObj, createStruct: structures[_tbName] });
  }

  async eventsPopulate(chNS = null, { limit, truncate = false, _tbName = 'events'} = {}) {
    const appendObj = false; // {fieldName: '__createdAt', order: 'DESC'}
    const collNS = this.mgClientInst.defaultCollectionNS(_tbName);
    const pl = pipelines.eventsPopulate(limit);
    return this.mgAggrToCHTb(collNS, this._defaultAltChNS(chNS, _tbName), pl, { truncate, appendObj, createStruct: structures[_tbName] });
  }

  async rapsPopulate(chNS = null, { limit, truncate = false, _tbName = 'raps'} = {}) {
    const appendObj = false; // {fieldName: '__createdAt', order: 'DESC'}
    const collNS = this.mgClientInst.defaultCollectionNS(_tbName);
    const pl = pipelines.rapsPopulate(limit);
    // inspectObj({pl, struct: structures[_tbName]})
    return this.mgAggrToCHTb(collNS, this._defaultAltChNS(chNS, _tbName), pl, { truncate, appendObj, createStruct: structures[_tbName] });
  }

  async beatsPopulate(chNS = null, { limit, truncate = false, _tbName = 'accounts'} = {}) {
    const foo = this;

    /*

          db.createView('beatsView1', 'beats',
        [
            { $set: { "_Options1_id" : { $arrayElemAt: [ "$options", 0 ] } } }, { $set: { "_idOptions1" : '$_Options1_id._id' } },
            { $out: 'beats_nick'},
        ]
        */
  }

  async createAltDbTables() {
    let results = await Promise.all(this.tablesArr.map(tableName => this.createTable(this.getAltTbNS(tableName), structures[tableName])));
    results = Object.fromEntries(this.tablesArr.map((x, i) => [x, results[i]]));
    return ResultsBase.multiple(results, 201);
  }

}

export {
  ClickHouseClient,
  ClickHouseClientMG,
  CHmgRapchat,
};
