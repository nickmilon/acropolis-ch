/* eslint-disable no-param-reassign */
/* eslint-disable no-bitwise */
/* eslint-disable prefer-const */
/* eslint-disable camelcase */

/**
 * { @link https://groups.google.com/g/clickhouse clickhouse Google Groups }
 * { @link https://www.instana.com/blog/reducing-clickhouse-storage-cost-with-the-low-cardinality-type-lessons-from-an-instana-engineer/ }
 * { @link https://www.instana.com/blog/improve-query-performance-with-clickhouse-data-skipping-index/ data skipping index bloom_filter ngrambf_v1 tokenbf_v1}
 * { @link https://www.alibabacloud.com/blog/clickhouse-kernel-analysis-storage-structure-and-query-acceleration-of-mergetree_597727 alibaba engine texchnical explanatiom}
 * { @link https://blog.tinybird.co/2021/09/22/tips-11-how-to-get-the-types-returned-by-a-query/ clickhouse tips}
 * { @link https://guides.tinybird.co/guide/postgres-to-clickhouse coming from postgres }
 * { @link https://undici.nodejs.org/ manual }
 * { @link https://kb.altinity.com/ Altinity ClickHouse Knowledge Base }
 * { @link https://den-crane.github.io/Everything_you_should_know_about_materialized_views_commented.pdf  materialized view mechanism internals !}
 * { @link https://morioh.com/p/94561b205a3d short manual }
 * { @link https://support.huaweicloud.com/intl/en-us/eu-west-0-cmpntguide-mrs/mrs_01_2345.html huawei cloud:Big Data MR service Using ClickHouse from Scratch }
 * { @link https://www.slideshare.net/Altinity/clickhouse-and-the-magic-of-materialized-views-by-robert-hodges-and-altinity-engineering-team materialized views with examples}
 * { @link https://medium.com/lightspeed-venture-partners/why-lightspeed-invested-in-clickhouse-a-database-built-for-speed-b67ec2d5f041 Why Lightspeed invested in ClickHouse: a database built for speed
 * { @ling https://altinity.com/blog/harnessing-the-power-of-clickhouse-arrays-part-1 arrays}
*/

import { Client, Pool, errors } from 'undici';
import { inspectIt } from 'acropolis-nd/lib/scripts/nodeOnly.js';
import { isString } from 'acropolis-nd/lib/Pythagoras.js';
import { ErrAcrCH } from './helpers/errors.js';
import { chFormatsProps } from './sql/varsCH/formats.js';

import { Events } from './toNode.js';
import { Enum32 } from '../../acropolis-nd/lib/Thales.js';
// const client = new Client('http://localhost:3000');
 
/**
 * those CH formats will be parsed as JSON
 * {@link https://clickhouse.com/docs/en/interfaces/formats/ available formats}
 */

const eventsCH = new Events();
const flagsCH = new Enum32(['spare1', 'spare2', 'resolve', 'emitOnRequest', 'throwNon200', 'throwClient']);
/**
 * trims sql by removing /n and extra spaces (making it single line) adds any ch options
 * @private
 * @param {String} sql an clickhouse sql string
 * @param {Object} chOpts clickhouse query options chOpts { session_id, query_id, input_format_skip_unknown_fields , compress decompress log_comment }
 * https://clickhouse.com/docs/en/operations/settings/settings/
 * no need to check if valid ch will return 404 error if invalid => Code: 115. DB::Exception: Unknown setting query
 * @returns {String} a url encoded query strings
 */
const chQueryStr = (sql = '', chOpts = {}) => {
  const sqlTrimEnc = (str) => encodeURIComponent(str.replace(/\n|  +/g, ' '));  // remove new lines double spaces
  return Object.entries(chOpts).reduce((pr, cr, ix) => `${pr}${(ix > 0) ? '&' : ''}${cr[0]}=${cr[1]}`, sql ? `/?query=${sqlTrimEnc(sql)}&` : '/?');
};

/**
 * an  {@link https://undici.nodejs.org/#/ undici} client for {@link https://undici.nodejs.org/#/ {@link https://undici.nodejs.org/#/ clickhouse}
 * @todo "http://127.0.0.1:8124/replicas_status"
 * @todo
 *        - loop on basicSql to auto create functions  as async execSql(sql, ...args) { return this.post(sql(...args)) }
 *        - allow for query options  session_id, session_timeout, wait_end_of_query,  query_id, input_format_skip_unknown_fields etc
 *        - use CH buffering option https://altinity.com/blog/2018/9/28/progress-reports-for-long-running-queries-via-http-protocol
 */

// class MyClass { async m() {} }
export class UndiciCH {
  constructor(uri, credentials = {}, {
    connections = 10,
    name = 'UndiciCH',
  } = {}) {
    this._props = { uri, name, reqCount: 0 };
    this.events = eventsCH;
    this.event = this.events.register(this.name, ['Error', 'Request', 'Created', 'Closed', 'ServerVerified']);
    this.defaultFlags = undefined;
    // this.flags = new Enum32(['spare1', 'spare2', 'resolve', 'emitOnRequest', 'throwNon200', 'throwClient']);
    // this.events.on('Created', (name, event, ...args) => console.log('aaaaaaaaaaaaaaa', name));

    this._headers = {
      'user-agent': name,                             // useful for tracing ch logs
      // 'Content-Encoding': 'deflate', // 'gzip', @todo check what is better
      'Accept-Encoding': '*',
      'X-ClickHouse-User': credentials?.user ?? 'default',
      'X-ClickHouse-Key': credentials?.password ?? '',
    };
    this._client = new Pool(uri, { connections });

    this._init(uri, connections)
      .then((result) => {
        this._props.server_ok = result;
        if (result !== true) { throw result; }
      });
  }

  get name() { return this._props.name; }

  async _init(uri, connections) {
    try {
      // const resp = await this.pingMS();
      // this._client = await new Pool(uri, { connections });
      /*
      Promise.all([
        await this.post(sqlBasic.createDatabase(this.workDbName, true)),
        await this.post(sqlBasic.createDatabase(this.altDbName, true)),
      ])
      const resp = await this.ping();
      this.log(`${this._dict.connectionStr} workDbName:${this.workDbName} altDbName:${this.altDbName}|ok [ping-ms: ${resp.timings.phases.total}]`)
      */
      // this.events.emit('created', this);
      // this.logger.info({ message: `ClickHouseUndici client ${this._props.name} created successfully`});
      setTimeout(() => { this._afterCreation(); }, 20);
      return true;
    } catch (err) {
      this.event.Error(err);
      return err;
    }
  }

  _afterCreation() {
    this.event.Created({ uri: this._props.uri });
  }

  async close() {   // @todo test it;
    await this._client.close();
    this.event.Closed();
  }

  async _request(method = 'GET', query = '', reqBody = null, { chOpts = {}, flags = this._defaultFlags } = {}) { // chOpts
    // console.log({sql, chOpts});
    this._props.reqCount += 1;
    // this.events.emit('reqStart', this._props.reqCount, sql, method, reqBody, chOpts);
    // chOpts.log_comment = chOpts.log_comment || this.name;  // { log_comment: this.name, ...chOpts } destructuring is inefficient b

    // console.log({req: 11, method, path, chOpts, reqBody})
    const path = (query.startsWith('/')) ? query : chQueryStr(query, chOpts);  // / = path is ready no need (as in /ping)
    let statusCode; let headers; let trailers; let body;
    // ---------------------------------------------------------------------common for events and errors;
    const reqInfo = (statusCodeCH, message = '') => {
      const reqBodySub = reqBody ? reqBody.toString().substring(0, 200) : '';
      const request = { name: this.name, message, sql: query, method, reqBodySub, chOpts, flags };
      return { statusCode: statusCodeCH ?? statusCode, headers, trailers, body, statusCodeHTP: statusCode, request };
    };

    try {
      ({ statusCode, headers, trailers, body } = await this._client.request({ path, method, headers: this._headers, body: reqBody }));
      const respFormat = headers['x-clickhouse-format'];
      body.setEncoding('utf8');
      // ------------------------------------------------------------------ resolve closure
      const resolveBody = async () => {
        if (chFormatsProps[respFormat]?.dec) {
          body = await body.json();
        } else body = await body.text();
      };
      // --------------------------------------------------------------------
      headers['x-acropolis-dtEnd'] = new Date();
      if (statusCode === 200) { // @warning below we do flag validation directly for efficiency never change flagsCH order for resolve or event
        if ((flags & 4) === 4) { await resolveBody(); }
        if ((flags & 8) === 8) { this.event.Request(reqInfo()); }  // for efficiency
        return { statusCode, headers, trailers, body };
      }
      // -------------------------------------------------------------------- http non 200 error
      if (!isString(body)) { body = await body.text(); }  // perhaps haven't resolved or tried JSON'; non 200 body is always text
      const message = 'http Error';
      const errorInfo = reqInfo(undefined, message);
      if (flagsCH.hasFlag(flags, 'throwNon200')) { throw new ErrAcrCH(4001, `${message} from:(${this.name}) body: ${body}`, errorInfo); }
      return errorInfo;
    } catch (err) {
      if (err instanceof ErrAcrCH) {
        this.event.Error(reqInfo(undefined, err.message));
        throw err;
      } else {
        if (!isString(body)) { body = err.message; }  // stream probably closed by now;
        const errorInfo = reqInfo(555, err.message);
        this.event.Error(errorInfo);
        if (flagsCH.hasFlag(flags, 'throwClient')) { throw err; }
        return errorInfo;
      }
    }
  }

  set defaultFlags(flags = ['resolve', 'throwClient']) { this._defaultFlags = flagsCH.flagsToNum(flags); }

  get defaultFlags() { return this._defaultFlags; }

  get defaultFlagsArr() { return flagsCH.numToFlags(this._defaultFlags); }

  async ping() { return this.get('/ping'); }

  async pingMS() {
    const dt = Date.now();
    const res = await this.ping();
    return { res, ms: Date.now() - dt };
  }

  async replicas_status() { return this.get('/replicas_status'); }

  async get(query, { chOpts = {}, flags } = {}) {
    return this._request('GET', query, undefined, { chOpts, flags });
  }

  async post(sqlQuery, data = null, { chOpts = {}, flags } = {}) {
    return this._request('POST', sqlQuery, data, { chOpts, flags });
  }

  async request(sqlQuery, data, { callback = (results) => results, chOpts = {}, flags } = {}) {
    if (data) {
      return callback(await this._request('POST', sqlQuery, data, { chOpts, flags })); // sql in path data in body
    }
    return callback(await this._request('POST', '', sqlQuery, { chOpts, flags })); // sql in body no data (prefer this to get coz no need for url encoding)
  }

  async test() {
    // const q = 'SELECT * FROM test.test1';
    const q = 'SELECT * FROM test.test1 FORMAT JSON';
    const res = await this.get(q);
    return res;
    // return this.post(q);
  }
}

export class UndiciCHext extends UndiciCH {
  constructor(uri, credentials = {}, {
    connections = 10,
    name = 'UndiciCHext',
  } = {}) {
    super(uri, credentials, { connections, name });
  };

  async memTable(tbName, arr, sql) {
    const schema = objToSchema(arr[0]);
    await this.client.request(DROP_TABLE(undefined, tbName));
    sqlStr = CREATE_TABLE_fromSchema(undefined, tbName, schema, 'Memory');
    await this.request(sqlStr);
    await this.request(`INSERT INTO ${tbName} FORMAT JSONEachRow`, JSON.stringify(arr));
    if (sql !== undefined) {
      const sqlRes = await this.client.request(sql);
      await this.client.request(DROP_TABLE(undefined, tbName));
      return sqlRes;
    }
    return tbName;
  }
}

export {
  eventsCH,
  flagsCH,
};
