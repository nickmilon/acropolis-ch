/* eslint-disable no-param-reassign */
/* eslint-disable no-bitwise */
/* eslint-disable prefer-const */
/* eslint-disable camelcase */

import { Pool } from 'undici';
import { isString } from 'acropolis-nd/lib/Pythagoras.js';
import { Events } from 'acropolis-nd/lib/Solon.js';
import { Enum32 } from 'acropolis-nd/lib/Thales.js';
import { ErrAcrCH } from './helpers/errors.js';
import { chFormatsProps } from './sql/varsCH/formats.js';

export const eventsCH = new Events();
export const flagsCH = new Enum32(['spare1', 'spare2', 'resolve', 'emitOnRequest', 'throwNon200', 'throwClient']);

/**
 * CH Response
 * @typedef {Object} CHresponse
 * @property {Integer} statusCode - http status code anything except 200 is an error (its explanation will be found in Body)
 * Currently CH sends 200 if query is successful not following HTTP conventions.
 * e.g. a CREATE TABLE query will reply with statusCode 200 and not a 201 (Created)
 * In case of an internal error from our side (no error from clickhouse) it will be 555 (we replace whatever statusCode CH has or has not sent
 * with 555. We do that in order to have a single property to check for any errors internal or external. In this case CH status code will be
 * saved in {@link statusCodeHTP}
 * statusCode, headers, trailers, body
 * @property {integer} statusCodeHTP - optional will be filled with actual HTTP status received from CH if we replaced it with a 555
 * @property {object} headers - HTTP headers as returned by CH to which we add our own 'x-acropolis-dtEnd' which contains dateTime of request
 * @property {object} trailers  - always empty object CH is not using it.
 * @property {string|object|StreamReadable} body  - CH response body (a Promise)
 * Normally CH returns a Promise in body. To simplify things we optionally can decode it to a String or an Object (for JSON like formats)
 * (depending on  headers['x-clickhouse-format']) if flag resolveBody has been set in request.
 * This is the suggested approach when expected data are not that many (a few hundred rows)
 * If resolveBody flag is not set you can resolve the Promise your self by resolving body.json() or body.text()
 * Alternatively you can consume the StreamReadable by piping it or whatever.
 * @Warning in case you neither consume or resolve the body you MUST destroy it by calling body.destroy()
 * @memberof CHclient
 */

/**
 * client Query options
 * @typedef {object} QueryOptions
 * @property {integer} flags enumeration output
 * @usage flagsCH.flagsToNum(['resolve']) => an integer that can be used in request options
 * available flags:
 * 'resolve': request will resolve response body
 * `emitOnRequest': client will emit emitOnRequest event (by default disabled for efficiency)
 * 'throwNon200':  throw error if statusCode !200
 * 'throwClient': throw if error on client
 * @property {object} flags enumeration output
 * @property {object} chOpts CH query options as object eg { query_id: 'foo' , session_id: 'bar'}
 * for available options {@link https://clickhouse.com/docs/en/operations/settings/settings/ see here}
 * @memberof CHclient
 */

/**
 * trims sql by removing /n and extra spaces (making it single line) adds any chOpts options to query string
 * @private
 * @param {String} sql an clickhouse sql string
 * @param {Object} chOpts clickhouse query options chOpts { session_id, query_id, input_format_skip_unknown_fields , compress decompress log_comment }
 * {@link https://clickhouse.com/docs/en/operations/settings/settings/}
 * no need to check validity CH will return 404 statusCode error if invalid => Code: 115. DB::Exception: Unknown setting query
 * @returns {String} a url encoded query strings
 */
const chQueryStr = (sql = '', chOpts = {}) => {
  const sqlTrimEnc = (str) => encodeURIComponent(str.replace(/\n|  +/g, ' '));  // remove new lines double spaces
  return Object.entries(chOpts).reduce((pr, cr, ix) => `${pr}${(ix > 0) ? '&' : ''}${cr[0]}=${cr[1]}`, sql ? `/?query=${sqlTrimEnc(sql)}&` : '/?');
};

/**
 * A node HTTP client for {@link https://clickhouse.com/ clickhouse} based on {@link https://undici.nodejs.org/#/ undici}
 * @class CHclient
 * @emmit
 */
export class CHclient {
  /**
   * Creates an instance of CHclient.φφφ
   * @param {String} uri of ch http server e.g.: uri: 'http://localhost:8123'
   * @param {Object} [credentials={ user: 'default', password: '' }] access credentials
   * @param {Object} options [{ connections = 10, name = 'CHclient'}] number of connections in pool, a name
   * @memberof CHclient
   */
  constructor(uri, credentials = { user: 'default', password: '' }, {
    connections = 10,
    name = 'CHclient',
  } = {}) {
    this._props = { uri, name, reqCount: 0 };
    this.events = eventsCH;
    this.event = this.events.register(this.name, ['Error', 'Request', 'Created', 'Closed', 'ServerVerified']);
    this.defaultFlags = undefined;
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

  async _init() {
    try {
      setTimeout(() => { this._afterCreation(); }, 20);
      return true;
    } catch (err) {
      this.event.Error(err);
      return err;
    }
  }

  /**
   * @fires CHclient#Created
   * @memberof CHclient
   * @return {void}
   */
  _afterCreation() {
    this.event.Created({ uri: this._props.uri });
  }

  /**
   * @fires CHclient#Closed
   * @memberof CHclient
   * @return {void}
   */
  async close() {   // @todo test it;
    await this._client.close();
    this.event.Closed();
  }

  /**
   * this is a private method called by {@link request}, {@link get} and {@link post} it is not supposed to be called
   * directly we only document it here for caller's reference
   * @private @async
   * @param {string} [method='GET'] http method
   * @param {string} [query=''] query string
   * @param {String|null|ReadableStream} [reqBody=null] request body
   * @param {Object} options [{ chOpts = {}, flags = this._defaultFlags }]
   * @throws ErrAcrCH if internal error or HTTP error if CH returned statusCode != 200 only if enabled by respective flags.
   * @emits Error, Request (optional);
   * @return {CHresponse} response
   * @memberof CHclient
   */
  async _request(method = 'GET', query = '', reqBody = null, { chOpts = {}, flags = this._defaultFlags } = {}) { // chOpts
    this._props.reqCount += 1;
    const path = (query.startsWith('/')) ? query : chQueryStr(query, chOpts);  // / = path is ready no need (as in /ping)
    let statusCode; let headers; let trailers; let body; let respFormat;
    // ---------------------------------------------------------------------common for events and errors;
    const reqInfo = (statusCodeCH, message = '') => {
      const reqBodySub = reqBody ? reqBody.toString().substring(0, 200) : '';
      const request = { name: this.name, message, sql: query, method, reqBodySub, chOpts, flags };
      return { statusCode: statusCodeCH ?? statusCode, headers, trailers, body, statusCodeHTP: statusCode, request };
    };

    try {
      ({ statusCode, headers, trailers, body } = await this._client.request({ path, method, headers: this._headers, body: reqBody }));
      respFormat = headers['x-clickhouse-format'];
      body.setEncoding('utf8');
      // ------------------------------------------------------------------ resolve closure
      const resolveBody = async () => {
        if (chFormatsProps[respFormat]?.dec) {
          body = await body.json();
        } else body = await body.text();
      };
      // --------------------------------------------------------------------
      headers['x-acropolis-dtEnd'] = new Date();
      if (statusCode === 200) { // @warning❗️ below we do flag validation directly for efficiency never change flagsCH order for resolve or event
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

  /**
   *
   * @readonly
   * @returns {string} name of instance
   * @memberof CHclient
   */
  get name() { return this._props.name; }

  /**
   * sets default flags
   * @param {Array} flags a flags array
   * @memberof CHclient
   */
  set defaultFlags(flags = ['resolve', 'throwClient']) { this._defaultFlags = flagsCH.flagsToNum(flags); }

  /**
   *
   * returns current default flags
   * @return {integer} flags
   * @memberof CHclient
   */
  get defaultFlags() { return this._defaultFlags; }

  /**
   * returns current default flags as array (verbose)
   * @memberof CHclient
   */
  get defaultFlagsArr() { return flagsCH.numToFlags(this._defaultFlags); }

  /**
   * pings CH server
   * @return {void}
   * @memberof CHclient
   */
  async ping() { return this.get('/ping'); }

  /**
   *
   * pings CH server
   * @return {integer} milliseconds to ping and return
   * @memberof CHclient
   */
  async pingMS() {
    const dt = Date.now();
    const res = await this.ping();
    return { res, ms: Date.now() - dt };
  }

  /**
   * get request for parameter details {@link _request see}
   * not to be called except in rare cases that you want to use GET explicity. Use {@link request instead}
   * @param {string} sqlQuery query to execute
   * @param {QueryOptions} options [{ chOpts = {}, flags }={}] {@link QueryOptions}
   * @return {CHresponse} {@link CHresponse}
   * @memberof CHclient
   */
  async get(sqlQuery, { chOpts = {}, flags } = {}) {
    return this._request('GET', sqlQuery, undefined, { chOpts, flags });
  }

  /**
   * get request for parameter details {@link _request see}
   * not to be called except in rare cases that you want to use POST explicity. Use {@link request instead}
   * @param {string} sqlQuery query to execute
   * @param {string|StreamReadable|null} data that will become request's body
   * @param {QueryOptions} options [{ chOpts = {}, flags }={}] {@link QueryOptions}
   * @return {CHresponse} {@link CHresponse}
   * @memberof CHclient
   */
  async post(sqlQuery, data = null, { chOpts = {}, flags } = {}) {
    return this._request('POST', sqlQuery, data, { chOpts, flags });
  }

  /**
   * executes a query request
   * This is the Main method of the class and the suggested way to execute queries
   * For efficiency it always uses POST passing sqlQuery in body if there are no data (so we don't have to url-encode the sqlQuery)
   * For parameter details {@link _request see}
   * @param {undefined|null|string} sqlQuery f
   * @param {undefined|null|string|ReadableStream} data if any
   * @param {*} options [{ callback = (results) => results, chOpts = {}, flags }]
   *  - callback optional function to be called with response as argument (for example can be used to daisy chain calls)
   *  - chOpts see {@link chOpts}
   *  - flags see {@link flags}
   * @return {CHresponse} response from CH server if no error
   * @memberof CHclient
   */
  async request(sqlQuery, data, { callback = (results) => results, chOpts = {}, flags } = {}) {
    if (data) {
      return callback(await this._request('POST', sqlQuery, data, { chOpts, flags })); // sql in path data in body
    }
    return callback(await this._request('POST', '', sqlQuery, { chOpts, flags })); // sql in body no data (prefer POST to get coz no need for url encoding)
  }
}
