/* eslint-disable camelcase */
/* eslint-disable no-await-in-loop */
/* eslint-disable max-len */
/* eslint-disable no-underscore-dangle */

import assert from 'assert';
import { setTimeout as setTimeoutAsync } from 'timers/promises';
import { arrDiffSym } from 'acropolis-nd/lib//Zeno.js';
import { convertMS } from 'acropolis-nd/lib/Chronos.js';
import { ConLog } from 'acropolis-nd/lib/scripts/nodeOnly.js';
import { confCH } from '../config.js';
import { CHclientExt } from '../lib/clientExt.js';
import { coreTypes } from '../lib/sql/varsCH/types.js';
import { DROP_TABLE,
  TRUNCATE_TABLE,
  CREATE_DATABASE,
  CREATE_TABLE_fromSchema,
  CREATE_TABLE_fromTb } from '../lib/sql/basic.js';

const logger = new ConLog('debug', { inclTS: true });

export class ClientExtBM extends CHclientExt {
  constructor(uri, credentials = {}, {
    connections = 50,
    name = 'ClientExtBM',
  } = {}) {
    super(uri, credentials, { connections, name });
  }

  async _init() {
    await super._init();
    this.defaultFlags = ['throwNon200', 'throwNon200', 'resolve'];
    await this.initDb();
  }

  async initDb() {
    this._props.testDb = 'testCH';
    this._props.tbRnd = `${this._props.testDb}.tbRandom`;
    this._props.tbOut = `${this._props.testDb}.tbOut`;
    let schema = Object.keys(coreTypes);
    schema = arrDiffSym(schema, ['Date32']);  // Date32 not supported in GenerateRandom tables
    schema = `(${schema.map((t) => `${t} ${t}`)})`;
    await this.request(CREATE_DATABASE(this._props.testDb));
    await Promise.all([this.request(DROP_TABLE(undefined, this._props.tbRnd)), this.request(DROP_TABLE(undefined, this._props.tbOut))]);
    await this.request(CREATE_TABLE_fromSchema(undefined, this._props.tbRnd, schema, { ENGINE: 'GenerateRandom(4096)' }));
    await this.request(CREATE_TABLE_fromTb(undefined, this._props.tbOut, undefined, this._props.tbRnd, { ENGINE: 'MergeTree ORDER BY UInt8' }));
  }

  async streamInOut(LIMIT = 1000, requests = 1) {
    // const abortController = new AbortController();
    // JSONEachRow 'throwClient' flagsCH.flagsToNum(['throwNon200']);
    const sqlStr = `INSERT INTO ${this._props.tbOut} SELECT * FROM ${this._props.tbRnd} LIMIT ${LIMIT}`;
    await this.request(TRUNCATE_TABLE(undefined, this._props.tbOut));
    const promisesArr = [];
    for (let idx = 0; idx < requests; idx += 1) { promisesArr.push(this.request(sqlStr)); }
    const dtStart = new Date();
    Promise.all(promisesArr);
    let count;
    do {
      count = await this.request(`SELECT toUInt32(count()) AS count FROM ${this._props.tbOut} FORMAT JSONEachRow`);
      count = count.body.count;
      await setTimeoutAsync(1000);
    } while (count < (LIMIT * requests));
    const duration = new Date() - dtStart;
    assert.equal(count, LIMIT * requests);
    logger.dir({ count, durationStr: convertMS(duration), duration, ops: (LIMIT * requests) / (duration / 1000) });
  }
}

export const clientExtBM = new ClientExtBM(confCH.uri, confCH.credentials, { connections: 10 });
