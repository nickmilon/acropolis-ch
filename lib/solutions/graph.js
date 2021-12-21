/* eslint-disable camelcase */
/* eslint-disable no-await-in-loop */
/* eslint-disable no-bitwise */

/**
 * { @link https://snap.stanford.edu/data/twitter-2010.html twitter graph }
 */


import { homedir } from 'os';

import { setTimeout as setTimeoutAsync } from 'timers/promises';
import { finished, pipeline } from 'stream/promises';
import { PageScroll } from 'acropolis-nd/lib/Euclid.js';

import * as sql from '../sql/basic.js';

import { UndiciCH, flagsCH } from '../client.js';
import { confCH, runOptions } from '../../config.js';

import { ConLog, consolDummy } from '../../../acropolis-nd/lib/scripts/nodeOnly.js';
import { ErrAcrCH } from '../helpers/errors.js';
import { graphSql } from './graphSql.js';
import { functionsUDF } from '../sql/functionsUDF.js';

const logLevel = runOptions?.tests?.logLevel || 'log';

const logger = (logLevel === 'silent') ? consolDummy : new ConLog('debug', { inclTS: true });

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
    this._props = { dbName, idType, typeAlias, parentAlias, childAlias, mutualAlias };
    this.ns = { nodes: `${dbName}.nodes`, edges: `${dbName}.edgesV81` };
    this.client = client;
    this.gSql = graphSql(this);
  }

  static inspectSql(sqlStr) {
    const sqlLog = (sqlOrFragment) => `-----[\n${sqlOrFragment}\n]----`;
    logger.log(sqlLog(sqlStr));
  }

  async parents(child, vector, scroll = '', { type = 0, FORMAT } = {}) {
    const sqlStr = this.gSql.parents(child, vector, scroll, { type, FORMAT });
    const result = await this.client.request(sqlStr);
    return result;
  }

  async children(parent, vector, scroll = '', { type = 0, FORMAT } = {}) {
    const sqlStr = this.gSql.children(parent, vector, scroll, { type, FORMAT });
    return this.client.request(sqlStr);
  }

  async childrenCount(parent, { type = 0, FORMAT = 'JSONCompactEachRow' } = {}) {
    return this.client.request(this.gSql.childrenCount(parent, { type, FORMAT } ));
  }

  async parentsCount(child, { type = 0, FORMAT = 'JSONCompactEachRow' } = {}) {
    return this.client.request(this.gSql.parentsCount(child, { type, FORMAT }));
  }
}

const client = new UndiciCH(confCH.uri, confCH.credentials, { connections: 10 });

export const graph = new Graph(client);
