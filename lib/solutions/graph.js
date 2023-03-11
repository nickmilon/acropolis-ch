/* eslint-disable camelcase */
/* eslint-disable no-await-in-loop */
/* eslint-disable no-bitwise */

/**
 * example solution
 * @module
 */

/**
 * { @link https://snap.stanford.edu/data/twitter-2010.html twitter graph }
 */

import { ConLog, consolDummy } from 'acropolis-nd/lib/scripts/nodeOnly.js';
import { CHclient } from '../client.js';
import { confCH, runOptions } from '../../acropolis-ch-conf.js';
import { graphSql } from './graphSql.js';

/**
 *
 * ┌─parent─┬────child─┬─meaning in a social network ──────────────────────────────────────┐
 * │id_Nick │ id_Seth  │ Seth is following Nick or in other words Nick is followed by Seth │
 * └────────┴──────────┴───────────────────────────────────────────────────────────────────┘
 * @exportsFix
 * @class Graph
 */
export class Graph {
  constructor(client, dbName = 'twitter',
    { idType = 'UInt32', typeAlias = 'tenant', parentAlias = 'parent', childAlias = 'child', mutualAlias = 'friends' } = {}) {
    this._props = { dbName, idType, typeAlias, parentAlias, childAlias, mutualAlias };
    this.ns = { nodes: `${dbName}.nodes`, edges: `${dbName}.edges` };
    this.client = client;
    this.gSql = graphSql(this);
  }

  static inspectSql(sqlStr, logger = console) {
    const sqlLog = (sqlOrFragment) => `-----[\n${sqlOrFragment}\n]----`;
    logger.log(sqlLog(sqlStr));
  }

  async parents(child, vector, scroll = '', { type = 0, FORMAT } = {}) {
    const sqlStr = this.gSql.parents(child, vector, scroll, { type, FORMAT });
    // console.log(' parents parents parents\n', sqlStr)
    const result = await this.client.request(sqlStr);
    return result;
  }

  async children(parent, vector, scroll = '', { type = 0, FORMAT } = {}) {
    const sqlStr = this.gSql.children(parent, vector, scroll, { type, FORMAT });
    return this.client.request(sqlStr);
  }

  async childrenCount(parent, { type = 0, FORMAT = 'JSONCompactEachRow' } = {}) {
    return this.client.request(this.gSql.childrenCount(parent, { type, FORMAT }));
  }

  async parentsCount(child, { type = 0, FORMAT = 'JSONCompactEachRow' } = {}) {
    return this.client.request(this.gSql.parentsCount(child, { type, FORMAT }));
  }
}

const client = new CHclient(confCH.uri, confCH.credentials, { connections: 10 });

export const graph = new Graph(client);
