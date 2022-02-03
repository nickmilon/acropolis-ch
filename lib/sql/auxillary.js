/**
 * auxillary sql statements
 * @module
 * @typedef {Array.<string>} nameTypeArr2d a 2d array of [name, typeCh] @example [ [ 'ID', 'UInt32' ], [ 'foo', 'string' ] ]
 * @typedef {} sqlFragment a part of an sql statement/expression
 */

import { strKV, strIfK, strIfKV, nameSpaceOrTb } from './fragments.js';
import { rawSql } from './basic.js';

/**
 *
 * @export
 * @param {string} dbName name of db (can be empty for default db)
 * @param {object} tbName table name;
 * @returns {Array} table structure array >  [{ name: 'id', type: 'UInt32' },{ name: 'UInt8', type: 'Nullable(UInt8)' }]
 */
export const tableStructure = (dbName, tbName) => {
  const callback = (results) => [results.statusCode, results.body.meta];
  return [`SELECT * FROM ${nameSpaceOrTb(dbName, tbName)} LIMIT 0 FORMAT JSON`, '', { callback }];
};
