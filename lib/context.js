/* eslint-disable arrow-body-style */

/**
 * context module: Provides a context via a closure so you can call predefined sql statements within this context.
 * Simplifies sql statement execution by presetting client, callback, chOpts and flags variables of client.request function.
 * with context doing the heavy lifting.
 * callback, chOpts and flags variable if included in sql statement itself will override those of context.
 * If you want to override those you must specify ALL of them in calling statement.
 * Creating a context has some cost and should be used only when you expect multiple statements under the same context.
 * Also have in mind that executing statements thorough a created context also adds some extra cost to the operation (although not mach)
 * compared to executing same statement directly via client.request or client.get or client.post
 * @module
 * @typedef {statementsArr2d) statementsArr2d a 2d array of [[name, statement [function]], ...]
 * @example [ 'SELECT', [Function: SELECT] ], [ 'SELECTraw', [Function: SELECTraw] ]... etc ]
 */

import { strKV, strIfK, strIfKV, nameSpaceOrTb } from './sql/fragments.js';
import { SELECT, SELECTraw } from './sql/select.js';
import * as auxillary from './sql/auxillary.js';
import * as statementsBasic from './sql/basic.js';

let statementsArr = Object.entries({ SELECT, SELECTraw, ...statementsBasic, ...auxillary });

/**
 * @export
 * @returns {statementsArr2d} current statementsArr
 */
export const contextStatementsGet = () => statementsArr;

/**
 * resets statementsArr to new value; provided so you can redefine it with the set of operations that you like
 * @Warning statementsArr2d is a singleton changing it will affect all subsequent calls to createContext from anywhere
 * @param {statementsArr2d} statementsArr2d f
 * @returns {*} void
 */
export const contextStatementsSet = (statementsArr2d) => { statementsArr = statementsArr2d; };

/**
 * appends statementsArr adding new values, provided so you can add to the set of build in operations that you like
 * @Warning statementsArr2d is a singleton changing it will affect all subsequent calls to createContext from anywhere
 * @param {statementsArr2d} statementsArr2d f
 * @returns {*} void
 * @example contextStatementsAppend(Object.entries(auxillary));
 */
export const contextStatementsAppend = (statementsArr2d) => { statementsArr = [...statementsArr, ...statementsArr2d]; };

export const createContext = (client, { callback, chOpts = {}, flags } = {}) => {
  const context = () => {
    const hook = async (sqlStrOrArr) => {
      if (client === undefined) { return sqlStrOrArr; }
      if (Array.isArray(sqlStrOrArr)) { return client.request(...sqlStrOrArr, { callback, chOpts, flags }); }
      return client.request(sqlStrOrArr, '', { callback, chOpts, flags });
    };
    return Object.fromEntries(statementsArr
      .map(([k, v]) => [k, async (...args) => hook(v(...args))]));
  };
  return context();
};
