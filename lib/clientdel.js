/* eslint-disable no-param-reassign */
/* eslint-disable no-bitwise */
/* eslint-disable prefer-const */
/* eslint-disable camelcase */


/**
 * client class and extras
 * @module Client
 * @overview
 */

/**
 * @module
 * @overview
 * client class and extras
 * @exports eventsCH
 * @exports flagsCH
 * @exports CHclient
 */



import { Pool } from 'undici';
import { isString } from 'acropolis-nd/lib/Pythagoras.js';
import { Events } from 'acropolis-nd/lib/Solon.js';
import { Enum32 } from 'acropolis-nd/lib/Thales.js';
import { ErrAcrCH } from './helpers/errors.js';
import { chFormatsProps } from './sql/varsCH/formats.js';

export const eventsCH = new Events();
export const flagsCH = new Enum32(['spare1', 'spare2', 'resolve', 'emitOnRequest', 'throwNon200', 'throwClient']);

/**
 * @summary foo
 *
 * Details 1234
 * @param {string} color - The shoe's color.
 * @returns {string} delete this
 */
const XXXsetColor = function(color) {
  return color;
};
 
export { XXXsetColor };
