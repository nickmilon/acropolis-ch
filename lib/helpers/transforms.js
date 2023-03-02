/* eslint-disable brace-style */
/* eslint-disable no-param-reassign */
/* eslint-disable no-bitwise */
/* eslint-disable max-len */
/*
const castToJsTypes = (metaOrResultsCH, dataArr = null) => {
  const typesToFunc = {
    'DateTime64(3)': val => new Date(`${val}Z`), // z is missing in ch
    DateTime: val => new Date(`${val}Z`),
    "DateTime('UTC')": val => new Date(`${val}Z`),
    UInt64: val => BigInt(val),
  }
  const dummy = val => val;
  try {
    const [meta, data] = (dataArr === null) ? [metaOrResultsCH.meta.ch.meta, metaOrResultsCH.data] : [metaOrResultsCH, dataArr]
    const typesDict = {};
    meta.forEach(x => typesDict[x.name] = typesToFunc[x.type] || dummy);
    data.map(x => Object.entries(x).forEach(([k, v]) => x[k] = typesDict[k](v)))
    return { success: true }
  } catch (err) {return { success: err }}
};
*/

/**
 * miscellaneous transformations
 * @module
 */

import { isNumber, isString } from 'acropolis-nd/lib/Pythagoras.js';
import { ErrAcrCH } from './errors.js';
import { coreTypes, extendedTypes, dummyFn } from '../sql/varsCH/types.js';

/**
 * @constant regex to get the underlying types from table descriptions (order is IMPORTANT i.e date after Date32)
 * @exportsFix
 */
export const typesRegex = new RegExp(/.*?(UInt8|UInt16|UInt32|UInt64|UInt256|Int8|Int16|Int32|Int64|Int128|Int256|Float32|Float64|String|UUID|Date32|DateTime64|DateTime|Date|Enum|AggregateFunction|tuple|IPv4|IPv6|Map|Point|Ring|MultiPolygon|Polygon).*/);

/**
 *
 * @param {Object} dataObj data
 * @param {*} colTypesObj as returned by body.meta on select or body.data on DESCRIBE TABLE;
 * @return {void} (mutates dataObj in place)
 */
export const dataTransform = (dataObj, colTypesObj) => {
  if (Array.isArray(dataObj)) { return dataObj.map((el) => dataTransform(el, colTypesObj)); }
  Object.entries(dataObj).forEach(([col, val]) => {
    const fn = coreTypes[colTypesObj[col]].toJS;
    // eslint-disable-next-line no-param-reassign
    dataObj[col] = fn(val);
  });
  return true;
};

/**
* extracts the underlined type of columns trimming out specifiers (Nullable, Array etc)
* @param {Array2d} metaArr array of column names as provided by Select body.meta results.meta or DESCRIBE TABLE body.data
* @returns {Array2d} {columnName, type}
*/
export const metaTrim = (metaArr) => metaArr.map((x) => [x.name, x.type.replace(typesRegex, '$1')]);

/**
* extracts the underlined type of columns trimming out specifiers (Nullable, Array etc)
* @param {Array2d} metaArr array of column names as provided by Select results.meta
* @returns {Object} {columnName, type}
*/
export const typesFromMeta = (metaArr) => Object.fromEntries(metaTrim(metaArr));

/**
 * a quick and dirty replacer so we can stringify to compatible CH types dates
 * will work for Date, Date32,DateTime64 and DateTime Types
 * under the assumption that Date, Date32 have T00:00:00.000Z;
 *  ❗️ will throw if we try to cast to Date32 || DateTime64 a date with T00:00:00.000Z
 * @param {Array} key array as returned by body.meta on select when FORMAt is JSON or JSONCompact
 * @param {Object} val value to Replace
 * @returns {*} val casted to type
 */
export const customReplacer = (key, val) => {
  if (typeof val === 'boolean') return val | 0;
  if (typeof val === 'bigint') return val.toString();
  // if (val.search && val.search(/(^\d{4}-\d{2}-\d{2}T.*?Z$)/) === 0) return val.slice(0, 19); // can't get it otherwise coz it has .toJSON property
  if (val.search && val.search(/(^\d{4}-\d{2}-\d{2}T.*?Z$)/) === 0) { // can't get it otherwise coz it has .toJSON property
    return `${val.replace('T00:00:00.000Z', '').slice(0, 19)}`;
  }
  return val;
};

/**
 * customized stringify
 * @param {*} dataJS array as returned by body.data on select when FORMAt is JSON or JSONCompact
 * @returns {string} json output
 */
export const JSONstringifyCustom = (dataJS) => JSON.stringify(dataJS, customReplacer);

export const toValueDefault = (val) => {
  if (typeof val === 'string') return `'${val}'`;
  if (typeof val === 'boolean') return val | 0;
  if (typeof val === 'bigint') return val.toString();
  if (typeof val.getMonth === 'function') return `'${val.toISOString().slice(0, 19)}'`; // casts OK, to Date, DateTime, DateTime64(with milliseconds striped)
  return val;
};

export const toValuesStr = (obj, transformFn = toValueDefault) => `( ${Object.values(obj).map((v) => transformFn(v)).join(', ')} )`;

export const toColumnNamesStr = (obj) => `( ${Object.keys(obj)} )`;

/**
* in case we want to parse bodyData string when FORMAT = Values
* (not very useful because will need eval to get js values from raw strings)
* @param {string} bodyData as returned by FORMAT Values
* @returns {Array} values as string ( any, numeric values become strings)
*/
export const bodyValuesToArr2d = (bodyData) => bodyData.slice(1, -1).split('),(').map((x) => x.replaceAll('\'', '').split(','));

// ------------------------------------------------------------------------------- casting to types
/**
 *
 * @param {Array} response as it comes from select ... FORMAT JSON body.meta or
 * @param {Bool} trimPrecision trims (3) from DateTime64(3)
 * DESCRIBE TABLE (select ... ) FORMAT JSON  body.data
 * @return {Object}  {columnName: columnType ..... }
 */

/**
 * generates a transform context from a query response
 * @param {Object} response query response as coming
 * a) from select body.meta when format = JSONCompact or JSON;
 * b) rom DESCRIBE TABLE body.data when format = JSON only
 * @param {Object} options [{ typesJT = coreTypes, filterDummy = true, asObj = false }={}] transform options
 * typesJT a jumpTable (defaults to coreTypes) {type: {toJs, fromJs }};
 * filterDummy it only outputs the columns for which we have toJs && fromJs functions
 * asObj returns an object instead of array;
 * @return {Array} Array if asObj === false else an Object 
 * return format: 
 * [
 *    { name: 'col_UInt64', idx: 3, baseType: 'UInt64', parms: '', fromJS: [Function: fromJS], toJS: [Function: toJS] },
 *    { name: 'col_DateTime64', idx: 18, baseType: 'DateTime64', parms: '3', fromJS: [Function: fromJS], toJS: [Function: toJS] },
 *    ......
 *    { name: 'col_ArrInt6432', idx: 21, baseType: 'Int64', parms: 'Array', fromJS: [Function: fromJS], toJS: [Function: toJS] }
 * ]
 */

export const castTransform = (response, { typesJT = coreTypes, filterDummy = true, asObj = false } = {}) => {
  const re1 = /(?<baseType>\w*)\W*(?<parms>\w*)/;
  const supportedFormats = ['JSONCompact', 'JSON'];
  const format = response.headers['x-clickhouse-format'];
  const describe = (response.body.data?.length && 'default_expression' in response.body.data[0]); // infer if command is DESCRIBE TABLE (in contrast to SELECT)
  let curType;
  if (!supportedFormats.includes(format)) { throw new ErrAcrCH(4011, `transformContext: supports only ${supportedFormats} formats`); }
  if (format === 'JSONCompact' && describe) { throw new ErrAcrCH(4011, 'transformContext: format must be JSON with DESCRIBE'); }
  const colTypesArr = (describe) ? response.body.data : response.body.meta;
  const colTrx = colTypesArr.map((col, idx) => {
    let { groups: { baseType, parms } } = re1.exec(col.type);
    if (parms !== '' && extendedTypes[baseType] !== undefined) { [baseType, parms] = [parms, baseType]; } // swap i.e Array(Int8)
    // prior to ch bool types if ((baseType === 'Int8' && col.name.endsWith('Bool'))) { baseType = 'Bool'; }
    curType = typesJT[baseType];
    const { fromJS, toJS } = curType;
    return { name: col.name, idx, baseType, parms, fromJS, toJS };
  });
  if (filterDummy === true) { return colTrx.filter((el) => (el.toJS !== dummyFn || el.fromJS !== dummyFn)); }
  if (asObj === true) { return Object.fromEntries(colTrx.map((col) => { const { name, ...rest } = col; return [name, rest]; })); }
  return colTrx;
};

/**
 *
 * @param {Object} data object or array
 * @param {Array} contextArr as returned by castTransform
 * @param {string} [fromOrTo='fromJS'] transform direction
 * @param {string} format one of valid CH formats currently  ['JSON', 'JSONCompact', 'JSONCompactEachRow']
 * @return {void} mutates data in place
 */
export const castData = (data, contextArr, fromOrTo = 'fromJS', format) => {
  // infer format if not set (when toJS easy to set it from headers) JSONCompactEachRow
  if (format === undefined) {
    if (Array.isArray(data)) { format = (Array.isArray(data[0])) ? 'JSONCompact' : 'JSON'; }
    else { format = 'JSONEachRow'; }
  }

  const idxKey = (format === 'JSON' || format === 'JSONEachRow') ? 'name' : 'idx';
  const castRow = (dataRow) => {
    const castRowColumn = (fn, idx) => {
      if (Array.isArray(dataRow[idx])) {
        dataRow[idx] = dataRow[idx].map((el) => fn(el));
      } else { dataRow[idx] = fn(dataRow[idx]); }
    };
    contextArr.forEach((ctx) => castRowColumn(ctx[fromOrTo], ctx[idxKey]));
  };

  if (format === 'JSONEachRow' || format === 'JSONCompactEachRow') { castRow(data); }
  else { data.forEach((dataRow) => castRow(dataRow)); }  // rows probably more than columns that's why we loop on rows first;
};

/**
 *
 * @param {Object} response a select query response
 * @param {Array} context a context Array as returned by castTransform if undefined will be computed on the fly
 * (simplifies things for JSON or JSONCompact formats) for one of transforms for repeated use;
 * better evaluate it once by calling castTransform
 * @param {*} options [{ typesJT = coreTypes }={}] a jumpTable (defaults to coreTypes) {type: {toJs, fromJs }};
 * @return {void} mutates data in place
 */
export const castResponse = (response, context, { typesJT = coreTypes } = {}) => {
  const format = response.headers['x-clickhouse-format'];
  if (context === undefined) {
    if (format === 'JSONCompactEachRow') { throw new ErrAcrCH(4011, 'castData JSONCompactEachRow has no meta'); }
    context = castTransform(response, { typesJT });
  }
  castData(format === 'JSONCompactEachRow' ? response.body : response.body.data, context, 'toJS', format);
};

// -------------------------------------------------------------------------------

/**
 * extracts an array of column names
 * @param {string} struct a table structure string i.e.: ( id UInt32, dtCr DateTime) ENGINE = MergeTree
 * @returns {Array} column names
 */
export const columnsFromStructArr = (struct) => {
  const re = /\((?<columns>[^)]+)\)/;
  const columnsStr = re.exec(struct).groups.columns.trim();
  return columnsStr.replace(/(\n|\s{2,8})/g, '').split(/(,|\s)/).filter((x) =>
    (x !== ' ' && x !== ',' && x !== '')).filter((el, idx) => (idx % 2 === 0));
};

/**
 * extracts column names
 * @param {string} struct a table structure string i.e.: ( id UInt32, dtCr DateTime) ENGINE = MergeTree
 * @returns {string} column names to be used for example in select when format is Values
 */
export const columnsFromStructStr = (struct) => `(${columnsFromStructArr(struct)})`;

/**
 * Default regex for parsing rows when reading from raw body.data text
 * Provide a regex for best-effort parsing of raw data.
 * Because raw data are not strictly formatted it is difficult to guess tha row shape so
 * So treat it with caution and test it against your actual data as it is error prone especially
 * when data include columns of multiline strings that include special characters as (,), {,}
 * Least error prone formats are derivatives of JSON and Values with CSV TSV the most unstable
 * here are some benchmark results for reference
 * JSONCompactEachRow   rows: 1000000: 10.593s
 * JSONEachRow          rows: 1000000: 16.988s  (with JSON.stringify => parse)
 * Values               rows: 1000000: 10.582s
 * CSV                  rows: 1000000: 11.007s
 * TSV                  rows: 1000000: 10.849s
 *
 * @param {string} format clickhouse format
 * @param {boolean} onlyKeys ignores format and returns just supported keys (mainly for testing)
 * @returns {RegExp} a regex expression
 */
export const rxDefaultRowMach = (format, onlyKeys = false) => {
  const compact = /(?<row>\[.+?\](?=\n))/g;
  const line = /(?<row>.+?\n)/g;
  const each = /(?<row>\{.+?\}(?=\n))/g;  // /(?<row>\{.+?\}(?=\n\{))/g;            // /(?<row>\{.+?\}(?=\n\{))/g;
  const rxJT = {
    JSONCompactEachRow: compact,                      // safe and faster than JSONEachRow
    JSONCompactStringsEachRow: compact,               // use it only for rows with no strings or with strings [\d a-Z]
    JSONEachRow: each,                                // safe
    JSONStringsEachRow: each,                         // use it only for rows with no strings or with strings [\d a-Z]
    Values: /(?<row>\(.+?\)(?=,\())/g,                // will fail if row contains '),(' we do a look ahead for this pattern
    CSV: line,                                        // good for anything except if rows contains multiline strings
    TSV: line,                                        // good for anything except if rows contains multiline strings
  };
  if (onlyKeys === true) { return Object.keys(rxJT); }
  const rx = rxJT[format];
  if (rx === undefined) { throw new ErrAcrCH(4011, `we only support ${Object.keys(rxJT)} formats`); }
  return rx;
};

export const objToSchema = (obj) => {
  const kt = Object.entries(obj).map(([k, v]) => {
    if (isString(v)) return [k, 'String'];
    if (isNumber(v)) return Number.isInteger(v) ? [k, 'Int32'] : [k, 'Float64'];
    return [k, 'String'];
  });
  return `(${kt.map((el) => el.join(' '))})`;
};
