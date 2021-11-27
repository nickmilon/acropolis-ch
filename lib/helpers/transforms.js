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

import { ErrAcrCH } from './errors.js';
import { isDate, isNumber, isString } from '../../../acropolis-nd/lib/Pythagoras.js';

/**
 * @constant regex to get the underlying types from table descriptions (order is IMPORTANT i.e date after Date32)
 * @export
 */
export const typesRegex = new RegExp(/.*?(UInt8|UInt16|UInt32|UInt64|UInt256|Int8|Int16|Int32|Int64|Int128|Int256|Float32|Float64|String|UUID|Date32|DateTime64|DateTime|Date|Enum|AggregateFunction|tuple|IPv4|IPv6|Map|Point|Ring|MultiPolygon|Polygon).*/);

/**
* extracts the underlined type of columns trimming out specifiers (Nullable, Array etc)
* @param {Array2d} metaArr array of column names as provided by Select results.meta
* @returns {Array2d} {columnName, type}
*/
export const metaTrim = (metaArr) => metaArr.map((x) => [x.name, x.type.replace(typesRegex, '$1')]);

/**
* extracts the underlined type of columns trimming out specifiers (Nullable, Array etc)
* @param {Array2d} metaArr array of column names as provided by Select results.meta
* @returns {Object} {columnName, type}
*/
export const typesFromMeta = (metaArr) => Object.fromEntries(metaTrim(metaArr));

export const toJsonTp = (tp, val) => {
  const defaultFn = (x) => x;
  const jt = {
    Date: (v) => v.toISOString().slice(0, 10),
    Date32: (v) => v.toISOString().slice(0, 10),
    DateTime: (v) => new Date(v).setMilliseconds(0) / 1000,  // so we don' mutate it
    DateTime64: (v) => v.getTime(),
  };
  return (jt[tp] || defaultFn)(val);
};

export const jsDateToCH = (val, chType) => {
  const chTypes = {
    Date: () => new Date(val).setHours(0, 0, 0, 0) / 1000,
    Date32: () => new Date(val).setHours(0, 0, 0, 0) / 1000,
    DateTime: () => new Date(val).setMilliseconds(0) / 1000,  // so we don' mutate it
    DateTime64: () => val.getTime(),
  };
  return chTypes[chType]();
};

/**
 * a replacer so we can stringify to compatible CH types dates will work only for DateTime64 and DateTime Types; 
 * @param {Array} key array as returned by body.meta on select when FORMAt is JSON or JSONCompact
 * @param {Object} val optional parser function defaults to defaultParser
 * @returns {*} val casted to type
 */
export const customReplacer = (key, val) => {
  if (typeof val === 'boolean') return val | 0;
  if (typeof val === 'bigint') return val.toString();
  if (val.search && val.search(/(^\d{4}-\d{2}-\d{2}T.*?Z$)/) === 0) return val.slice(0, 19); // can't get it otherwise coz it has .toJSON property
  return val;
};

/**
 * customized stringify
 * @param {*} dataJS array as returned by body.meta on select when FORMAt is JSON or JSONCompact
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

const DEL_toJson = (obj) => {
  const replacer = (key, val) => {
    if (typeof val === 'boolean') return val | 0;
    if (typeof val === 'bigint') return val.toString();
    if (val.search && val.search(/(^\d{4}-\d{2}-\d{2}T.*?Z$)/) === 0) return val.slice(0, 19); // can't get it otherwise coz it has .toJSON property
    return val;
  }
  if (Array.isArray(obj)) { obj.map((el) => toJson(el)) };
  return JSON.stringify(obj, customReplacer)

}
export const toValuesStr = (obj, transformFn = toValueDefault) => `( ${Object.values(obj).map((v) => transformFn(v)).join(', ')} )`;

export const toColumnNamesStr = (obj) => `( ${Object.keys(obj)} )`;

export const defaultParser = (val, type) => {
  const defaultFn = (x) => x;
  const jt = {
    Date: () => new Date(val),
    Date32: () => new Date(val),
    DateTime: () => new Date(val),
    DateTime64: () => new Date(val),
    UInt64: () => BigInt(val),
    UInt128: () => BigInt(val),
    UInt256: () => BigInt(val),
    Int64: () => BigInt(val),
    Int128: () => BigInt(val),
    Int256: () => BigInt(val),
  };
  return (jt[type] ?? defaultFn)(val);
};

/**
 * Low level function for parsing results usefully when we have multiple selects with same data shape
 * for a one off select use resultsParse
 * @param {Array} metaArr array as returned by body.meta on select when FORMAt is JSON or JSONCompact
 * @param {Object} parser optional parser function defaults to defaultParser
 * @returns {Function} parsing function;
 */
export const createParserFromMeta = (metaArr, parser = defaultParser) => {
  const metaTrimmed = metaTrim(metaArr);
  const parseRecord = (data) => {
    if (Array.isArray(data)) {
      return Object.fromEntries(metaTrimmed.map(([columnName, tp], idx) => [columnName, parser(data[idx], tp)]));  // JSONCompact format or similar
    }
    metaTrimmed.forEach(([name, tp]) => { data[name] = parser(data[name], tp); });  // as from JSON data
    return data; // mutated in place but returned it anyway
  };
  const parse = (bodyDataArr) => bodyDataArr.map((record) => parseRecord(record));
  return parse;
};

/**
 * function for parsing select results
 * for a one off select use resultsParse
 * @param {object} result from a select request
 * @param {Object} parser optional parser function defaults to defaultParser
 * @returns {void} mutates result.body.data in place
 */
export const resultsParse = async (result, parser = defaultParser) => {
  if (result.statusCode === 200) {
    result.body.data = createParserFromMeta(result.body.meta, parser)(result.body.data);
  }
};

/**
* in case we want to parse bodyData string when FORMAT = Values,
* (not much useful though coz will need eval to get js values from raw strings/
* @param {string} bodyDataArr array of column names as provided by Select results.meta
* @returns {Array} values as string
*/
export const bodyValuesToArr2d = (bodyDataArr) => {
  bodyDataArr = bodyDataArr.split('),(');
  return bodyDataArr.map((el) => el.replace(/(^\(|\)$)/g, '').split(','));
};

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
/*

SELECT * FROM /// LIMIT 1 FORMAT JSON
meta: [
        { name: 'id', type: 'UInt32' },
        { name: 'UInt8', type: 'Nullable(UInt8)' },
        { name: 'UInt16', type: 'Nullable(UInt16)' },
        { name: 'UInt32', type: 'Nullable(UInt32)' },
        { name: 'UInt64', type: 'Nullable(UInt64)' },
        { name: 'UInt256', type: 'Nullable(UInt256)' },
        { name: 'Int8', type: 'Nullable(Int8)' },
        { name: 'Int16', type: 'Nullable(Int16)' },
        { name: 'Int32', type: 'Nullable(Int32)' },
        { name: 'Int64', type: 'Nullable(Int64)' },
        { name: 'Int128', type: 'Nullable(Int128)' },
        { name: 'Int256', type: 'Nullable(Int256)' },
        { name: 'Float32', type: 'Nullable(Float32)' },
        { name: 'Float64', type: 'Nullable(Float64)' },
        { name: 'String', type: 'Nullable(String)' },
        { name: 'UUID', type: 'Nullable(UUID)' },
        { name: 'Date', type: 'Nullable(Date)' },
        { name: 'Date32', type: 'Nullable(Date32)' },
        { name: 'IPv4', type: 'Nullable(IPv4)' },
        { name: 'IPv6', type: 'Nullable(IPv6)' }
      ]
m.map((x) => [x.name, x.type]
s.replace(/Nullable\((.*?)\)/, '$1'

data types

UInt8, UInt16, UInt32, UInt64, UInt256, Int8, Int16, Int32, Int64, Int128, Int256   https://clickhouse.com/docs/en/sql-reference/data-types/int-uint/
Float32, Float64
Decimal(P, S), Decimal32(S), Decimal64(S), Decimal128(S), Decimal256(S)
Boolean  0 - 1;
String
FixedString(N)
UUID  is a 16-byte number   generateUUIDv4
Date
Date32      =  Date('2100-01-01').getTime() / 1000
DateTime('UTC') if no timezone gets timezone from server
DateTime64(precision, [timezone])
EnumLowCardinality(data_type)
array(T) T = data_type
AggregateFunction
Nested(name1 Type1, Name2 Type2);
tuple
Nullable(typename)
IPv4
IPv6
Map
Geo
*/