/* eslint-disable no-bitwise */
/* eslint-disable max-len */
/**
 * @module
 * @fileoverview
 * clickhouse types
 * see {@link https://clickhouse.com/docs/en/operations/settings/settings/ settings}
 * @exports chTypes
 */

import { tbStructParens } from '../fragments.js';

const dtToDateStr = (dt) => dt.toISOString().substring(0, 10);
export const dummyFn = (val) => val;

/**
 * DateTime64 parses string input in timezone defined on column creation
 * CREATE TABLE dt (id Nullable(UInt8), `utc` DateTime64(3, 'UTC'), `default` DateTime64(3) ) ENGINE = TinyLog
 * insert into dt Values (1, 1639804942756, 1639804942756)
 *
*/
export const coreTypes = {
  Bool: { min: false, max: true, toJS: (val) => val === 1, fromJS: (val) => val | 0 },                                                // synonym to Int8;
  UInt8: { min: 0, max: (2 ** 8) - 1, toJS: dummyFn, fromJS: dummyFn },                                                   // [0 : 255]
  UInt16: { min: 0, max: (2 ** 16) - 1, toJS: dummyFn, fromJS: dummyFn },                                                   // [0 : 65535]
  UInt32: { min: 0, max: (2 ** 32) - 1, toJS: dummyFn, fromJS: dummyFn },                                                   // [0 : 4294967295]
  UInt64: { min: 0n, max: (BigInt(2n ** 64n) - 1n), toJS: (val) => BigInt(val), fromJS: (val) => val.toString() },                          // [0 : 18446744073709551615]
  UInt128: { min: 0n, max: (BigInt(2n ** 128n) - 1n), toJS: (val) => BigInt(val), fromJS: (val) => val.toString() },                        // [0 : 340282366920938463463374607431768211455]
  UInt256: { min: 0n, max: (BigInt(2n ** 256n) - 1n), toJS: (val) => BigInt(val), fromJS: (val) => val.toString() },                        // [0 : 115792089237316195423570985008687907853269984665640564039457584007913129639935]
  Int8: { min: -1 * (2 ** 7), max: (2 ** 7) - 1, toJS: dummyFn, fromJS: dummyFn },                                          // [-128 : 127]
  Int16: { min: -1 * (2 ** 15), max: (2 ** 15) - 1, toJS: dummyFn, fromJS: dummyFn },                                       // [-32768 : 32767]
  Int32: { min: -1 * (2 ** 31), max: (2 ** 31) - 1, toJS: dummyFn, fromJS: dummyFn },                                       // [-2147483648 : 2147483647]
  Int64: { min: -1n * (2n ** 63n), max: (2n ** 63n) - 1n, toJS: (val) => BigInt(val), fromJS: (val) => val.toString() },                    // [-9223372036854775808 : 9223372036854775807]
  Int128: { min: -1n * (2n ** 127n), max: (2n ** 127n) - 1n, toJS: (val) => BigInt(val), fromJS: (val) => val.toString() },                 // [-170141183460469231731687303715884105728 : 170141183460469231731687303715884105727]
  Int256: { min: -1n * (2n ** 255n), max: (2n ** 255n) - 1n, toJS: (val) => BigInt(val), fromJS: (val) => val.toString() },                 // [-57896044618658097711785492504343953926634992332820282019728792003956564819968 ]
  Float32: { min: 1.175494351e-38, max: 3.402823466e+38, toJS: dummyFn, fromJS: dummyFn },
  Float64: { min: Number.MIN_VALUE * 2, max: Number.MAX_VALUE, toJS: dummyFn, fromJS: dummyFn },
  String: { min: '', max: 'Z', toJS: dummyFn, fromJS: dummyFn },                                        // max for compatibility only,
  UUID: { min: '00000000-0000-0000-0000-000000000000', max: 'ffffffff-ffff-ffff-ffff-ffffffffffff', toJS: dummyFn, fromJS: dummyFn },
  Date: { min: new Date('1970-01-01Z'), max: new Date('2148-12-31Z'), toJS: (val) => new Date(val), fromJS: (val) => dtToDateStr(val) },
  Date32: { min: new Date('1925-01-01Z'), max: new Date('2283-11-11Z'), toJS: (val) => new Date(val), fromJS: (val) => dtToDateStr(val) },
  DateTime: { min: new Date('1970-01-01 12:00:00Z'), max: new Date('2105-12-31 23:59:59Z'), toJS: (val) => new Date(`${val}Z`), fromJS: (val) => val.setMilliseconds(0) / 1000 },
  DateTime64: { min: new Date('1925-01-01 00:00:00Z'), max: new Date('2283-11-11 23:59:59Z'), toJS: (val) => new Date(`${val}Z`), fromJS: (val) => val.getTime() },
  IPv4: { min: '0.0.0.0', max: '255.255.255.255', toJS: dummyFn, fromJS: dummyFn },
  IPv6: { min: '::', max: 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff', toJS: dummyFn, fromJS: dummyFn },

};

export const extendedTypes = {
  FixedString: (n) => `FixedString(${n})`,
  LowCardinality: (dataType) => `LowCardinality(${dataType})`,
  Array: (dataType) => `array(${dataType})`,
  Nested: (nameTypeArr2d) => `Nested ${tbStructParens(nameTypeArr2d)}`,
  tuple: (tupleStr) => `tuple(${tupleStr})`,
  Nullable: (dataType) => `Nullable(${dataType})`,
  Map: (key, value) => `Map(${key}, ${value})`,
  Point: 'Point',
  Ring: 'Ring',
  Polygon: 'Polygon',
  MultiPolygon: 'MultiPolygon',
  /**
   * @param {string} name function name
   * @param  {...any} types_of_arguments see {@link https://clickhouse.com/docs/en/sql-reference/data-types/aggregatefunction/ AggregateFunction}
   * @returns {string} sql fragment
   * @example AggregateFunction('anyIf', 'String', 'UInt8') >> 'AggregateFunction(anyIf String, UInt8)'
   */
  AggregateFunction: (name, ...typesOfArguments) => `AggregateFunction(${name} ${typesOfArguments.join(', ')})`,
};
