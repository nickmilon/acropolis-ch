/* eslint-disable max-len */
/**
 * @module
 * @fileoverview
 * clickhouse types
 * see {@link https://clickhouse.com/docs/en/operations/settings/settings/ settings}
 * @exports chTypes
 */

import { tbStructParens } from '../fragments.js';

export const chTypes = {
  UInt8: 'UInt8',
  UInt16: 'UInt16',
  UInt32: 'UInt32',
  UInt64: 'UInt64',
  UInt256: 'UInt256',
  Int8: 'Int8',
  Int16: 'Int16',
  Int32: 'Int32',
  Int64: 'Int64',
  Int128: 'Int128',
  Int256: 'Int256',
  Float32: 'Float32',
  Float64: 'Float64',
  String: 'String',
  FixedString: (n) => `FixedString(${n})`,
  UUID: 'UUID',
  Date: 'Date',
  Date32: 'Date32',
  DateTime: (timezone = '') => `DateTime(${timezone})`,
  DateTime64: (precision = 3, timezone = '') => `DateTime63(${precision} ${timezone})`,
  Enum: (enums) => `Enum(${enums})`,
  LowCardinality: (dataType) => `LowCardinality(${dataType})`,
  array: (dataType) => `array(${dataType})`,
  /**
   * @param {string} name function name
   * @param  {...any} types_of_arguments see {@link https://clickhouse.com/docs/en/sql-reference/data-types/aggregatefunction/ AggregateFunction}
   * @returns {string} sql fragment
   * @example AggregateFunction('anyIf', 'String', 'UInt8') >> 'AggregateFunction(anyIf String, UInt8)'
   */
  AggregateFunction: (name, ...typesOfArguments) => `AggregateFunction(${name} ${typesOfArguments.join(', ')})`,
  Nested: (nameTypeArr2d) => `Nested ${tbStructParens(nameTypeArr2d)}`,
  tuple: (tupleStr) => `tuple(${tupleStr})`,
  Nullable: (dataType) => `Nullable(${dataType})`,
  IPv4: 'IPv4',
  IPv6: 'IPv6',
  Map: (key, value) => `Map(${key}, ${value})`,
  Point: 'Point',
  Ring: 'Ring',
  Polygon: 'Polygon',
  MultiPolygon: 'MultiPolygon',
};

export const toTypeCH = (tp, val) => {
  const defaultFn = (x) => x;
  const jt = {
    String: (v) => `'${v}'`,
    UUID: (v) => `'${v}'`,
    Date: (v) => v.toISOString().slice(0, 10),
    Date32: (v) => v.toISOString().slice(0, 10),
    DateTime: (v) => new Date(v).setMilliseconds(0) / 1000,  // so we don' mutate it
    DateTime64: (v) => v.getTime(),
    IPv4: (v) => `'${v}'`,
    IPv6: (v) => `'${v}'`,
  };
  const fn = jt[tp] || defaultFn;
  return fn(val);
};

 


