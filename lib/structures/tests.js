
import { tbStructParens } from '../sql/fragments.js';

export const sqlTests = {
  createTableObjRndFlat: ({ nameSpace } = {}) => `
    CREATE TABLE IF NOT EXISTS ${nameSpace}
    (
      id UInt32,
      int1 UInt32,
      int2 UInt32,
      intFn UInt32,
      dt DateTime,
      dtCr DateTime,
      ts UInt64,
      str  String
    )
    ENGINE = MergeTree()
    ORDER BY (id, dtCr)
    SETTINGS index_granularity = 8192
    `,

  createTableNumbers: (ns = 'test.numbers') => `
   CREATE TABLE IF NOT EXISTS ${ns}
   (number UInt64)
   ENGINE = MergeTree()
   ORDER BY number
   SETTINGS index_granularity = 8192
  `,
};

export const engine = `
MergeTree()
ORDER BY (id)
SETTINGS index_granularity = 8192 
`;


/*
export const simpleTpArr2d = Object.entries(chTypes).filter(([, v]) => typeof v === 'string');
export const simpleTpArr2dNullable = simpleTpArr2d.map(([k, v]) => [k, `Nullable(${v})`]);
export const simpleStructAll = tbStructParens([['id', 'UInt32'], ...simpleTpArr2d]);
export const simpleStructAllNullable = tbStructParens([['id', 'UInt32'], ...simpleTpArr2dNullable]);
 

export const mostTypesStruct = `
(
  id UInt32,
  Bool Nullable(Bool),
  UInt8 Nullable(UInt8),
  UInt16 Nullable(UInt16),
  UInt32 Nullable(UInt32),
  UInt64 Nullable(UInt64),
  UInt128 Nullable(UInt128),
  UInt256 Nullable(UInt256),
  Int8 Nullable(Int8),
  Int16 Nullable(Int16),
  Int32 Nullable(Int32),
  Int64 Nullable(Int64),
  Int128 Nullable(Int128),
  Int256 Nullable(Int256),
  Float32 Nullable(Float32),
  Float64 Nullable(Float64),
  String Nullable(String),
  UUID Nullable(UUID),
  Date Nullable(Date),
  Date32 Nullable(Date32),
  DateTime Nullable(DateTime),
  DateTime64 Nullable(DateTime64(3)),
  IPv4 Nullable(IPv4),
  IPv6 Nullable(IPv6)
)
`
/*

/*
/*
data types

'UInt8', 'UInt16', 'UInt32', 'UInt64', 'UInt256', 'Int8', 'Int16', 'Int32', 'Int64', 'Int128', 'Int256', 'Float32', 'Float64', 
'String', 'FixedString', 'UUID', 'Date', 'Date32', 'DateTime', 
'Enum', 'LowCardinality', 'array', 'AggregateFunction', 
'Nested', 'tuple', 'Nullable', 'IPv4', 'IPv6', 'Map', 'Point', 'Ring', 'Polygon', 'MultiPolygon']

Float32', Float64
Decimal(P, S), Decimal32(S), Decimal64(S), Decimal128(S), Decimal256(S)
Boolean  0 - 1;
String
FixedString(N)
UUID  is a 16-byte number   generateUUIDv4
Date
Date32      =  Date('2100-01-01').getTime() / 1000
DateTime('UTC') if no timezone gets timezone from server
DateTime64(precision, [timezone])
Enum
LowCardinality(data_type)
array(T) T = data_type
AggregateFunction
Nested(name1 Type1, Name2 Type2);
tuple
Nullable(typename)
IPv4
IPv6
Map
Geo 




{
  intFn: 225216,
  dt: 2011-09-17T04:28:22.556Z,
  int1: 75072,
  int2: 72791,
  dtCr: 2001-05-17T06:38:30.003Z,
  ts: 1316233702556,
  str: 'fymhb'
}
*/
 
