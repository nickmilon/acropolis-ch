/* eslint-disable max-len */

/* 
  pack:
  u32 x, y;
  u64 v = ((u64)x) << 32 | y;
  unpack:
  x = (u32)((v & 0xFFFFFFFF00000000LL) >> 32);
  y = (u32)(v & 0xFFFFFFFFLL);
*/

// SELECT nm_UnPack32Arr(nm_Pack32Arr([4294967295,1])) => [4294967295,1]
export const functionsUDF = {
  nm_Pack32: 'CREATE FUNCTION IF NOT EXISTS nm_Pack32 AS (x32 , y32) -> toUInt64(bitOr(bitShiftLeft(toUInt64(x32), 32), y32))',
  nm_UnPack32_x: 'CREATE FUNCTION IF NOT EXISTS nm_UnPack32_x AS (v64) -> toUInt32( bitShiftRight(bitAnd(v64, 0xFFFFFFFF00000000 ), 32))',
  nm_UnPack32_y: 'CREATE FUNCTION IF NOT EXISTS nm_UnPack32_y AS (v64) -> toUInt32(bitAnd(v64, 0xFFFFFFFF ))',
  nm_Pack32Arr: 'CREATE FUNCTION IF NOT EXISTS nm_Pack32Arr AS (arr32) -> nm_Pack32(arr32[1], arr32[2])',
  nm_UnPack32Arr: 'CREATE FUNCTION IF NOT EXISTS nm_UnPack32Arr AS (v64) -> [nm_UnPack32_x(v64) , nm_UnPack32_y(v64)]',
  nm_ScaleWithin: 'CREATE FUNCTION IF NOT EXISTS nm_ScaleWithin AS (num, minAllowed, maxAllowed, min, max) -> (maxAllowed - minAllowed) * (num - min) / (max - min) + minAllowed',
  nm_RandWithin: 'CREATE FUNCTION IF NOT EXISTS nm_RandWithin AS (min, max) -> nm_ScaleWithin(rand32(), min, max, 0, 4294967295)',
};
