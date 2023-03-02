/**
 * some table structures use in tests
 * @module
 */

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
