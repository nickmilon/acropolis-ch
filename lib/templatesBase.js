const templatesBase = {
  insertJson: nameSpace => `INSERT INTO ${nameSpace} FORMAT JSONEachRow`,
  createGraphPC: ({nameSpace, extraFieldsFragment} = {}) => {
    return `
    CREATE TABLE ${nameSpace}
    (
      parent,
      child FixedString(36) Codec set, 
      relation UInt64,
      dtCreated DateTime Codec minmax
      ${(extraFieldsFragment) ? `,${extraFieldsFragment}` : ''}
    )
    ENGINE = MergeTree()
    ORDER BY (parent, child)
    PARTITION BY substring(parent, 34, 3) 
    SETTINGS index_granularity = 8192
    `
  }
}

export {
  templatesBase,
};
