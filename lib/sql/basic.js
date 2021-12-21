/* eslint-disable camelcase */
/* eslint-disable no-warning-comments */
/* eslint-disable capitalized-comments */

// import { nameSpaceOrTb, strKV1, strIfV1K, StrIfV1KV, strIfVKV } from './fragments.js';
import { strKOperatorV, strKV, strIfK, strIfKV, nameSpaceOrTb } from './fragments.js';

/**
 * {@link https://clickhouse.com/docs/en/sql-reference/statements/create/database/ CREATE DATABASE }
 * @export
 * @param {string} dbName name of db (can be empty for default db)
 * @param {object} option [{ TEMPORARY, IF_NOT_EXISTS = true, ON_CLUSTER }] options
 * @returns {results} a results object
 */
export const CREATE_DATABASE = (dbName, { IF_NOT_EXISTS = true, ON_CLUSTER, engine = '' } = {}) => `
  CREATE DATABASE ${strIfK({ IF_NOT_EXISTS })} ${dbName} ${strIfKV({ ON_CLUSTER })} ${engine}
  `;

/**
 * @export
 * @param {string} dbName name of db (can be empty for default db)
 * @param {object} option [{IF_EXISTS = true, ON_CLUSTER }] options
 * @returns {results} a results object
 */
export const DROP_DATABASE = (dbName, { IF_EXISTS = true, ON_CLUSTER } = {}) =>
  `DROP DATABASE ${strIfK({ IF_EXISTS })} ${dbName} ${strIfKV({ ON_CLUSTER })}`;

/**
 * {@link https://clickhouse.com/docs/en/sql-reference/statements/drop/ DROP TABLE};
 * @export
 * @param {string} dbName name of db (can be empty for default db)
 * @param {string} tbName name of table
 * @param {object} options [{ TEMPORARY, IF_EXISTS = true, ON_CLUSTER }={}] gggg
 * @returns {results} a results object
 */
export const DROP_TABLE = (dbName, tbName, { TEMPORARY, IF_EXISTS = true, ON_CLUSTER } = {}) =>
  `DROP ${strIfK({ TEMPORARY })} TABLE ${strIfK({ IF_EXISTS })} ${nameSpaceOrTb(dbName, tbName)} ${strIfKV({ ON_CLUSTER })}`;

/**
 * {@link https://clickhouse.com/docs/en/sql-reference/statements/truncate/ TRUNCATE TABLE}; TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
 * @export
 * @param {string} dbName name of db (can be empty for default db)
 * @param {string} tbName name of table
 * @param {object} options [{IF_EXISTS = true, ON_CLUSTER }]
 * @returns {results} a results object
 */
export const TRUNCATE_TABLE = (dbName, tbName, { IF_EXISTS = true, ON_CLUSTER } = {}) =>
  `TRUNCATE TABLE ${strIfK({ IF_EXISTS })} ${nameSpaceOrTb(dbName, tbName)} ${strIfKV({ ON_CLUSTER })}`;

/**
 * SHOW CREATE [TEMPORARY] [TABLE|DICTIONARY|VIEW] [db.]table|view [INTO OUTFILE filename] [FORMAT format]
 * {@link https://clickhouse.com/docs/en/sql-reference/statements/show/ SHOW CREATE};
 * @export
 * @param {string} what [TABLE|DICTIONARY|VIEW] what to show
 * @param {string} dbName name of db (can be empty for default db)
 * @param {string} entityName name of entity (table view etc)
 * @param {object} options [{TEMPORARY, INTO, FORMAT}]  (format CSV by default coz it doesn't double escape \\n)
 * @returns {results} a results object
 */
export const SHOW_CREATE = (what = 'TABLE', dbName, entityName, { TEMPORARY, INTO, FORMAT = 'CSV' } = {}) =>
  `SHOW CREATE ${strIfK({ TEMPORARY })} ${what} ${nameSpaceOrTb(dbName, entityName)} ${strIfKV({ INTO, FORMAT })}`;

/**
 * DESC|DESCRIBE TABLE [db.]table [INTO OUTFILE filename] [FORMAT format]
 * {@link https://clickhouse.com/docs/en/sql-reference/statements/describe-table/ DESCRIBE TABLE | DESC};
 * @export
 * @param {string} dbName name of db (can be empty for default db)
 * @param {string} tbName name of  table
 * @param {object} options [{INTO, FORMAT}]
 * @returns {string} sqlStatement
 */
export const DESC = (dbName, tbName, { INTO, FORMAT = 'Pretty' } = {}) => `DESC ${nameSpaceOrTb(dbName, tbName)} ${strIfKV({ INTO, FORMAT })}`;

/**
 * EXISTS [TEMPORARY] [TABLE|DICTIONARY] [db.]name [INTO OUTFILE filename] [FORMAT format]
 * {@link https://clickhouse.com/docs/en/sql-reference/statements/exists/ EXISTS};
 * @export
 * @param {string} what [TABLE|DICTIONARY|VIEW] what to show
 * @param {string} dbName name of db (can be empty for default db)
 * @param {string} entityName name of entity (table view etc)
 * @param {object} options [{TEMPORARY, INTO, FORMAT}]
 * @returns {results} a results object '0\n' | '1\n' in body if no format specified
 */
export const EXISTS = (what = 'TABLE', dbName, entityName, { TEMPORARY, INTO, FORMAT } = {}) =>
  `EXISTS ${strIfK({ TEMPORARY })} ${what} ${nameSpaceOrTb(dbName, entityName)} ${strIfKV({ INTO, FORMAT })}`;

/**
 * {@link https://clickhouse.com/docs/en/sql-reference/statements/create/table/  CREATE TABLE};
 * @export
 * @param {string} dbName name of db (can be empty for default db)
 * @param {string} tbName name of table
 * @param {string} schema table schema
 * @param {string} engine table engine can be '' in case engine parms are included in schema
 * @param {object} options [{IF_NOT_EXISTS = true, ON_CLUSTER }] see options
 * @returns {results} a results object
 */
export const CREATE_TABLE_fromSchema = (dbName, tbName, schema, { ENGINE, TEMPORARY, IF_NOT_EXISTS = true, ON_CLUSTER } = {}) => `
  CREATE ${strIfK({ TEMPORARY })} TABLE ${strIfK({ IF_NOT_EXISTS })} ${nameSpaceOrTb(dbName, tbName)} ${strIfKV({ ON_CLUSTER })}
  ${schema}
  ${strIfKV({ ENGINE })}
  `;

/**
 * {@link https://clickhouse.com/docs/en/sql-reference/statements/create/table/  CREATE TABLE};
 * @export
 * @param {string} dbName name of db (can be empty for default db)
 * @param {string} tbName name of table
 * @param {string} dbNameOrg original dbName
 * @param {string} tbNameOrg original table name
 * @param {object} options [{IF_NOT_EXISTS = true, engine (if not same) }] see options
 * @returns {results} a results object
 */
export const CREATE_TABLE_fromTb = (dbName, tbName, dbNameOrg, tbNameOrg, { IF_NOT_EXISTS = true, ENGINE } = {}) => `
  CREATE TABLE ${strIfK({ IF_NOT_EXISTS })} ${nameSpaceOrTb(dbName, tbName)} AS ${nameSpaceOrTb(dbNameOrg, tbNameOrg)} 
  ${strIfKV({ ENGINE })}
  `;

// CREATE TABLE [IF NOT EXISTS] [db.]table_name[(name1 [type1], name2 [type2], ...)] ENGINE = engine AS SELECT
// CREATE TABLE t1 (x String) ENGINE = Memory AS SELECT 1

/**
 * {@link https://clickhouse.com/docs/en/sql-reference/statements/create/table/  CREATE TABLE};
 * @export
 * @param {string} dbName name of db (can be empty for default db)
 * @param {string} tbName name of table
 * @param {string} schema table schema
 * @param {string} engine table engine
 * @param {string} SELECT Select statement
 * @param {object} options [{IF_NOT_EXISTS = true}] options
 * @returns {results} a results object
 */
export const CREATE_TABLE_fromSelect = (dbName, tbName, schema, SELECT, { IF_NOT_EXISTS = true, ENGINE } = {}) => `
  CREATE TABLE ${strIfK({ IF_NOT_EXISTS })} ${nameSpaceOrTb(dbName, tbName)}
  ${schema}
  ${strIfKV({ ENGINE })}
  AS ${strKV({ SELECT })} 
  `;

/**
 * ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE WHERE filter_expr
 * {@link https://clickhouse.com/docs/en/sql-reference/statements/alter/delete/  ALTER TABLE DELETE};
 * @export
 * @param {string} dbName name of db (can be empty for default db)
 * @param {string} tbName name of table
 * @param {string} WHERE clause
 * @param {object} options [{ON_CLUSTER}] options
 * @returns {results} a results object
 */
export const ALTER_TABLE_DELETE = (dbName, tbName, WHERE, { ON_CLUSTER } = {}) =>
  `ALTER TABLE ${nameSpaceOrTb(dbName, tbName)} ${strIfKV({ ON_CLUSTER })} DELETE ${strIfKV({ WHERE })}`;

/**
 * ALTER TABLE [db.]table UPDATE column1 = expr1 [, ...] WHERE filter_expr
 * {@link https://clickhouse.com/docs/en/sql-reference/statements/alter/update/  ALTER TABLE UPDATE};
 * @export
 * @param {string} dbName name of db (can be empty for default db)
 * @param {string} tbName name of table
 * @param {Object} colEqObj i.e: { foo: 1, bar: 2 };
 * @param {string} WHERE clause
 * @param {object} options [{ON_CLUSTER}] options
 * @returns {results} a results object
 */
export const ALTER_TABLE_UPDATE = (dbName, tbName, colEqObj, WHERE, { ON_CLUSTER } = {}) =>
  `ALTER TABLE ${nameSpaceOrTb(dbName, tbName)} ${strIfKV({ ON_CLUSTER })} UPDATE ${strKOperatorV(colEqObj)} ${strIfKV({ WHERE })}`;
 
/**
 * OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL] [DEDUPLICATE [BY expression]]
 * {@link https://clickhouse.com/docs/en/sql-reference/statements/optimize/  OPTIMIZE TABLE};
 * @export
 * @param {string} dbName name of db (can be empty for default db)
 * @param {string} tbName name of table
 * @param {object} options see above link
 * @returns {results} a results object
 */
export const OPTIMIZE_TABLE = (dbName, tbName, { ON_CLUSTER, PARTITION, PARTITION_ID, FINAL, BY } = {}) =>
  `OPTIMIZE TABLE ${nameSpaceOrTb(dbName, tbName)} ${strIfKV({ ON_CLUSTER, PARTITION, PARTITION_ID })} ${strIfK({ FINAL })} ${strIfKV({ BY })}`;

/**
 * INSERT INTO [db.]table [(c1, c2, c3)] FORMAT Values (v11, v12, v13), (v21, v22, v23), ..
 * {@link https://clickhouse.com/docs/en/sql-reference/statements/insert-into/  INSERT INTO};
 * @export
 * @param {string} dbName name of db (can be empty for default db)
 * @param {string} tbName name of table
 * @param {string | Stream } data data to insert string | stream etc depending on FORMAT
 * @param {object} options [{IF_NOT_EXISTS = true, engine (if not same) columns if format is Values}] see options
 * @returns {results} a results object
 */
export const INSERT_INTO = (dbName, tbName, data, { columns = '', FORMAT = 'Values' } = {}) =>
  [`INSERT INTO ${nameSpaceOrTb(dbName, tbName)} ${columns} ${strIfKV({ FORMAT })}`, data];

/**
 * short cut as the most common case. @note use input_format_skip_unknown_fields
 * {@link https://clickhouse.com/docs/en/sql-reference/statements/insert-into/  INSERT INTO};
 * @export
 * @param {string} dbName name of db (can be empty for default db)
 * @param {string} tbName name of table
 * @param {string} data data to insert string | stream etc depending on FORMAT
 * @returns {results} a results object
 */
export const INSERT_INTO_JSONEachRow = (dbName, tbName, data) =>
  [`INSERT INTO ${nameSpaceOrTb(dbName, tbName)} FORMAT JSONEachRow}`, data];

/**
 * for compatibility / debugging etc
 * @export
 * @param {string} sqlStr sql statement
 * @param {any} [data] string | json | stream
 * @returns {results} an array [sqlSt , data] if data is defined else sqlStr
 */
export const rawSql = (sqlStr, data) => ((data === undefined) ? sqlStr : [sqlStr, data]);

// export { select } from './select.js';

/*

CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [NULL|NOT NULL] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    name2 [type2] [NULL|NOT NULL] [DEFAULT|MATERIALIZED|ALIAS expr2] [compression_codec] [TTL expr2],
    ...
) ENGINE = engine
export const truncateTable = (ns, ifExists = true) =>
  `TRUNCATE TABLE ${sqlFr.ifExists(ifExists)} ${ns}`;

export const existsRecord = (ns, where) =>
  `SELECT 'TRUE' FROM ${ns} WHERE ${where} LIMIT 1 FORMAT CSV`; // 'TRUE' is dummy Native is most efficient
export const countSimple = (ns, where = '', format = 'CSV') =>
  `SELECT count(*) FROM ${ns} ${where} FORMAT ${format}`;
export const fetch = (ns, where) =>
  `SELECT * FROM ${ns} WHERE ${where} FORMAT JSON`;
export const fetchLast = (ns, byField = '__createdAt', order = 'DESC') =>
  `SELECT ${byField} FROM ${ns} ORDER BY ${byField} ${order} LIMIT 1 FORMAT JSON`;

;
export const renameTables = (fromToPairArr, cluster = null) =>
  `RENAME TABLE ${fromToPairArr.map((x) => x.join(' TO ')).join(', ')}${amendCluster(cluster)}`;  // [ [ 'db1.tb1', 'db1.tb2' ], [ 'db1.tb2', 'db2.tb2' ] ]
// @warning firstOrLast withTies = true can result on vast amount of records if ORDER BY produces too many results
export const firstOrLast = (ns, orderBy, withTies = true) =>
  `SELECT * FROM ${ns} ORDER BY ${orderBy} LIMIT 1 ${sqlFr.clauseBool('WITH TIES', withTies)} FORMAT JSON`;
export const insertFromSelect = (ns, SELECT) =>
  `INSERT INTO ${ns} SELECT  ${SELECT}`;
export const alterTableDelete = (ns, WHERE = '1 = 2', cluster = null) =>
  `ALTER TABLE ${ns} ${amendCluster(cluster)} DELETE WHERE ${WHERE}`;
export const alterTableUpdate = (ns, updateObj, WHERE = '1 = 2') =>
  `ALTER TABLE ${ns} UPDATE${Object.keys(updateObj).map(k => ` ${k} = ${updateObj[k]}`)} WHERE ${WHERE}`;
export const tableCopy = (nsFrom, nsTo) =>
  insertFromSelect(nsTo, `* FROM ${nsFrom}`);

*/

/* todo
select 'exists' from rapchat_alt.ext_accountsLastSeen limit 1 FORMAT Native
export const existsRecord = (ns, WHERE, format = 'CSV') => `EXISTS TABLE ${ns} FORMAT ${format}`


  ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE WHERE filter_expr
  ALTER TABLE [db.]table UPDATE column1 = expr1 [, ...] WHERE filter_expr

  OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL] [DEDUPLICATE]

*/

/*

RENAME TABLE [db11.]name11 TO [db12.]name12, [db21.]name21 TO [db22.]name22, ... [ON CLUSTER cluster]

EXISTS [TEMPORARY] [TABLE|DICTIONARY] [db.]name [INTO OUTFILE filename] [FORMAT format]
CHECK TABLE [db.]name
SELECT count(*) FROM rapchat.accounts FORMAT CSV

(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [compression_codec] [TTL expr2],
    ...
) ENGINE = engine
----
copy structure CREATE TABLE [IF NOT EXISTS] [db.]table_name AS [db2.]name2 [ENGINE = engine]
insert into rc_bkp.events select * from rapchat_alt.events

*/

