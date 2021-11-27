/* eslint-disable no-warning-comments */
/* eslint-disable capitalized-comments */

import * as sqlFr from './fragments.js';

export const amendCluster = cluster => sqlFr.clauseOrStr(' ON CLUSTER', cluster);

export const insertJSONEachRow = (ns) =>
  `INSERT INTO ${ns} FORMAT JSONEachRow`;
export const truncateTable = (ns, ifExists = true) =>
  `TRUNCATE TABLE ${sqlFr.ifExists(ifExists)} ${ns}`;
export const showCreateTable = ns =>
  `SHOW CREATE TABLE ${ns} FORMAT CSV`; // csv coz doesn't include /n
export const showCreateDatabase = (dbName) =>
  `SHOW CREATE DATABASE ${dbName}`;
export const dropTable = (ns, ifExists = true) =>
  `DROP TABLE ${sqlFr.ifExists(ifExists)} ${ns}`;
export const existsTable = (ns, format = 'CSV') =>
  `EXISTS TABLE ${ns} FORMAT ${format}`;
export const existsRecord = (ns, where) =>
  `SELECT 'TRUE' FROM ${ns} WHERE ${where} LIMIT 1 FORMAT CSV`; // 'TRUE' is dummy Native is most efficient
export const countSimple = (ns, where = '', format = 'CSV') =>
  `SELECT count(*) FROM ${ns} ${where} FORMAT ${format}`;
export const fetch = (ns, where) =>
  `SELECT * FROM ${ns} WHERE ${where} FORMAT JSON`;
export const fetchLast = (ns, byField = '__createdAt', order = 'DESC') =>
  `SELECT ${byField} FROM ${ns} ORDER BY ${byField} ${order} LIMIT 1 FORMAT JSON`;
export const createTable = (ns, structure, ifNotExists = true) =>
  `CREATE TABLE ${sqlFr.ifNotExists(ifNotExists)} ${ns} ${structure} FORMAT JSON`;
export const createDatabase = (dbName, ifNotExists = true, cluster = null) =>
  `CREATE DATABASE ${sqlFr.ifNotExists(ifNotExists)} ${dbName} ${_amendCluster(cluster)} FORMAT JSON`;
export const copyTableStructure = (fromNS, toNS, ifNotExists = true, engine = '') =>
  `CREATE TABLE ${sqlFr.ifNotExists(ifNotExists)} ${toNS} AS ${fromNS} ${engine} FORMAT JSON`;
export const renameTables = (fromToPairArr, cluster = null) =>
  `RENAME TABLE ${fromToPairArr.map((x) => x.join(' TO ')).join(', ')}${_amendCluster(cluster)}`;  // [ [ 'db1.tb1', 'db1.tb2' ], [ 'db1.tb2', 'db2.tb2' ] ]
// @warning firstOrLast withTies = true can result on vast amount of records if ORDER BY produces too many results
export const firstOrLast = (ns, orderBy, withTies = true) =>
  `SELECT * FROM ${ns} ORDER BY ${orderBy} LIMIT 1 ${sqlFr.clauseBool('WITH TIES', withTies)} FORMAT JSON`;
export const insertFromSelect = (ns, SELECT) =>
  `INSERT INTO ${ns} SELECT  ${SELECT}`;
export const alterTableDelete = (ns, WHERE = '1 = 2', cluster = null) =>
  `ALTER TABLE ${ns} ${_amendCluster(cluster)} DELETE WHERE ${WHERE}`;
export const alterTableUpdate = (ns, updateObj, WHERE = '1 = 2') =>
  `ALTER TABLE ${ns} UPDATE${Object.keys(updateObj).map(k => ` ${k} = ${updateObj[k]}`)} WHERE ${WHERE}`;
export const tableCopy = (nsFrom, nsTo) =>
  insertFromSelect(nsTo, `* FROM ${nsFrom}`);


/* todo
select 'exists' from rapchat_alt.ext_accountsLastSeen limit 1 FORMAT Native
export const existsRecord = (ns, WHERE, format = 'CSV') => `EXISTS TABLE ${ns} FORMAT ${format}`

  DESCRIBE TABLE rapchat_alt.account
  ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE WHERE filter_expr
  ALTER TABLE [db.]table UPDATE column1 = expr1 [, ...] WHERE filter_expr

  OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL] [DEDUPLICATE]

*/


/*
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster] [ENGINE = engine(...)]
RENAME TABLE [db11.]name11 TO [db12.]name12, [db21.]name21 TO [db22.]name22, ... [ON CLUSTER cluster]

EXISTS [TEMPORARY] [TABLE|DICTIONARY] [db.]name [INTO OUTFILE filename] [FORMAT format]
CHECK TABLE [db.]name
SELECT count(*) FROM rapchat.accounts FORMAT CSV
SELECT * FROM rapchat.accounts ORDER BY __createdAt DESC LIMIT 1
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [compression_codec] [TTL expr2],
    ...
) ENGINE = engine
----
copy structure CREATE TABLE [IF NOT EXISTS] [db.]table_name AS [db2.]name2 [ENGINE = engine]
insert into rc_bkp.events select * from rapchat_alt.events

*/

