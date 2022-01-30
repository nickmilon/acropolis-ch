/**
 * @module sql/system
 * @overview
 * useful system sql statements
 * @exports tableSize columnsSize  partitions
 */

export const tableSize = (database = '%', table = '%') => `
SELECT
    database,
    table,
    formatReadableSize(sum(data_compressed_bytes) AS size) AS compressed,
    formatReadableSize(sum(data_uncompressed_bytes) AS usize) AS uncompressed,
    round(usize / size, 2) AS compress,
    sum(rows) AS rows,
    count() AS part_count
FROM system.parts
WHERE (active = 1) AND (table LIKE '${table}') AND (database LIKE '${database}')
GROUP BY database, table
ORDER BY size DESC
`;

export const columnsSize = (tableNS) => `
SELECT
    database,
    table,
    column,
    formatReadableSize(sum(column_data_compressed_bytes) AS size) AS compressed,
    formatReadableSize(sum(column_data_uncompressed_bytes) AS usize) AS uncompressed,
    round(usize / size, 2) AS compress
FROM system.parts_columns
WHERE (active = 1) AND (table LIKE '${tableNS}')
GROUP BY database, table, column
ORDER BY size DESC;
`;

export const partitions = (tableNS) => `
SELECT
    partition, name, active
FROM system.parts
WHERE table = '${tableNS}'
`;
