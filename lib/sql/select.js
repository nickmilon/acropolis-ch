/* eslint-disable no-shadow */
/* eslint-disable arrow-body-style */

/**
 * select sql helper
 * @module
 */


import { strIfK } from './fragments.js';

/**
 * a single version of select
 * {@link https://clickhouse.com/docs/en/sql-reference/statements/select/ SELECT};
 * @exportsFix
 * @param {string} expList Expression
 * @param {Object} options Select {DISTINCT: true|false}
 * @returns {string} sql statement
 */
export const SELECTraw = (expList, { DISTINCT = false } = {}) => {
  return `SELECT ${(DISTINCT === true) ? 'DISTINCT' : ''}  ${expList}`;
};

/**
 * {@link https://clickhouse.com/docs/en/sql-reference/statements/select/limit/ LIMIT};
 * @exportsFix
 * @param {integer|undefined} LIMIT limit
 * @param {integer|undefined} OFFSET offset
 * @param {boolean|undefined} WITH_TIES true | false
 * @returns {string} LIMIT n,m if limit else OFFSET m
 */
export const limitOffset = (LIMIT, OFFSET, WITH_TIES = false) => {
  if (Number.isInteger(LIMIT)) return `LIMIT ${Number.isInteger(OFFSET) ? OFFSET : 0},${LIMIT} ${strIfK({ WITH_TIES })}`;
  if (Number.isInteger(OFFSET)) return `OFFSET ${OFFSET}`; // ignore WITH TIES it only aplites to n, m format
  return '';
};

// @privet
const strIfKVMulti = (obj, sep = ' ', filterFn = (x) => (x !== undefined)) => {
  return Object.entries(obj).filter(([, v]) => filterFn(v))
    .map(([k, v]) => {
      if (v.startsWith(k) || v.startsWith('OFFSET')) { return v; }
      return `${k.replaceAll('_', ' ')} ${v}`;
    }).join(sep).trim();
};

/**
 * {@link https://clickhouse.com/docs/en/sql-reference/statements/select/ SELECT};
 * @exportsFix
 * @param {string} SELECT Expression
 * @param {Object} options Select {DISTINCT: true|false all others values (if any) must be strings of functions that return string}
 * @returns {string} sql statement
 */
export const SELECT = (SELECT, {
  DISTINCT,
  WITH,
  FROM,
  SAMPLE,
  ARRAY_JOIN,
  GLOBAL,
  PREWHERE,
  WHERE,
  GROUP_BY,
  HAVING,
  ORDER_BY,
  LIMIT_BY, // !!: LIMIT BY is not related to LIMIT. They can both be used in the same query.
  LIMIT,
  SETTINGS,
  UNION,
  INTO_OUTFILE,
  FORMAT = 'JSON',
} = {}) => {
  const opts = {
    WITH,
    SELECT: `${(DISTINCT === true) ? 'DISTINCT' : ''} ${SELECT}`,
    FROM,
    SAMPLE,
    ARRAY_JOIN,
    GLOBAL,
    PREWHERE,
    WHERE,
    GROUP_BY,
    HAVING,
    ORDER_BY,
    LIMIT_BY,
    LIMIT,
    SETTINGS,
    UNION,
    INTO_OUTFILE,
    FORMAT,
  };
  return strIfKVMulti(opts, '\n');
};

/*
  [WITH expr_list|(subQuery)]
  SELECT [DISTINCT [ON (column1, column2, ...)]] expr_list
  [FROM [db.]table | (subQuery) | table_function] [FINAL]
  [SAMPLE sample_coeff]
  [ARRAY JOIN ...]
  [GLOBAL] [ANY|ALL|ASOF] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI] JOIN (subquery)|table (ON <expr_list>)|(USING <column_list>)
  [PREWHERE expr]
  [WHERE expr]
  [GROUP BY expr_list] [WITH ROLLUP|WITH CUBE] [WITH TOTALS]
  [HAVING expr]
  [ORDER BY expr_list] [WITH FILL] [FROM expr] [TO expr] [STEP expr]
  [LIMIT [offset_value, ]n BY columns]
  [LIMIT [n, ]m] [WITH TIES]
  [SETTINGS ...]
  [UNION  ...]
  [INTO OUTFILE filename]
  [FORMAT format]
*/
