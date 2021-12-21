/* eslint-disable object-property-newline */
// <!--cSpell:disable -->

/* eslint-disable no-underscore-dangle */

import { strKOperatorV, strKV, strIfK, strIfKV, vectorToOrderLimit, nameSpaceOrTb } from '../sql/fragments.js';
import { limitOffset, SELECT, SELECTraw } from '../sql/select.js';

export const graphSql = (graphInst) => {
  // eslint-disable-next-line no-unused-vars
  const { dbName, idType, parentAlias, childAlias, mutualAlias } = graphInst._props;
  const { ns } = graphInst;
 
  /**
   *
   * primary = 'child' optimizes for polling parents for activities
   * primary = 'parent', is preferable for pushing activities to children
   * any way both are covered with a secondary Key but still better to make proper selection here
   * @param {String} [primary='child', part of primary key
   * @return {String} sql string
   */
  const structEdges = (primary = 'child') => {
    const secondary = (primary === 'child') ? 'parent' : 'child';
    const primaryKey = `(${['type', primary, secondary, 'dtCr'].join(', ')})`;
    const secondaryKey = `(${['type', secondary, primary, 'dtCr'].join(', ')})`;
    return `
    CREATE TABLE ${ns.edges}
      (
        type UInt16,
        parent ${idType},
        child ${idType},
        sign Int8,
        dtCr DateTime,
        pcSum UInt64 MATERIALIZED parent + child,
        PROJECTION proj_reversedPrimary ( SELECT * ORDER BY ${secondaryKey} ),
        PROJECTION proj_parents  ( SELECT type, child,  sum(sign),toDate(dtCr) GROUP BY type, child, dtCr ),
        PROJECTION proj_children ( SELECT type, parent, sum(sign),toDate(dtCr) GROUP BY type, parent, dtCr ),
      ENGINE = MergeTree
      PARTITION BY toYYYYMM(dtCr)
      PRIMARY KEY ${primaryKey}
      ORDER BY ${primaryKey}      
      SAMPLE BY ${primary}
      SETTINGS index_granularity = 8192
    `;
  };

  const structNodes = () => `
    CREATE TABLE ${ns.nodes}
      (
        id ${idType},
        dtCr DateTime,
        name String(20)
      )
      ENGINE = MergeTree()
      PRIMARY KEY id
      ORDER BY id
      SETTINGS index_granularity = 8192
    `;

  const structNodesSim = () => `
      CREATE TABLE ${ns.nodesSim}
      (
        id ${idType}, 
        dtCr DateTime,
        expectC Float32,
        expectP Float32
      )
      ENGINE = MergeTree()
      ORDER BY (id, dtCr)
      SETTINGS index_granularity = 8192 
      `;

  const children = (parent, vector, scroll = '', { type = 0, FORMAT = 'JSONCompact' } = {}) => {
    const ol = vectorToOrderLimit(vector, 'child');
    return `
    SELECT child
    FROM ${ns.edges}
    WHERE type = ${type} AND (parent = ${parent}) ${scroll}
    GROUP BY child
    HAVING sum(sign) = 1
    ${ol.order}
    ${ol.limit}
    FORMAT ${FORMAT}
    `;
  };

  const mutual = (target, { type = 0, FORMAT = 'JSONCompact' } = {}) => `
    WITH ${target} AS target
    SELECT parent AS mutual
    FROM ${ns.edges}
    WHERE (type = 0) AND (child = target)
    GROUP BY parent
    HAVING sum(sign) = 1
    INTERSECT
    SELECT child AS mutual
    FROM ${ns.edges}
    WHERE (type = 0) AND (parent = target)
    GROUP BY child
    HAVING sum(sign) = 1
    FORMAT ${FORMAT}
    `;

  const parents = (child, vector, scroll = '', { type = 0, FORMAT = 'JSONCompact' } = {}) => {
    const ol = vectorToOrderLimit(vector, 'parent');
    return `
    SELECT parent
    FROM ${ns.edges}
    WHERE type = ${type} AND (child = ${child}) ${scroll}
    GROUP BY parent
    HAVING sum(sign) = 1
    ${ol.order}
    ${ol.limit}
    FORMAT ${FORMAT}
    `;
  };

  const parentsCount = (child, { type = 0, FORMAT = 'JSONCompact' } = {}) => `
    SELECT 
      sum(sign) AS count 
      FROM ${ns.edges}
      WHERE type = ${type} AND child = ${child}
      FORMAT ${FORMAT}
  `;

  const childrenCount = (parent, { type = 0, FORMAT = 'JSONCompact' } = {}) => `
    SELECT 
      sum(sign) AS count 
      FROM ${ns.edges}
      WHERE type = ${type} AND parent = ${parent}
      FORMAT ${FORMAT}
  `;

  const histogram = (parent, { type = 0, WITH_FILL = false, FORMAT = 'JSONCompact' } = {}) => `
    SELECT
      type,
      parent,
      toYYYYMM(dtCr) AS YearMonth,
      sum(sign) AS count,
      -- bar(count, 100, 300)
    FROM ${ns.edges}
    WHERE parent = type = ${type} AND ${parent}
    GROUP BY
        type,
        parent,
        YearMonth
    ORDER BY YearMonth ${strIfK(WITH_FILL)}
    FORMAT ${FORMAT}
  `;

  const top = (parentOrChild = 'parent', LIMIT = 10) => `
      SELECT
        count(*) AS total, ${parentOrChild}
        FROM ${ns.edges}
        GROUP BY (${parentOrChild})
        ORDER BY total DESC
        ${limitOffset({ LIMIT, OFFSET: 0 })}
    `;

  return {
    structNodes, structEdges, structNodesSim,
    children, parents, parentsCount, childrenCount, histogram, top,
  };
};
