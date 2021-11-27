/* eslint-disable object-property-newline */
// <!--cSpell:disable -->

/* eslint-disable no-underscore-dangle */

import { strKOperatorV, strKV, strIfK, strIfKV, vectorToOrderLimit, nameSpaceOrTb } from '../sql/fragments.js';
import { limitOffset, SELECT, SELECTraw } from '../sql/select.js';

export const graphSql = (graphInst) => {
  // eslint-disable-next-line no-unused-vars
  const { dbName, idType, parentAlias, childAlias, mutualAlias, graphNs } = graphInst._props;
  const { ns } = graphInst;

  /**
   *
   * ['child', 'parent'] optimizes for polling parents for activities
   * ['parent', 'child'] is preferable for pushing activities to children
   * any way both are covered with a secondary Key but still better to make proper selection here
   * @param {Array} [primaryKeyArr=['child', 'parent']] an array defining primary key
   * @return {str} sql string
   */
  const structEdges = (primaryKeyArr = ['child', 'parent']) => {
    const primaryKey = `(${['type', ...primaryKeyArr].join(', ')})`;
    const secondaryKey = `(${['type', ...primaryKeyArr.reverse()].join(', ')})`;
    return `
    CREATE TABLE ${ns.edges}
      (
        feed UInt16,
        parent ${idType},
        child ${idType},
        sign Int8,
        dtCr DateTime,
        pcSum UInt64 MATERIALIZED parent + child,
        PROJECTION proj_reversedPrimary ( SELECT * ORDER BY ${secondaryKey} ),
        PROJECTION proj_parents
        (
          SELECT 
            parent,
            sum(sign) AS children
          GROUP BY parent
        ),
        PROJECTION proj_children
        (
          SELECT 
            type, child,
            sum(sign) AS parents
          GROUP BY child
        )
      )
      ENGINE = MergeTree
      -- PARTITION BY parent % 10  *unremark if you want to control partition
      PRIMARY KEY ${primaryKey}
      ORDER BY ${primaryKey}      
      SAMPLE BY parent
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

 
  const children = (parent, vector, scroll = '', { FORMAT = 'JSONCompact' } = {}) => {
    const ol = vectorToOrderLimit(vector, 'parent');
    return `
    SELECT child
    FROM ${ns.edges}
    WHERE feed = ${feed} AND (parent = ${parent}) ${scroll}
    GROUP BY child
    HAVING sum(sign) = 1
    ${ol.order}
    ${ol.limit}
    FORMAT ${FORMAT}
    `;
  };

  const parents = (child, vector, scroll = '', { feed = 0, FORMAT = 'JSONCompact' } = {}) => {
    const ol = vectorToOrderLimit(vector, 'parent');
    return `
    SELECT parent
    FROM ${ns.edges}
    WHERE feed = ${feed} AND (child = ${child}) ${scroll}
    GROUP BY parent
    HAVING sum(sign) = 1
    ${ol.order}
    ${ol.limit}
    FORMAT ${FORMAT}
    `;
  };

  const top = (parentOrChild = 'parent', LIMIT = 10) => `
      SELECT
        count(*) AS total, ${parentOrChild}
        FROM ${graphNs}
        GROUP BY (${parentOrChild})
        ORDER BY total DESC
        ${limitOffset({ LIMIT, OFFSET: 0 })}
    `;

  return {
    structNodes, structEdges, structNodesSim,
    children, parents, top,
  };
};
