/* eslint-disable implicit-arrow-linebreak */
// import * as sqlFr from './fragments.js'
import { offsetLimit, formatOrJSON } from './fragments.js'


const mutual_del = (id1, id2, {tableNS, parentAlias, childAlias, LIMIT, OFFSET = 0, FORMAT } = {}) =>
  `
    SELECT parent AS ${parentAlias}, child AS ${childAlias}
    FROM ${tableNS}
    WHERE (parent = '${id1}' AND child = '${id2}') OR (parent = '${id2}' AND child = '${id1}')
    ORDER BY child ASC, parent
    ${offsetLimit({ LIMIT, OFFSET })}
    ${formatOrJSON(FORMAT)}
    `

const mutual = (id, { tableNS, mutualAlias, LIMIT, OFFSET = 0, FORMAT } = {}) =>
  `
  SELECT mutual AS ${mutualAlias}
    FROM 
    (
        SELECT
            count(*) AS count,
            if(parent = '${id}', child, parent) AS mutual
        FROM ${tableNS}
        WHERE (child = '${id}') OR (parent = '${id}')
        GROUP BY mutual
    )
  WHERE count = 2
  ORDER BY mutual ASC
  ${offsetLimit({ LIMIT, OFFSET })}
  ${formatOrJSON(FORMAT)}
  `

const areMutual = (id1, id2, {tableNS, FORMAT} = {}) =>
  `
  SELECT if(count(*) = '2', 1, 0) AS isMutual
  FROM ${tableNS}
  WHERE (parent = '${id1}' AND child = '${id2}') OR (parent = '${id2}' AND child = '${id1}')
  ${formatOrJSON(FORMAT)}
  `

const children = (idParent, {tableNS, childAlias, LIMIT, OFFSET = 0, FORMAT} = {}) =>
  `
  SELECT child As ${childAlias}
  FROM ${tableNS}
  WHERE (parent = '${idParent}')
  ORDER BY child ASC
  ${offsetLimit({ LIMIT, OFFSET })}
  ${formatOrJSON(FORMAT)}
  `
const parents = (idChild, {tableNS, parentAlias, LIMIT, OFFSET = 0, FORMAT} = {}) =>
  `
  SELECT parent As ${parentAlias}
  FROM ${tableNS}
  WHERE (child = '${idChild}')
  ORDER BY parent ASC
  ${offsetLimit({ LIMIT, OFFSET })}
  ${formatOrJSON(FORMAT)}
`

export {
  mutual,
  areMutual,
  children,
  parents,
};
