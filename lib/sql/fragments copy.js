const clauseOrStr = (name, value, str = '') => ((value) ? `${name} ${value}` : `${str}`)  // caller should space separate fragment from rest of sql
const clauseBool = (parm, trueOrFalse) => ((trueOrFalse === true) ? ` ${parm} ` : '')     // no need to caller space separate

const clauseObj = obj => Object.entries(obj).map(([k, v]) => clauseOrStr(k, v)).join(' ').trim() //

const ifExists = trueOrFalse => clauseBool('IF EXISTS', trueOrFalse)
const ifNotExists = trueOrFalse => clauseBool('IF NOT EXISTS', trueOrFalse)
const onCluster = CLUSTER => clauseOrStr('ON CLUSTER', CLUSTER)
const offsetLimit = ({ LIMIT, OFFSET = 0 } = {}) => `${clauseObj({LIMIT, OFFSET})}`
const format = FORMAT => clauseObj({FORMAT})
const formatOrJSON = FORMAT => `FORMAT ${FORMAT ? FORMAT : 'JSON'}`


export {
  clauseOrStr,
  clauseBool,
  clauseObj,
  offsetLimit,
  ifExists,
  ifNotExists,
  onCluster,
  format,
  formatOrJSON,
}

