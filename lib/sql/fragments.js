/**
 * fragments module: various helpers that return sql fragments
 * @module sql/fragments
 * @typedef { nameTypeArr2d a 2d array of [name, typeCh] @example [ [ 'ID', 'UInt32' ], [ 'foo', 'string' ] ]
 * @typedef {string) sqlFragment a part of an sql statement/expression
 */

/**
 * @export
 * @param {Object} obj a key value pair where value is either true or false
 * @param {string} operator operator string
 * @param {string} joinStr final join string
 * @return {sqlFragment} an sql fragment with keys of object where underscores are replaced with spaces separated by sep
 * @example
 * strKOperatorV({ foo: 1, bar: '\"valueOfBar\"' }) > 'foo = 1, bar = "valueOfBar"'
 */
export const strKOperatorV = (obj, operator = ' = ', joinStr = ', ') =>
  Object.entries(obj).map(([k, v]) => `${k}${operator}${v}`).join(joinStr);

/**
 * @export
 * @param {Object} obj a key value pair where value is either true or false
 * @param {string} sep separator
 * @return {sqlFragment} an sql fragment with keys of object where underscores are replaced with spaces separated by sep
 */
export const strK = (obj, sep = ' ') =>
  Object.entries(obj).map(([k]) => `${k.replaceAll('_', ' ')}`).join(sep).trim();

/**
 * @export
 * @param {object} obj a key value pair where value is either true or false
 * @param {string} sep separator
 * @return {sqlFragment} an sql fragment with keys values of object where underscores in keys are replaced with spaces separated by sep
 */
export const strKV = (obj, sep = ' ') =>
  Object.entries(obj).map(([k, v]) => `${k.replaceAll('_', ' ')} ${v}`).join(sep).trim();

/**
 * @export
 * @param {object} obj a key value pair where value is either true or false
 * @param {string} sep = [' '] separator
 * @param {Function} filterFn function to filter
 * @return {sqlFragment} an sql fragment with keys of object where underscores are replaced with spaces separated by sep filtered by filterFn
 * @example
 * strIfK({'WITH_TIES': true}) > 'WITH TIES'
 */
export const strIfK = (obj, sep = ' ', filterFn = (x) => (x === true)) =>
  Object.entries(obj).filter(([, v]) => filterFn(v))
    .map(([k]) => `${k.replaceAll('_', ' ')}`).join(sep).trim();

/**
 * @export
 * @param {object} obj a key value pair where value is either true or false
 * @param {string} sep = [' '] separator
 * @param {Function} filterFn function to filter
 * @return {sqlFragment} an sql fragment with keys values of object where underscores are replaced with spaces separated by sep filtered by filterFn
 * @example <caption>Example usage of strIfKV</caption>
 * strIfKV({LIMIT: 10, OFFSET: 20}) > 'LIMIT 10 OFFSET 20'
 * strIfKV({LIMIT: undefined, OFFSET: 20}) > 'OFFSET 20'
 * strIfKV({'ON_CLUSTER': 'cluster1'}) > 'ON CLUSTER cluster1'
 */
export const strIfKV = (obj, sep = ' ', filterFn = (x) => (x !== undefined)) =>
  Object.entries(obj).filter(([, v]) => filterFn(v))
    .map(([k, v]) => `${k.replaceAll('_', ' ')} ${v}`).join(sep).trim();

// const dbNameDefault;
// const tbNameDefault;
/**
 * @export
 * @param {string} dbName database name
 * @param {string} tbName table name
 * @return {sqlFragment} an sql fragment 'dbName.tbName'
 * @example nameSpace('accountsDB', 'personsTB') > 'accountsDB.personsTB'
 */
export const nameSpace = (dbName, tbName) => `${dbName}.${tbName}`;

/**
 * @export
 * @param {string} dbName database name
 * @param {string} tbNameOrNS table name
 * @return {sqlFragment} an sql fragment '[dbName.]tbName'
 * @example
 * nameSpaceOrTb(undefined, 'personsTB') > 'personsTB'  nameSpaceOrTb(undefined, 'personsDb.personsTB') => 'personsDb.personsTB'
 */
export const nameSpaceOrTb = (dbName, tbNameOrNS) => (dbName ? nameSpace(dbName, tbNameOrNS) : tbNameOrNS);

/**
 * @export
 * @param {string} str sql string
 * @param {string} tbName table name
 * @return {sqlFragment} str with removed empty lines and white space at start-end
 */
export const sqlPrettify = (str) => str.replace(/^\s*\n|^ +|\s+$/gm, '');

/**
 * @export
 * @param {nameTypeArr2d} nameTypeArr2d  [key types array]
 * @returns {sqlFragment} sql fragment for table structure definition
 */
export const tbStruct = (nameTypeArr2d) => nameTypeArr2d.map(([k, v]) => `${k} ${v}`).map((x) => `\n\t${x}`).toString().replace('\n', '');

/**
 * @export
 * @param {nameTypeArr2d} nameTypeArr2d  [key types array]
 * @returns {sqlFragment} sql fragment for table structure definition as tbStruct but includes (parentheses)
 */
export const tbStructParens = (nameTypeArr2d) => `(\n${tbStruct(nameTypeArr2d)}\n)`;

/**
 * @export
 * @param {sqlFragment} structStr table structure fragment
 * @returns {nameTypeArr2d} reverse of tbStructParens
 */
export const tbStructParse = (structStr) => structStr.trim().replace(/^\(|\)$/g, '').split(',').map((x) => x.trim().split(' '));

/**
 * @export
 * @param {sqlFragment} structStr table structure fragment
 * @returns {Object} {column_name: type, ...}
 */
export const tbStructParseObj = (structStr) => Object.fromEntries(tbStructParse(structStr));

/**
 * @export
 * @param {integer} vector table structure fragment
 * @param {integer} orderBy table structure fragment
 * @returns {Object} {order, limit}
 */
export const vectorToOrderLimit = (vector, orderBy = 'id') => {
  if (Number.isInteger(vector)) {
    return { order: `ORDER BY ${orderBy} ${(Math.sign(vector) === 1) ? 'ASC' : 'DESC'}`, limit: `LIMIT ${Math.abs(vector)}` };
  }
  return { order: '', limit: '' };
};

/**
 * @param {undefined|integer} LIMIT rows
 * @param {integer} OFFSET rows
 * @returns {sqlFragment} LIMIT X OFFSET Y
 * @example
 * limitOffset(undefined, 10) => 'OFFSET 10'
 * limitOffset(20, 10) => 'LIMIT 20 OFFSET 10'
 */
export const limitOffset = (LIMIT, OFFSET) => `${strIfKV({ LIMIT, OFFSET })}`;

/**
 * @param {undefined|String} FORMAT format defaults to JSON
 * @returns {sqlFragment} FORMAT xxxx or JSON if undefined
 */
export const formatOrJSON = (FORMAT) => `FORMAT ${FORMAT || 'JSON'}`;
