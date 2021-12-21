/* eslint-disable no-await-in-loop */
import { UndiciCH } from './client.js';
import { DROP_TABLE, CREATE_TABLE_fromSchema } from './sql/basic.js';
import { objToSchema, JSONstringifyCustom } from './helpers/transforms.js';

import { functionsUDF } from './sql/functionsUDF.js';

export class ClientExt extends UndiciCH {
  constructor(uri, credentials = {}, {
    connections = 10,
    name = 'ClientExt',
  } = {}) {
    super(uri, credentials, { connections, name });
  }

  async addFunctionsUDF() {
    // eslint-disable-next-line no-restricted-syntax
    for (const fnUDF of Object.values(functionsUDF)) { await this.client.request(fnUDF); }
  }

  /**
   * Convenient but dirty way to create a table where schema is inferred from 1st row of data array
   * ONLY to be used short lived tables as no primary key can be defined
   * @param {String} dbName or undefined for memory tables
   * @param {String} tbName table Name
   * @param {Array} arr data
   * @param {string} [engine='Memory'] CH engine to use
   * @returns {Object}  request Result
   * @memberof ClientExt
   */
  async quickTable(dbName, tbName, arr, engine = 'Memory') {
    const schema = objToSchema(arr[0]);
    await this.request(CREATE_TABLE_fromSchema(tbName, tbName, schema, engine));
  }

  /**
   * Quick and dirty procedure to insert data from a js array into a memory table that is created on the spot,
   * optionally executing an sql against those data and returning results on a single step
   * if no sql is provided then caller is responsible to drop the table when no logger needed.
   * @param {String} tbName table name @warning table will be DROPPED if already exists! 
   * @param {Array} arr array of Jso
   * @param {String} sql optional if provided sql is run and table is dropped
   * @return {any}  results from applying sql to table or table name if no sql is provided 
   * @memberof ClientExt
   */
  async quickTableMemory(tbName, arr, sql) {
    await this.request(DROP_TABLE(undefined, tbName));
    await this.quickTable(undefined, tbName, arr, 'Memory');
    await this.request(`INSERT INTO ${tbName} FORMAT JSONEachRow`, JSONstringifyCustom(arr));
    if (sql !== undefined) {
      const sqlRes = await this.request(sql);
      this.request(DROP_TABLE(undefined, tbName));
      return sqlRes;
    }
    return tbName;
  }
}
