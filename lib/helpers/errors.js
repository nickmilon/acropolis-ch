// import { ErrAcropolis } from 'acropolis-nd/lib/Hamartia.js';

/**
 * error classes
 * @module
 */

/**
 * !! just replace static errorDict() in subclass when inherit
 * @class ErrAcropolis
 * @param {number} code number to be extracted from errorDict
 * @param {string} [extraMsg=''] any extra string to be amended to message
 * @extends {Error}
 */
class ErrAcropolis extends Error {
  constructor(code, extraMsg = '', args = {}) {
    super();
    this.message = this.constructor.errorMsg(code, extraMsg);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
    this.code = code;
    this.args = args;
  }

  static errorDict() { // replace errorDict() in subclass
    return {
      1000: 'Dummy_Error',
    };
  }

  static errorMsg(code, extraMsg) {
    return `${this.errorDict()[code]} ${extraMsg}`;
  }
}

/**
 * this class is for use in this library only do not subclass it use @see ErrAcropolis instead
 * @private
 * @class ErrAcropolisCH
 * @extends {ErrAcropolis}
 */
class ErrAcrCH extends ErrAcropolis {
  constructor(code, extraMsg = '', args = {}) {
    super(code, extraMsg, args);
  }

  static errorDict() {
    return {
      4001: 'clickhouse returned statusCode: ',
      4011: 'this format is not supported: ',
    };
  }
}

export { ErrAcrCH };
