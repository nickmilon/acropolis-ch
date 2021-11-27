/* eslint-disable no-await-in-loop */
/* eslint-disable no-inner-declarations */

import { Readable, Transform } from 'stream';
import { rxDefaultRowMach } from './transforms.js';

export class ReadableArrOrFn extends Readable {
  constructor(dataArrOrFn, { highWaterMark = 16384 } = {}) {
    super({ objectMode: true, encoding: 'utf8', highWaterMark });
    this._props = { status: 200, message: 'ok', dt: { Start: new Date(), End: new Date() } };
    if (Array.isArray(dataArrOrFn)) { // create a generator and use its next for dataFn function
      function* data() { yield* dataArrOrFn; }
      this.genFn = data();
      this.dataFn = () => {
        const dataNext = this.genFn.next();
        if (dataNext.done === false) { return dataNext.value; }
        return null;
      };
    } else { this.dataFn = dataArrOrFn; }
  }

  _read() { this.push(this.dataFn()); }
}

// https://nodejs.org/api/stream.html#transform_flushcallback
// https://nodejs.org/api/stream.html
export class TransformParseRaw extends Transform {
  constructor(formatOrRegex = 'JSONCompactEachRow', { highWaterMark = 16384, funcTrx } = {}) {
    super({ objectMode: true, encoding: 'utf8', highWaterMark });
    this._counters = { rows: 0, chunks: 0 };
    this._funcTrx = funcTrx;
    this._leftOver = '';
    this._rxRowMatch = (formatOrRegex instanceof RegExp) ? formatOrRegex : rxDefaultRowMach(formatOrRegex);
  }

  get counters() { return this._counters; }

  async _process(chunk, callback) {
    try {
      const data = this._leftOver + chunk;  // `${this._leftOver}${chunk}`;
      let row = null;
      let index = -1;
      const matchIterator = data.matchAll(this._rxRowMatch);
      // eslint-disable-next-line no-restricted-syntax
      for (const match of matchIterator) {
        this._counters.rows += 1;
        index = match.index;
        row = match.groups.row;
        // console.log(row);
        if (this._funcTrx === undefined) {
          this.push(row);
        } else {
          const out = await this._funcTrx(row);
          if (Array.isArray(out)) {
            out.forEach((el) => this.push(el));
          } else { this.push(out); }
        }
      }
      if (row === null) { // chunk is small so we got no match save data for next iteration or _flush
        this._leftOver = data;
      } else {
        this._leftOver = data.substring(index + row.length);
      }
      // console.dir({ counters: this._counters, chunk, data, index, row, leftOver: this._leftOver });
      return callback(null);
    } catch (err) {
      return callback(err);
    }
  }

  async _transform(chunk, encoding, callback) {
    this._counters.chunks += 1;
    await this._process(chunk, callback);
  }

  // https://nodejs.org/api/stream.html#transform_flushcallback
  _flush(callback) {
    if (this._leftOver !== '' && this._leftOver !== '\n') {
      // console.dir({ msg: 'got leftOver', leftOver: `[${this._leftOver}]` });
      return this._process(this._leftOver, callback);
    }
    return callback();
  }

  // https://nodejs.org/api/stream.html#writable_finalcallback
  // eslint-disable-next-line class-methods-use-this
  _final(callback) {
    return callback();
  }
}
