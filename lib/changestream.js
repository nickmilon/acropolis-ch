/* eslint-disable camelcase */
/* eslint-disable max-lines-per-function */
import { promisify } from 'util';

import { Writable, Transform, pipeline, finished } from '../../legacy-export.js';
import { sleepMs } from 'acropolis-nd/hellas/delphi.js';

import { PipelineHandler } from '../../modules/code_to_move.js'
import { Timestamp } from 'acropolis-mg/legacy-export.js';
import { inspectObj } from '../app-functions.js';
import { convertMS } from 'acropolis-nd/hellas/athena.js';


const pipelineAsync = promisify(pipeline)


// eslint-disable-next-line brace-style
class DummyWritable extends Writable { constructor() { super({ objectMode: true, encoding: 'utf8', highWaterMark: 128 });}

  // eslint-disable-next-line class-methods-use-this
  async _write(doc, enc, next) { next();}
}


 

/**
 * changeStream
 */
class ChangeStream extends Transform {

  /**
   * @todo move it to ch driver
   * @param {*} mgClientInst
   * @param {*} ns
   * @param {date | object | integer } startFrom
   * @param {*} watchOpts {maxAwaitTimeMS, fullDocument, resumeAfter, startAtOperationTime,  batchSize, collation, readPreference }
   * @note operationType one of insert, update, replace, delete or invalidate
   * @todo handle this: Uncaught Exception: CollectionScan died due to position in capped collection being deleted. Last seen record id: RecordId(6898495648140623911)
   *      Occurs when we lag oplog by a lot
   *      RecordId is kind of a date id= RecordId >> 32  new Date( id * 1000 ) except RecordID is too big to be right shifted
   *
   */
  constructor(mgClientInst, ns, { pl = [], pipeTo = [new DummyWritable()], watchOpts = { fullDocument: 'updateLookup'}, name = 'CS01', startFrom, funcTrxDoc = doc => doc, funcIncDoc = doc => true, role = 'primary' } = {}) {
    super({ objectMode: true, highWaterMark: 512 }); // don't encoding: 'utf8' unless readable expects string (.push will produce an error)
    this._props = { ns, mgClientInst, pl, pipeTo, watchOpts, name, startFrom, funcTrxDoc, funcIncDoc, _id: `ChangeStream_${name}` };
    this._control = { docCount: 0, status: 'created', role, lagMs: 0, lagStr: convertMS(0), dtLast: new Date()};
    this._lastData = null;
    const plDefault = [{'$addFields': { _nm_: { clusterDt: { $toDate: { $dateToString: {date: '$clusterTime'}}} } } }]
    this._props.pl = [...this._props.pl, ...plDefault];
    this._plResults = { success: true, statusCode: 200, error: null}
  }

  async _startFromToWatchOpts(startFrom, watchOpts = this._props.watchOpts) { // should be called from error/catch block
    const rt = {};

    if (Number.isInteger(startFrom)) {
      const skip = Math.max(2000, startFrom) // allow for dropped entries while establishing values
      const opLog = this._props.mgClientInst.opLogCollection;
      const ts = await opLog.findOne({}, { skip, projection: { ts: 1 } })
      if (ts === null) {throw new Error('ChangeStream, opLog entry not Found')}
      rt.startAtOperationTime = ts.ts;
    } else if (startFrom && typeof startFrom.getMonth === 'function') {  // it is a Date
      if (isNaN(startFrom.getTime())) { throw new Error('_startFromToWatchOpts: invalid Date') }
      rt.startAtOperationTime = new Timestamp(1, startFrom.getTime() / 1000)
      // inspectObj({ startDtTS: new Date(rt.startAtOperationTime.getHighBits() * 1000) })
    } else if (startFrom && startFrom._id !== undefined) {
      rt.resumeAfter = startFrom._id;
    }
    return {...watchOpts, ...rt};
  }

  streamFinish(aStream) {
    finished(aStream, (err) => {
      if (err) {
        this.log(`Stream: ${aStream.constructor.name} Error ${JSON.stringify(err)}`, 'error')
        this._plResults = { success: false, error: err, statusCode: 501}
      } else {
        this.log(`Stream: ${aStream.constructor.name} Finished smoothly`, 'info')
      }
    });
  }

  async start({ startFrom = this._props.startFrom, opsCounter, progressOnMs } = {}) { // coz mgClientInst may have not been instantiated yet
    const { watchOpts, pipeTo } = this._props;
    try {
      const watchOptsCurrent = await this._startFromToWatchOpts(startFrom, watchOpts) // clone so props_.watchOpts remains intact
      const startAtOperationTimeAsDate = (watchOptsCurrent.startAtOperationTime) ? new Date(watchOptsCurrent.startAtOperationTime.getHighBits() * 1000).toUTCString() : ''
      this.log(`starting at opLogTime:${startAtOperationTimeAsDate} watchOpts: ${JSON.stringify(watchOptsCurrent)}`, 'info')
      this._control.status = 'starting'
      this._control.dtStart = new Date();
      this._control.clusterDtStart = NaN;
      this._control.progressOnMs = progressOnMs;
      if (opsCounter) {this._control.opsCounter = opsCounter} // extra thing to be included in logs
      if (this._control.progressOnMs) { this.progress() }
      const [db, coll] = this._props.ns.split('.')
      const watchDbOrColl = (coll === undefined) ? this._props.mgClientInst.getDb(db) : this._props.mgClientInst.getCollection(db, coll)
      const changeStream = watchDbOrColl.watch(this._props.pl, watchOptsCurrent)
      this._changeStream = changeStream;
      this._control.status = 'started';
      const pipelinesArr = [changeStream, this, ...pipeTo]
      pipelinesArr.forEach(streamElement => this.streamFinish(streamElement));
      await pipelineAsync(pipelinesArr)
      this.close(this.close('END success'))
      return this._plResults
    } catch (err) {
      this.log(`Error ${err.message} Details: ${JSON.stringify(err)}|watch`, 'error');
      this._plResults = { success: false, error: err, statusCode: 502}
      await sleepMs(100);
      this.close(`startCatchError${err}`)
      return this._plResults
     }
  }

  async progress(data = this._lastData) {

    if (data) {
      const dt = new Date();
      this._control.lagMs = dt - data._nm_.clusterDt;
      if (isNaN(this._control.clusterDtStart)) {this._control.clusterDtStart = data._nm_.clusterDt}
      this._control.runOpLog = data._nm_.clusterDt - this._control.clusterDtStart// opLog Time covered since start
      this._control.runMs = dt - this._control.dtStart;
      this._control.runGain = (this._control.runOpLog / this._control.runMs).toFixed(2) // must be > 1 if we are ever going to catch up;
      this._control.runRemainingMs = Math.round(this._control.lagMs / this._control.runGain)
      this._control.runRemainingStr = convertMS(this._control.runRemainingMs)
      this._control.lagStr = convertMS(this._control.lagMs);
      // inspectObj({control: this._control})
      this.log('progress');
      this._control.dtLast = dt;
    }
    if (Number.isInteger(this._control.progressOnMs)) {setTimeout(this.progress.bind(this), this._control.progressOnMs)}
  }

  async _transform(data, encoding, callback) {
    try {
      this._control.docCount += 1;
      this._lastData = data;
      // data._nm_ = {...data._nm_, ...this._control}
      if (this._props.funcIncDoc(data)) { // selective push so we can exclude documents in real time if necessary (i.e check for duplicates)
        this.push(this._props.funcTrxDoc(data))
      }
      return callback();
    } catch (err) {
      await this.close(`transform_error:${err}`);
      await sleepMs(200);                           // give it a chance to end stream with what we have up to now otherwise stream closes with transfer closed with outstanding read data remaining
      return callback(err)                          // signal the error anyway
    }
  }

  async log(msg, level = 'info', logger = this._props.mgClientInst.logger) {
    if (logger) {
      logger[level](`|${this._props.name}|${msg}|control:${JSON.stringify(this._control)}`)
    }
  }

  async destroy(msg = '') {this.close(`destroy:${msg}|`)}

  async close(reason = 'unknown') {
    const isNotClosing = !this._control.status.startsWith('clos')
    if (isNotClosing) {
      // console.log('eeeeeeeeeeeeeeeee', {reason, status: this._control.status })
      this._control.dtNow = new Date();
      this._control.status = `closing: ${reason}`
      this.log('', 'warn')
      if (this._changeStream !== undefined) {
        this.push(null)
        this._changeStream.close();
        this._changeStream = undefined;
      }
      this._control.status = `closed: ${reason}`
    }
  }

  async end(msg = '') { this.close(`end${msg}|`)}

}

class StreamTrx extends Transform {
  constructor({ name = 'StreamTrx', highWaterMark = 2048, funcTrxDoc = doc => doc, dtStart = new Date(), logger, logSec = 60 } = {}) {
    super({ objectMode: true, encoding: 'utf8', highWaterMark });
    this._props = { name, funcTrxDoc, docLast: null };
    if (logger) {
      this._props.logger = logger
      this._props.interval = setInterval(() => { this.log('cnt')}, logSec * 1000);
    }
    this._metadata = { docCount: 0, status: 200, message: 'ok', dt: { Start: dtStart, End: new Date() } };
  }

  async _transform(data, encoding, callback) {
    try {
      this._metadata.docCount += 1;
      const doc = this._props.funcTrxDoc(data);
      if (this._metadata.docCount === 1) { this._props.docFirst = doc }
      // if (this._metadata.docCount === 1000) { console.log({"this": this._readableState}) }
      this.push(JSON.stringify(doc))
      this._props.docLast = doc;
      return callback();
    } catch (err) {
      this._metadata.status = 500;
      this._metadata.message = `error:${err.message}`;
      await this.log(err, 'error');
      await this.end();
      await sleepMs(100);                           // give it a chance to end stream with what we have up to now otherwise stream closes with transfer closed with outstanding read data remaining
      return callback(err)                          // signal the error anyway
    }
  }

  jsonMetadata(message = this._metadata.message, status = this._metadata.status) {
    this._metadata.message = message;
    this._metadata.status = status;
    this._metadata.dt.End = new Date();
    this._metadata.dt.ms = this._metadata.dt.End - this._metadata.dt.Start
    this._metadata.dt.OPS = Number((this._metadata.docCount / (this._metadata.dt.ms / 1000)).toFixed(2));
    return `${JSON.stringify(this._metadata)}`
  }

  async log(msg, level = 'info') {
    // console.log('loggggggggggggggggg', {msg, level})
    if (this._props.logger) { await this._props.logger[level](`|${this._props.name}|${this.jsonMetadata(msg)}`) }
  }

  get docFirst() { return this._props.docFirst}

  get docLast() { return this._props.docLast}

  _finish() {
    this.log('finish ')
  }

  async _close() {
    this.log('close', 'info');
  }

  async _destroy(err, callback) {
    this.log(`destroy ${err}`);
    return callback(err)
  }

  async end() {  // comes from readable
    if (this._props.interval) {
      clearInterval(this._props.interval);
      this.log('end', 'info');
    }
    await sleepMs(30);            // give it sometime to process last object (otherwise we can loose it if consumer is fast)
    this.push(null)
  }
}


export {
  DummyWritable,
  ChangeStream,
  StreamTrx,
}
