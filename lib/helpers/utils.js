/* eslint-disable no-param-reassign */

import { docUpdateOpts } from '../../config.js';

const { DateTimeCreated, DateTimeUpdated } = docUpdateOpts;

const docSetDt = (document = {}, fld = DateTimeCreated, asTimestamp = false) => {
  document[fld] = (asTimestamp === false) ? new Date() : new Date().getTime();
  return document;
};
const docSetDtCrt = (document = {}, asTimestamp = false) => docSetDt(document, DateTimeCreated, asTimestamp);
const docSetDtUpd = (document = {}, asTimestamp = false) => docSetDt(document, DateTimeUpdated, asTimestamp);

export {
  docSetDt,
  docSetDtCrt,
  docSetDtUpd,
};
