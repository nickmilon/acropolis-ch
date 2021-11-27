/* eslint-disable no-await-in-loop */

import { objRndFlat } from 'acropolis-nd/lib/Eratosthenes.js';
import { randomBetween } from 'acropolis-nd/lib/Pythagoras.js';

const rndObjArr = (start = 1, end = 1000) => {
  const resArr = [];
  for (let cnt = start; cnt < end - start + 2; cnt += 1) {
    resArr.push({ id: cnt, ...objRndFlat() });
  }
  return resArr;
};

const populatePaging = async (coll, count = 190, version = 0) => {
  const [currentCount, currentDoc] = await Promise.all([coll.countDocuments(), coll.findOne()]);
  if (currentCount >= count && currentDoc.version === version) { return { success: true, result: 'exists' }; }
  if (currentCount > 0) { await coll.drop(); }
  const pd = (num) => num.toString().padStart(3, '0');
  for (let cnt = 1; cnt <= count; cnt += 1) {
    const doc = {
      _id: pd(cnt),
      pg03: pd(Math.ceil(cnt / 3)),
      pg04: pd(Math.ceil(cnt / 4)),
      pg10: pd(Math.ceil(cnt / 10)),
      rnd1: pd(randomBetween(1, count)),
      rnd2: pd(randomBetween(1, 10)),
      v: version,
    };
    await coll.insertOne(doc);
  }
  return { success: true, result: 'ok' };
};

export {
  rndObjArr,
  populatePaging,
};
