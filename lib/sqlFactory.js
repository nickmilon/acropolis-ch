class SqlFactory {
  constructor({ name = 'SqlFactory' } = {}) {
    this._props = { name};
  }
}


static singleLiner = str => str.trim().replace(/\n/g, ' ').replace(/  +/g, ' ')     // can be replaced with tag https://github.com/dmnd/dedent/blob/master/dedent.js

static selectMultiple(topSelect, ...selectStatements) {
  let cnt = 0;
  const selItems = selectStatements.map(statement => {
    cnt += 1;
    return `(${statement}) AS Sel${cnt}\n`
  });
  return `${topSelect}\n${selItems}` 
} 
 

export {
  SqlFactory
}