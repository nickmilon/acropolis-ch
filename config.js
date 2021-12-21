/**
 *  configuration module
 *  @todo: get from package when json imports are available const { name, version } = require('./package.json');
 *  @todo: put in on .gitignore and have a sim-linked copy so doesn't get overriten in pulls or git update-index --assume-unchanged 
 */

const confCH = { // clickhouse
  uri: 'http://localhost:8123',
  connections: 10,
  credentials: { user: 'default', password: 'nickmilon' },
  dbs: ['work', 'test'], // will be created IF NOT EXISTS and verifyServer === true
};
const runOptions = {
  tests: { logLevel: 'debug' },
};

export { confCH, runOptions };
