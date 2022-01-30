const assert = await import('assert'); // ignore

let result;
let body;
let statusCode;
const ipn = process.cwd().endsWith('integration') ? '/_code/node/acropolis-ch/lib/client.js' : './lib/client.js'; // ignore
// `${process.cwd()}/lib/client.js` //   '/_code/node/acropolis-ch/lib/client.js'; // ignore
// import { CHclient } from '../lib/client.js'; /_code/node/acropolis-ch/lib/client.js   await import('./lib/client.js')
const { CHclient } = await import(ipn) // ❗️ used from REPL in modules replace with: import { CHclient } from 'acropolis-ch';
const confCH = { uri: 'http://localhost:8123', credentials: { user: 'default', password: 'nickmilon' } };

// create client instance
const client = new CHclient(confCH.uri, confCH.credentials, { connections: 10 });
result = await client.get('SELECT * FROM numbers(1, 6) FORMAT JSON');
// console.dir({result})
({ statusCode } = result);
assert.equal(result.statusCode, 200);