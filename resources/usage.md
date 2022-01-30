
```js 
➡️ ➡️ ➡️	const assert = await import('assert'); // ignore

➡️ ➡️ ➡️	let result;
➡️ ➡️ ➡️	let body;
➡️ ➡️ ➡️	let statusCode;
➡️ ➡️ ➡️	const ipn = process.cwd().endsWith('integration') ? '/_code/node/acropolis-ch/lib/client.js' : './lib/client.js'; // ignore
// 👇 `${process.cwd()}/lib/client.js` //   '/_code/node/acropolis-ch/lib/client.js'; // ignore
// 👇 import { CHclient } from '../lib/client.js'; /_code/node/acropolis-ch/lib/client.js   await import('./lib/client.js')
➡️ ➡️ ➡️	const { CHclient } = await import(ipn) // ❗️ used from REPL in modules replace with: import { CHclient } from 'acropolis-ch';
➡️ ➡️ ➡️	const confCH = { uri: 'http://localhost:8123', credentials: { user: 'default', password: 'nickmilon' } };

// 👇 create client instance
➡️ ➡️ ➡️	const client = new CHclient(confCH.uri, confCH.credentials, { connections: 10 });
➡️ ➡️ ➡️	result = await client.get('SELECT * FROM numbers(1, 6) FORMAT JSON');
{
  statusCode: 200,
  headers: {
    date: 'Sun, 30 Jan 2022 02:05:05 GMT',
    connection: 'Keep-Alive',
    'content-type': 'application/json; charset=UTF-8',
    'x-clickhouse-server-display-name': 'Y720',
    'transfer-encoding': 'chunked',
    'x-clickhouse-query-id': 'be265e85-86f4-46c5-9535-1a3fa9c7716d',
    'x-clickhouse-format': 'JSON',
    'x-clickhouse-timezone': 'Europe/Athens',
    'keep-alive': 'timeout=3',
    'x-clickhouse-summary': '{"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}',
    'x-acropolis-dtEnd': 2022-01-30T02:05:05.384Z
  },
  trailers: {},
  body: {
    meta: [ { name: 'number', type: 'UInt64' } ],
    data: [
      { number: '1' },
      { number: '2' },
      { number: '3' },
      { number: '4' },
      { number: '5' },
      { number: '6' }
    ],
    rows: 6,
    rows_before_limit_at_least: 6,
    statistics: { elapsed: 0.000171452, rows_read: 6, bytes_read: 48 }
  }
}
// 👇 console.dir({result})
➡️ ➡️ ➡️	({ statusCode } = result);
{
  statusCode: 200,
  headers: {
    date: 'Sun, 30 Jan 2022 02:05:05 GMT',
    connection: 'Keep-Alive',
    'content-type': 'application/json; charset=UTF-8',
    'x-clickhouse-server-display-name': 'Y720',
    'transfer-encoding': 'chunked',
    'x-clickhouse-query-id': 'be265e85-86f4-46c5-9535-1a3fa9c7716d',
    'x-clickhouse-format': 'JSON',
    'x-clickhouse-timezone': 'Europe/Athens',
    'keep-alive': 'timeout=3',
    'x-clickhouse-summary': '{"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}',
    'x-acropolis-dtEnd': 2022-01-30T02:05:05.384Z
  },
  trailers: {},
  body: {
    meta: [ { name: 'number', type: 'UInt64' } ],
    data: [
      { number: '1' },
      { number: '2' },
      { number: '3' },
      { number: '4' },
      { number: '5' },
      { number: '6' }
    ],
    rows: 6,
    rows_before_limit_at_least: 6,
    statistics: { elapsed: 0.000171452, rows_read: 6, bytes_read: 48 }
  }
}
➡️ ➡️ ➡️	assert.equal(result.statusCode, 200);
```
