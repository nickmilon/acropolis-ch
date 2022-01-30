# acropolis-ch
Yet one more Node js HTTP client for [clickhouse](https://clickhouse.com/). This one is based on [undici](https://undici.nodejs.org/)

## Why
Although there are already a few [js libraries for Clickhouse](https://clickhouse.com/docs/en/interfaces/third-party/client-libraries/),
we decided to write this one from scratch, reasons been among others:
-   Existing libs couldn't cover the needs for the project that this library was written for in first place where performance was a paramount requirement.
-   As clickhouse has quite a few idiosyncrasies compared to classic sql we needed something more than a plain driver in order to make 
    onboarding easy for developers.
-   Node's native standard http library is quite problematic with a huge back-log of issues and performance is not optimal.
    Node team is thinking of retiring it and probably replace/substitute it with undici which is already under [Node's organization umbrella](https://twitter.com/matteocollina/status/1298148085210775553?lang=en).

## Conventions - Design:
-   Library only supports ESM modules (imports) commonjs (require) is not supported, If you need commonjs support feel free to fork the library or transpile it somehow to commonjs.
-   Supports connection pools and input/output streams out of the box.
-   Library follows a minimalist design you only have to understand just one method (request) from a single class in order to start using it.
Still batteries are included and gradually covers more developer needs by providing utilities for common tasks and caters for future needs
and particular use cases by providing expandable building blocks. Also we try to adhere to best practices with usage examples and code in tests. 
-  ### Dependencies:
    There are no dependencies except undici and [acropolis-nd](https://github.com/nickmilon/acropolis-nd) a tiny utilities lib. 
-  ### Security - sql injections
    Library doesn't attempt to safeguard against sql injections, to mitigate against those threats you can:
    -   sterilize your queries beforehand.
    -   use prepared queries (views). 
    -   use parameterized queries.

## License: [Apache](./LICENSE)

# Quick start:
-   ## installation:
  
    `npm install acropolis-ch --save` to install.
    ### check installation:
    1.  `cd yourInstallationFolder/node_modules/acropolis-ch/`<br>
    2.  <em>edit const confCH in scripts/usage.js to reflect your clickhouse url and credentials</em><br>
    3.  `npm run usageREPL` to check installation and connection to clickhouse<br>
    4.  <em>check output for errors or warnings</em><br>
 
-   ## basic usage:
---
<!--usageStart-->
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
<!--usageEnd-->
---
## Testing:
Almost full coverage tests are provided in __tests__ directory. To run the tests you will need to install [jest](https://jestjs.io/) and <br>
```npm run test ```<br>
Tests are also meant to demonstrate usage and best practices that's why plenty of output is provided on console during test runs.
You can limit output verbosity by setting logLevel variable in /config.js to one of available levels.
## Disclosure
-   This project is in no way connected to official clickhouse or undici projects.
-   Library has been used in production without issues for quite some time, still we encourage to do your own testing/evaluation before using in production.  
-   Suggestions bug, reports and pull requests are always welcomed.

## Acknowledgements:
Many thanks to [rapchat](https://rapchat.com) for partially funding initial development of this project.
___
## 📖 Resources and further reading:
- [see here](notes/resources.md)
___