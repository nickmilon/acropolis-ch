# acropolis-ch

Node js HTTP client for:&nbsp;
<a href="https://clickhouse.com" target= "_blank">
<img src="./resources/images/clickhouse.svg" alt="clickhouse.com" height=14>&nbsp;&nbsp;clickhouse
</a>
using [undici](https://undici.nodejs.org/) http library.
___

## Why
Although there are already a few [js libraries for Clickhouse](https://clickhouse.com/docs/en/interfaces/third-party/client-libraries/),
we decided to write this one from scratch, reasons been among others:
-   Existing libs couldn't cover the needs for the project that this library was written for  where performance was a paramount requirement.
-   As clickhouse has quite a few idiosyncrasies compared to classic sql we needed something more than a plain driver in order to make 
    onboarding easy for developers.
-   Node's native standard http library is quite problematic with a huge back-log of issues and performance is not optimal.
    Node team is thinking of retiring it and probably replace/substitute it with undici which is already under [Node's organization umbrella](https://twitter.com/matteocollina/status/1298148085210775553?lang=en).
This is not a CMS tool.

## Conventions - Design - Features:
- Library only supports ESM modules (imports) commonjs (require) is not supported, If you need commonjs support feel free to fork the library or transpile it somehow to commonjs.<br>
- Supports connection pools and input/output streams out of the box.
- Library follows a minimalist design you only have to understand just one method (request) from a single class in order to start using it.
- Batteries are included and gradually covers more developer needs by providing utilities for common tasks and caters for future needs
and particular use cases by providing expandable building blocks. Also we try to adhere to best practices with usage examples and code in tests. 
-  ### Dependencies:
    There are no dependencies except undici and [acropolis-nd](https://github.com/nickmilon/acropolis-nd) a tiny utilities lib. 
-  ### Security - sql injections:
    Library doesn't attempt to safeguard against sql injections, to mitigate against those threats you can:
    -   sterilize your queries beforehand.
    -   use prepared queries (views). 
    -   use parameterized queries.

## License: [Apache](./LICENSE)

# Quick start:
-   ## installation:
  
    `npm install acropolis-ch --save`
    ### check installation and clickhouse connectivity:
    `cd yourInstallationFolder/node_modules/acropolis-ch/`<br>
    <em>edit const confCH in ./scripts/usage.js to reflect your clickhouse url and credentials</em><br>
    `npm run usageREPL` to check installation and connection to clickhouse http server<br>
    <em>check output for errors or warnings</em><br>
 
-   ## basic usage:
---
<!--usageStart-->
```js 

/**
...  * @summary usage example 🤯
...  * This is a script modified to run under Node's REPL to produce a usage.md file
...  * therefore it contains some strange syntax that should be not used in a normal module in particular:
...  * 1) sometimes uses 'var' for declarations instead of const and/or let
...  * 2) uses dynamic imports (a function) in place of import statements as in JS modules.
...  * ❗️So you will have to adjust for those if you reuse part of this code
...  */
// 👇❗️in your modules replace with: import { CHclient, ...  } from 'acropolis-ch'
➡️ const { CHclient, flagsCH, createContext, formatStr } = await import(`${impDir}/index.js`)
// 👇just for easy client configuration (provide your parameters here)
➡️ const confCH = { uri: 'http://vm-srv:8123', credentials: { user: 'default', password: '123' } };
// 👇create client instance with given parameters
➡️ const client = new CHclient(confCH.uri, confCH.credentials, { connections: 10 });

// 👇 check ping CH server  🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥
➡️ result = await client.ping() // 👈 ping CH server
➡️ result.statusCode // ✅ statusCode should always be 200 if all ok and CH server is reachable no error even if credentials are wrong
200
➡️ result = await client.request('SELECT * FROM numbers(1, 3) FORMAT JSON') // 👈 run a CH query
{
  statusCode: 200,
  headers: {
    date: 'Sat, 11 Mar 2023 07:06:52 GMT',
    connection: 'Keep-Alive',
    'content-type': 'application/json; charset=UTF-8',
    'x-clickhouse-server-display-name': 'vm-srv',
    'transfer-encoding': 'chunked',
    'x-clickhouse-query-id': '4f2696a8-f1d4-48ba-a917-b050a3fa5236',
    'x-clickhouse-format': 'JSON',
    'x-clickhouse-timezone': 'Etc/UTC',
    'keep-alive': 'timeout=3',
    'x-clickhouse-summary': '{"read_rows":"3","read_bytes":"24","written_rows":"0","written_bytes":"0","total_rows_to_read":"3","result_rows":"0","result_bytes":"0"}',
    'x-acropolis-dtEnd': 2023-03-11T07:06:52.594Z
  },
  trailers: {},
  body: {
    meta: [ { name: 'number', type: 'UInt64' } ],
    data: [ { number: '1' }, { number: '2' }, { number: '3' } ],
    rows: 3,
    rows_before_limit_at_least: 3,
    statistics: { elapsed: 0.001143589, rows_read: 3, bytes_read: 24 }
  }
}

/**
...  * @summary CH query demo🚦🤯
...  * All queries to ch are async and return an object { statusCode, body, headers, trailers} when resolved
...  * You can read more about client flags in library docs.
...  * body can be a string or json or a promise depending on some flag settings and CH format used
...  */
➡️ var { statusCode, result, body, headers } = await client.request('SELECT * FROM numbers(1, 3) FORMAT CSV');
➡️ body; // 👇 body now is a text because we changed CH format to CSV
'1\n2\n3\n'
➡️ result = await client.request('DROP TABLE IF EXISTS default.usage1');
➡️ result = await client.request('DROP TABLE usage1');
➡️ result.statusCode; // 👇 statusCode returned by CH is 404 since table doesn't exist as we dropped it already if existed
404
➡️ result.body; // 👇 body contains verbose info for the error
"Code: 60. DB::Exception: Table default.usage1 doesn't exist. (UNKNOWN_TABLE) (version 23.2.1.2537 (official build))\n"

/**
...  * @summary ℹ️💁context usage
...  * context simplifies sql statement execution by presetting client and CH options and support of intellisense typing
...  * in some editors. Also can be easily extended to support user specific sql queries.
...  * Following line creates a client context where flags do not specify flag 'resolve' so body returned by any query 
...  * that uses this context will be a stream
...  */
➡️ ctxStream = createContext(client, { chOpts: {}, flags: flagsCH.flagsToNum(['throwNon200']) });
// 👇 create an other client context that resolves body and specifies a clickhouse option
➡️ ctxResolve = createContext(client, { chOpts: { output_format_json_quote_64bit_integers: 0 }, flags: flagsCH.flagsToNum(['resolve']) });
➡️ result = await ctxStream.CREATE_TABLE_fromSchema('default', 'usage1', '(number UInt64)', { ENGINE: 'MergeTree ORDER BY number' });
➡️ result = await ctxStream.SELECT('*', { FROM: 'numbers(1, 100000)', FORMAT: formatStr.CSV}); //  specify format by formatsStr for convenience
➡️ [typeof result.body, typeof result.body._read ] // 👈 since context flags does not specify flag 'resolve' body will be a readable stream
[ 'object', 'function' ]

// 👇 inserting into table usage1 body the stream of previous SELECT 
➡️ result = await ctxStream.INSERT_INTO('default', 'usage1', result.body, {FORMAT: 'CSV'}) // 👈 inserting into table usage1 body stream of previous SELECT 
➡️ result = await ctxResolve.SELECT('count(*) as count', { FROM: 'default.usage1', FORMAT: formatStr.JSONEachRow});
➡️ assert.equal(result.body.count, 100000 ); // 👈 we just inserted 100K records from one table to an other 🤪 clickHouse is so fast 
➡️ result = await ctxResolve.DROP_TABLE('default', 'usage1');
➡️ assert.equal(result.statusCode, 200);
```
<!--usageEnd-->
-   ## more examples:<br>
    - [visit tests code](test)
    - [read library's docs](https://nickmilon.github.io/acropolis-ch/)
---
## Testing:
Almost full coverage tests are provided in [test folder](test). To run the tests you will need to install [jest](https://jestjs.io/) and <br>
```npm run test ```<br>
Tests are also meant to demonstrate usage and best practices that's why plenty of output is provided on console during test runs.<br>
You can limit output verbosity by setting logLevel variable in acropolis-ch-conf.js to one of available levels.
## Disclosure
- This project is in no way connected to official clickhouse or undici projects.
- Library has been used in production without issues for quite some time, still we encourage to do your own testing/evaluation before using in production.

## ❔ Questions - Issues
- For any suggestions, questions or bugs, feel free to create an <a href="https://github.com/nickmilon/acropolis-ch/issues">issue</a>
___
## 📖 Awesome Resources and further reading:
- [see here](resources/awesome.md)
___
