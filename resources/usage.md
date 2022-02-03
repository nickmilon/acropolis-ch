
```js 
>>>/*🚥 
... This is a script modified to run under Node's REPL to produce a usage.md file
... therefore it contains some strange syntax that should be not used in a normal module in particular:
... 1) sometimes uses 'var' for declarations instead of const and/or let
... 2) uses dynamic imports (function) in place of import statements as in JS modules.
... ❗️So you will have to adjust for those if you reuse part of this code
... */
undefined
>>>const assert = await import('assert'); // ignore
undefined
>>>// const timersAsync = await import('timers/promises') 
undefined

undefined

undefined

undefined
>>>const impDir = process.cwd().endsWith('integration') ? '/_code/node/acropolis-ch' : '.'; // ignore only used to evaluate indirect import path to rub on node's repl
undefined
>>>const acropolisLib = await import(`${impDir}/index.js`);  // ❗️in your modules replace with: import { client } from 'acropolis-ch';
undefined
>>>const { stdoutMsg }  = await import(`${impDir}/scripts/usageAux.js`)  // {{DEL}} auxillary functions used only by this script
undefined
>>>const { CHclient, flagsCH,  } = acropolisLib.client;
undefined
>>>const { createContext } = acropolisLib.context;
undefined
>>>const { formatStr } = acropolisLib.formats; 
undefined


>>>// 👇 just for easy client configuration (provide your parameters here)
undefined
>>>const confCH = { uri: 'http://localhost:8123', credentials: { user: 'default', password: 'nickmilon' } };
undefined
>>>// 👇create client instance with given parameters
undefined
>>>const client = new CHclient(confCH.uri, confCH.credentials, { connections: 10 });
undefined

>>>stdoutMsg('// 👇 check ping CH server '); // {{DEL}}
// 👇 check ping CH server  🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥
undefined
>>>var result = await client.ping() // 👈 ping CH server
undefined
>>>assert.equal(result.statusCode, 200); // ✅ statusCode should always be 200 if no error even if credentials are wrong
undefined
>>>assert.equal(result.body, 'Ok.\n');   // ✅ ping body text should be 'Ok.\n'
undefined
>>>if (result.statusCode !== 200 ) { stdoutMsg(' ❗️❗️❗️ CH http server is unreachable1'); }  // {{DEL}}
undefined
>>>const statusCodePing = result.statusCode;
undefined
>>>var result = await client.request('SELECT * FROM numbers(1, 3) FORMAT JSON') // 👈 run a CH query
undefined
>>>if (result.statusCode === 403 && statusCodePing === 200 ) {  stdoutMsg(`❗️❗️❗️CH http server responds but probably your credentials are wrong\n ${result.body}`); }  // {{DEL}>>>if (result.statusCode === 403 && statusCodePing === 200 ) {  stdoutMsg(`❗️❗️❗️CH http server responds but probably your credentials are wrong\n ${result.body}`); }  // {{DEL}} 
undefined

>>>/*
... 🚦🤯All queries to ch are async and return an object { statusCode, body, headers, trailers} when resolved
... body can be a string or json or a promise depending on some flag settings and CH format used
... You can read more about client flags in library docs.
... */
undefined
>>>var { statusCode, result, body, headers } = await client.request('SELECT * FROM numbers(1, 3) FORMAT CSV');
undefined
>>>body; // 👇 body now is a text because we changed CH format to CSV
'1\n2\n3\n'
>>>var result = await client.request('DROP TABLE IF EXISTS default.usage1');
undefined
>>>var result = await client.request('DROP TABLE usage1');
undefined
>>>result.statusCode; // 👇 statusCode returned by CH is 404 since table doesn't exist as we dropped it already if existed
404
>>>result.body; // 👇 body contains verbose info for the error
"Code: 60. DB::Exception: Table default.usage1 doesn't exist. (UNKNOWN_TABLE) (version 22.1.3.7 (official build))\n"
>>>/*
... ℹ️💁context simplify sql statement execution by presetting client and CH options and support of intellisense typing
... in some editors. Also can be easily extended to support user specific sql queries.
... Following line creates a client context since flags do not specify flag 'resolve' body returned by any query 
... that uses this context will be a stream
... */
undefined
>>>var ctxStream = createContext(client, { chOpts: {}, flags: flagsCH.flagsToNum(['throwNon200']) });
undefined
>>>// 👇 create an other client context that resolves body and specifies a clickhouse option
undefined
>>>var ctxResolve = createContext(client, { chOpts: { output_format_json_quote_64bit_integers: 0 }, flags: flagsCH.flagsToNum(['resolve']) });
undefined
>>>var result = await ctxStream.CREATE_TABLE_fromSchema('default', 'usage1', '(number UInt64)', { ENGINE: 'MergeTree ORDER BY number' });
ERR 2005 integration wrong contents [>>>/*🚥 
... Th]
undefined
>>>var result = await ctxStream.SELECT('*', { FROM: 'numbers(1, 100000)', FORMAT: formatStr.CSV}); //  specify format by formatsStr for convenience
undefined
>>>[typeof result.body, typeof result.body._read ] // 👈 since context flags does not specify flag 'resolve' body will be a readable stream
[ 'object', 'function' ]

>>>// 👇 inserting into table usage1 body the stream of previous SELECT 
undefined
>>>var result = await ctxStream.INSERT_INTO('default', 'usage1', result.body, {FORMAT: 'CSV'}) // 👈 inserting into table usage1 body stream of previous SELECT 
undefined
>>>var result = await ctxResolve.SELECT('count(*) as count', { FROM: 'default.usage1', FORMAT: formatStr.JSONEachRow});
undefined
>>>assert.equal(result.body.count, 100000 ); // 👈 we just inserted 100K records from one table to an other 🤪 clickHouse is so fast 
undefined
>>>var result = await ctxResolve.DROP_TABLE('default', 'usage1');
undefined
>>>assert.equal(result.statusCode, 200);
undefined
>>>stdoutMsg('─────END─────'.repeat(4)) // {{DEL}}
```
