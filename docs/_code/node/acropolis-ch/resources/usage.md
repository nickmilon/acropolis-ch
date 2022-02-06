
```js 

/**
...  * @summary:🚦🤯 usage example
...  * This is a script modified to run under Node's REPL to produce a usage.md file
...  * therefore it contains some strange syntax that should be not used in a normal module in particular:
...  * 1) sometimes uses 'var' for declarations instead of const and/or let
...  * 2) uses dynamic imports (a function) in place of import statements as in JS modules.
...  * ❗️So you will have to adjust for those if you reuse part of this code
...  */

▶️▶️ const assert = await import('assert'); // ignore

// 👇❗️in your modules replace with: import { CHclient, ...  } from 'acropolis-ch'
▶️▶️ const { CHclient, flagsCH, createContext, formatStr } = await import(`${impDir}/index.js`)
 auxillary functions used only by this script
// 👇just for easy client configuration (provide your parameters here)
▶️▶️ const confCH = { uri: 'http://localhost:8123', credentials: { user: 'default', password: 'nickmilon' } };
// 👇create client instance with given parameters
▶️▶️ const client = new CHclient(confCH.uri, confCH.credentials, { connections: 10 });

// 👇 check ping CH server  🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥
▶️▶️ result = await client.ping() // 👈 ping CH server
▶️▶️ assert.equal(result.statusCode, 200); // ✅ statusCode should always be 200 if no error even if credentials are wrong
▶️▶️ assert.equal(result.body, 'Ok.\n');   // ✅ ping body text should be 'Ok.\n'
▶️▶️ const statusCodePing = result.statusCode;
▶️▶️ result = await client.request('SELECT * FROM numbers(1, 3) FORMAT JSON') // 👈 run a CH query

/**
...  * @summary:🚦🤯 CH query demo
...  * All queries to ch are async and return an object { statusCode, body, headers, trailers} when resolved
...  * You can read more about client flags in library docs.
...  * body can be a string or json or a promise depending on some flag settings and CH format used
...  */
▶️▶️ var { statusCode, result, body, headers } = await client.request('SELECT * FROM numbers(1, 3) FORMAT CSV');
▶️▶️ body; // 👇 body now is a text because we changed CH format to CSV
'1\n2\n3\n'
▶️▶️ result = await client.request('DROP TABLE IF EXISTS default.usage1');
▶️▶️ result = await client.request('DROP TABLE usage1');
▶️▶️ result.statusCode; // 👇 statusCode returned by CH is 404 since table doesn't exist as we dropped it already if existed
404
▶️▶️ result.body; // 👇 body contains verbose info for the error
"Code: 60. DB::Exception: Table default.usage1 doesn't exist. (UNKNOWN_TABLE) (version 22.1.3.7 (official build))\n"

/**
...  * @summary: ℹ️💁context usage
...  * context simplifies sql statement execution by presetting client and CH options and support of intellisense typing
...  * in some editors. Also can be easily extended to support user specific sql queries.
...  * Following line creates a client context where flags do not specify flag 'resolve' so body returned by any query 
...  * that uses this context will be a stream
...  */
▶️▶️ ctxStream = createContext(client, { chOpts: {}, flags: flagsCH.flagsToNum(['throwNon200']) });
// 👇 create an other client context that resolves body and specifies a clickhouse option
▶️▶️ ctxResolve = createContext(client, { chOpts: { output_format_json_quote_64bit_integers: 0 }, flags: flagsCH.flagsToNum(['resolve']) });
▶️▶️ result = await ctxStream.CREATE_TABLE_fromSchema('default', 'usage1', '(number UInt64)', { ENGINE: 'MergeTree ORDER BY number' });
▶️▶️ result = await ctxStream.SELECT('*', { FROM: 'numbers(1, 100000)', FORMAT: formatStr.CSV}); //  specify format by formatsStr for convenience
▶️▶️ [typeof result.body, typeof result.body._read ] // 👈 since context flags does not specify flag 'resolve' body will be a readable stream
[ 'object', 'function' ]

// 👇 inserting into table usage1 body the stream of previous SELECT 
▶️▶️ result = await ctxStream.INSERT_INTO('default', 'usage1', result.body, {FORMAT: 'CSV'}) // 👈 inserting into table usage1 body stream of previous SELECT 
▶️▶️ result = await ctxResolve.SELECT('count(*) as count', { FROM: 'default.usage1', FORMAT: formatStr.JSONEachRow});
▶️▶️ assert.equal(result.body.count, 100000 ); // 👈 we just inserted 100K records from one table to an other 🤪 clickHouse is so fast 
▶️▶️ result = await ctxResolve.DROP_TABLE('default', 'usage1');
▶️▶️ assert.equal(result.statusCode, 200);
```
