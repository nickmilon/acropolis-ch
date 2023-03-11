[1G[0J>>>[4G
[1G[0J>>>[4G/**
[1G[0J... [5G * @summary usage example 🤯
[1G[0J... [5G * This is a script modified to run under Node's REPL to produce a usage.md file
[1G[0J... [5G * therefore it contains some strange syntax that should be not used in a normal module in particular:
[1G[0J... [5G * 1) sometimes uses 'var' for declarations instead of const and/or let
[1G[0J... [5G * 2) uses dynamic imports (a function) in place of import statements as in JS modules.
[1G[0J... [5G * ❗️So you will have to adjust for those if you reuse part of this code
[1G[0J... [5G */
[90mundefined[39m
[1G[0J>>>[4Gconst impDir = process.cwd().endsWith('integration') ? '/_nm/prgs/node/acropolis-ch' : '.'; // {{DEL}}
[90mundefined[39m
[1G[0J>>>[4G// 👇❗️in your modules replace with: import { CHclient, ...  } from 'acropolis-ch'
[90mundefined[39m
[1G[0J>>>[4Gconst { CHclient, flagsCH, createContext, formatStr } = await import(`${impDir}/index.js`)
[90mundefined[39m
[1G[0J>>>[4Gconst { stdoutMsg }  = await import(`${impDir}/scripts/usageAux.js`)  // {{DEL}}
[90mundefined[39m
[1G[0J>>>[4G// 👇just for easy client configuration (provide your parameters here)
[90mundefined[39m
[1G[0J>>>[4Gconst confCH = { uri: 'http://vm-srv:8123', credentials: { user: 'default', password: '123' } };
[90mundefined[39m
[1G[0J>>>[4G// 👇create client instance with given parameters
[90mundefined[39m
[1G[0J>>>[4Gconst client = new CHclient(confCH.uri, confCH.credentials, { connections: 10 });
[90mundefined[39m
[1G[0J>>>[4G
[1G[0J>>>[4GstdoutMsg('// 👇 check ping CH server '); // {{DEL}}
// 👇 check ping CH server  🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥🚥
[90mundefined[39m
[1G[0J>>>[4Gvar result = await client.ping() // 👈 ping CH server
[90mundefined[39m
[1G[0J>>>[4Gresult.statusCode // ✅ statusCode should always be 200 if all ok and CH server is reachable no error even if credentials are wrong
[33m200[39m
[1G[0J>>>[4Gif (result.statusCode !== 200 ) { stdoutMsg(' ❗️❗️❗️ CH http server is unreachable1'); }  // {{DEL}}
[90mundefined[39m
[1G[0J>>>[4Gconst statusCodePing = result.statusCode;  // {{DEL}}
[90mundefined[39m
[1G[0J>>>[4Gresult = await client.request('SELECT * FROM numbers(1, 3) FORMAT JSON') // 👈 run a CH query
{
  statusCode: [33m200[39m,
  headers: {
    date: [32m'Sat, 11 Mar 2023 15:49:49 GMT'[39m,
    connection: [32m'Keep-Alive'[39m,
    [32m'content-type'[39m: [32m'application/json; charset=UTF-8'[39m,
    [32m'x-clickhouse-server-display-name'[39m: [32m'vm-srv'[39m,
    [32m'transfer-encoding'[39m: [32m'chunked'[39m,
    [32m'x-clickhouse-query-id'[39m: [32m'd42fe0b8-7337-436b-b295-7c342157ebbf'[39m,
    [32m'x-clickhouse-format'[39m: [32m'JSON'[39m,
    [32m'x-clickhouse-timezone'[39m: [32m'Etc/UTC'[39m,
    [32m'keep-alive'[39m: [32m'timeout=3'[39m,
    [32m'x-clickhouse-summary'[39m: [32m'{"read_rows":"3","read_bytes":"24","written_rows":"0","written_bytes":"0","total_rows_to_read":"3","result_rows":"0","result_bytes":"0"}'[39m,
    [32m'x-acropolis-dtEnd'[39m: [35m2023-03-11T15:49:49.647Z[39m
  },
  trailers: {},
  body: {
    meta: [ { name: [32m'number'[39m, type: [32m'UInt64'[39m } ],
    data: [ { number: [32m'1'[39m }, { number: [32m'2'[39m }, { number: [32m'3'[39m } ],
    rows: [33m3[39m,
    rows_before_limit_at_least: [33m3[39m,
    statistics: { elapsed: [33m0.009139902[39m, rows_read: [33m3[39m, bytes_read: [33m24[39m }
  }
}
[1G[0J>>>[4Gif (result.statusCode === 403 && statusCodePing === 200 ) {  stdoutMsg(`❗️❗️❗️CH http server responds but probably your credentials are wrong\n ${result.body}`); }[1G[0J>>>if (result.statusCode === 403 && statusCodePing === 200 ) {  stdoutMsg(`❗️❗️❗️CH http server responds but probably your credentials are wrong\n ${result.body}`); }  [1G // {{DEL}}
[90mundefined[39m
[1G[0J>>>[4G
[1G[0J>>>[4G/**
[1G[0J... [5G * @summary CH query demo🚦🤯
[1G[0J... [5G * All queries to ch are async and return an object { statusCode, body, headers, trailers} when resolved
[1G[0J... [5G * You can read more about client flags in library docs.
[1G[0J... [5G * body can be a string or json or a promise depending on some flag settings and CH format used
[1G[0J... [5G */
[90mundefined[39m
[1G[0J>>>[4Gvar { statusCode, result, body, headers } = await client.request('SELECT * FROM numbers(1, 3) FORMAT CSV');
[90mundefined[39m
[1G[0J>>>[4Gbody; // 👇 body now is a text because we changed CH format to CSV
[32m'1\n2\n3\n'[39m
[1G[0J>>>[4Gvar result = await client.request('DROP TABLE IF EXISTS default.usage1');
[90mundefined[39m
[1G[0J>>>[4Gvar result = await client.request('DROP TABLE usage1');
[90mundefined[39m
[1G[0J>>>[4Gresult.statusCode; // 👇 statusCode returned by CH is 404 since table doesn't exist as we dropped it already if existed
[33m404[39m
[1G[0J>>>[4Gresult.body; // 👇 body contains verbose info for the error
[32m"Code: 60. DB::Exception: Table default.usage1 doesn't exist. (UNKNOWN_TABLE) (version 23.2.1.2537 (official build))\n"[39m
[1G[0J>>>[4G
[1G[0J>>>[4G/**
[1G[0J... [5G * @summary ℹ️💁context usage
[1G[0J... [5G * context simplifies sql statement execution by presetting client and CH options and support of intellisense typing
[1G[0J... [5G * in some editors. Also can be easily extended to support user specific sql queries.
[1G[0J... [5G * Following line creates a client context where flags do not specify flag 'resolve' so body returned by any query 
[1G[0J... [5G * that uses this context will be a stream
[1G[0J... [5G */
[90mundefined[39m
[1G[0J>>>[4Gvar ctxStream = createContext(client, { chOpts: {}, flags: flagsCH.flagsToNum(['throwNon200']) });
[90mundefined[39m
[1G[0J>>>[4G// 👇 create an other client context that resolves body and specifies a clickhouse option
[90mundefined[39m
[1G[0J>>>[4Gvar ctxResolve = createContext(client, { chOpts: { output_format_json_quote_64bit_integers: 0 }, flags: flagsCH.flagsToNum(['resolve']) });
[90mundefined[39m
[1G[0J>>>[4Gvar result = await ctxStream.CREATE_TABLE_fromSchema('default', 'usage1', '(number UInt64)', { ENGINE: 'MergeTree ORDER BY number' });
[90mundefined[39m
[1G[0J>>>[4Gvar result = await ctxStream.SELECT('*', { FROM: 'numbers(1, 100000)', FORMAT: formatStr.CSV}); //  specify format by formatsStr for convenience
[90mundefined[39m
[1G[0J>>>[4G[typeof result.body, typeof result.body._read ] // 👈 since context flags does not specify flag 'resolve' body will be a readable stream
[ [32m'object'[39m, [32m'function'[39m ]
[1G[0J>>>[4G
[1G[0J>>>[4G// 👇 inserting into table usage1 body the stream of previous SELECT 
[90mundefined[39m
[1G[0J>>>[4Gvar result = await ctxStream.INSERT_INTO('default', 'usage1', result.body, {FORMAT: 'CSV'}) // 👈 inserting into table usage1 body stream of previous SELECT 
[90mundefined[39m
[1G[0J>>>[4Gvar result = await ctxResolve.SELECT('count(*) as count', { FROM: 'default.usage1', FORMAT: formatStr.JSONEachRow});
[90mundefined[39m
[1G[0J>>>[4Gassert.equal(result.body.count, 100000 ); // 👈 we just inserted 100K records from one table to an other 🤪 clickHouse is so fast 
[90mundefined[39m
[1G[0J>>>[4Gvar result = await ctxResolve.DROP_TABLE('default', 'usage1');
[90mundefined[39m
[1G[0J>>>[4Gassert.equal(result.statusCode, 200);
[90mundefined[39m
[1G[0J>>>[4GstdoutMsg('─────END─────'.repeat(4)) // {{DEL}}