#!/usr/bin/env node

import { confCH, runOptions } from '../config.js';
// import  *  as foo from 'node:zlib' ;
// console.dir(foo)
import { createGunzip, unzip  } from 'node:zlib';
// const unzip = zlib.createGunzip();

console.dir(unzip)
import fs from 'fs';
// const pathIn = '/home/milon/Downloads/link_status_search_with_ordering_real_csv.zip'
const pathIn = '/home/milon/Downloads/active_follower_real_sql.zip'

const streamOut = fs.createWriteStream('del_XXXXX_1');
const streamIn = fs.createReadStream(pathIn);
// const unzipXX = createGunzip();
// streamIn.pipe(unzip()).pipe(streamOut);
 
streamIn.pipe(unzip).pipe(streamOut).on('finish', (err) => {
   if (err) return reject(err);
   else resolve();
  })


// const fileContents = fs.createReadStream('file1.txt.gz');
// const writeStream = fs.createWriteStream('file1.txt');
// const unzip = zlib.createGunzip();

// fileIn.pipe(unzip).pipe(fileOut);
// fileIn.pipe(unzip.Extract({ path: pathIn }))


// Fetch http://example.com/foo.gz, gunzip it and store the results in 'out'
// request('http://example.com/foo.gz').pipe(zlib.createGunzip()).p

//  const fs = require("fs");
// const { pipeline } = require("node:stream");


console.log("end end ====================");