#!/usr/bin/env node
/**
 * import { confCH, runOptions } from '../config.js';
 * if we want smaller sample ==> :  gunzip -c twitter-2010.txt.gz | head -1000000  | gzip  > del.gz 
 * curl https://snap.stanford.edu/data/twitter-2010.txt.gz | gunzip -c | head -100000 |  gzip  > del_100000.gz
 * npm run twt-data-import --twtpath=/mnt/sda3/del_5000000.gz, 
 */
 
import { graphSim } from '../lib/solutions/graphSimulation.js';

const twtpath = process.env.npm_config_twtpath 
await graphSim.doAll(twtpath)
console.log("end end ====================");