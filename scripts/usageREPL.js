#!/usr/bin/env node
/* eslint-disable new-cap */

import repl from 'repl';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const replRun = (filePathIn) => {
  const replOptions = {
    prompt: '➡️ ➡️ ➡️\t',
    ignoreUndefined: true,
    input: new fs.createReadStream(filePathIn),
    output: process.stdout,
  };
  const replServer = repl.start(replOptions);
  replServer.writer.options.depth = 10;
  replServer.on('exit', () => { process.stdout.write('END\n'); });
};

replRun(path.join(path.dirname(fileURLToPath(import.meta.url)), '/usage.js'));
