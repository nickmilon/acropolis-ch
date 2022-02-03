export const stdoutMsg = (msg = '', fill =  '🚥') => {
  const message = `${msg} ${fill.repeat(Math.max(70 - msg.length, 1))}\n`; // {{DEL}}
  process.stdout.write(message);
  if (msg.startsWith('❗️❗️❗️')) { process.exit(1); }
};
