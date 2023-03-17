{
  xxxxxxxxxxxx: 1,
  doclet: Doclet {
    comment: '/**\n *\n * line 1\n * line 2\n * @module\n */',
    meta: {
      filename: 'clientdel.js',
      lineno: 6,
      columnno: 0,
      path: '/_code/node/acropolis-ch/lib',
      code: {}
    },
    description: '<div class="nm-desc-module"><p>line 1<br>line 2</p></div>',
    kind: 'module',
    name: 'clientdel',
    longname: 'module:clientdel',
    ___id: 'T000002R000002',
    ___s: true
  }
}
{
  xxxxxxxxxxxx: 2,
  doclet: Doclet {
    comment: '/**\n' +
      ' * UUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUU.\n' +
      ' * @param {string} color1 - The first color, in hexadecimal format.\n' +
      ' * @param {string} color2 - The second color, in hexadecimal format.\n' +
      ' * @return {string} The blended color.\n' +
      ' */',
    meta: {
      range: [ 1233, 1273 ],
      filename: 'clientdel.js',
      lineno: 42,
      columnno: 0,
      path: '/_code/node/acropolis-ch/lib',
      code: {
        id: 'astnode100000038',
        name: 'exports.blend',
        type: 'FunctionDeclaration',
        paramnames: [ 'color1', 'color2' ]   // +++++++++++++++++++ 
      }
    },
    description: '<div class="nm-desc-function"><p>UUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUU.</p></div>',
    params: [
      {
        type: { names: [ 'string' ] },
        description: '<p>The first color, in hexadecimal format.</p>',
        name: 'color1'
      },
      {
        type: { names: [ 'string' ] },
        description: '<p>The second color, in hexadecimal format.</p>',
        name: 'color2'
      }
    ],
    returns: [
      {
        type: { names: [ 'string' ] },
        description: '<p>The blended color.</p>'
      }
    ],
    name: 'blend',
    longname: 'module:clientdel.blend',
    kind: 'function',
    memberof: 'module:clientdel',
    scope: 'static',
    ___id: 'T000002R000005',
    ___s: true
  }
}
{
  xxxxxxxxxxxxxxxxxxxxxxeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee: 33,
  lineStart: 'export const blendAAA = (color1, color2) => { [color1, color2]}'
}
{
  xxxxxxxxxxxx: 3,
  doclet: Doclet {
    comment: '/**\n' +
      ' * Blend two colors together.\n' +
      ' * AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.\n' +
      ' * BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB\n' +
      ' * @param {string} color1 - The first color, in hexadecimal format.\n' +
      ' * @param {string} color2 - The second color, in hexadecimal format.\n' +
      ' * @return {Array} The blended color.\n' +
      ' */',
    meta: {
      range: [ 939, 1002 ],
      filename: 'clientdel.js',
      lineno: 33,
      columnno: 0,
      path: '/_code/node/acropolis-ch/lib',
      code: {
        id: 'astnode100000026',
        name: 'exports.blendAAA',
        type: 'VariableDeclaration'
      }
    },
    description: '<div class="nm-desc-constant"><p>Blend two colors together.<br>AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.<br>BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB</p></div>',
    params: [
      {
        type: { names: [ 'string' ] },
        description: '<p>The first color, in hexadecimal format.</p>',
        name: 'color1'
      },
      {
        type: { names: [ 'string' ] },
        description: '<p>The second color, in hexadecimal format.</p>',
        name: 'color2'
      }
    ],
    returns: [
      {
        type: { names: [ 'Array' ] },
        description: '<p>The blended color.</p>'
      }
    ],
    name: 'blendAAA',
    longname: 'module:clientdel.blendAAA',
    kind: 'constant',
    memberof: 'module:clientdel',
    scope: 'static',
    ___id: 'T000002R000003',
    ___s: true
  }
}
{
  xxxxxxxxxxxx: 4,
  doclet: Package {
    kind: 'package',
    name: 'acropolis-ch',
    longname: 'package:acropolis-ch',
    author: {
      name: '@nickmilon',
      email: 'nickmilon@geognos.com',
      url: 'https://stackoverflow.com/users/199352/nickmilon'
    },
    bugs: { url: 'https://github.com/nickmilon/acropolis-ch/issues' },
    dependencies: {
      'acropolis-nd': 'github:nickmilon/acropolis-nd',
      undici: '^4.10.4'
    },
    description: '<div class="nm-desc-package">node clickhouse client</div>',
    engines: { node: '>=16.10.0' },
    files: [ '/_code/node/acropolis-ch/lib/clientdel.js' ],
    homepage: 'https://github.com/nickmilon/acropolis-ch#readme',
    keywords: [ 'node', 'clickhouse', 'driver', 'client', 'utilities' ],
    licenses: [ { type: 'Apache-2.0' } ],
    main: 'index.js',
    repository: {
      type: 'git',
      url: 'git+https://github.com/nickmilon/acropolis-ch.git'
    },
    version: '1.0.8',
    ___id: 'T000002R000009',
    ___s: true
  }
}