// <!--cSpell:disable -->

/**
 * clickhouse formats { nm: name, in:for input, out: for output dec = decodable}
 * see {@link https://clickhouse.com/docs/en/interfaces/formats/ formats}
 * @module
 */


/**
 * @fileoverview
 * clickhouse formats { nm: name, in:for input, out: for output dec = decodable}
 * see {@link https://clickhouse.com/docs/en/interfaces/formats/ formats}
 * @exportsFix {chFormatsDefs, filterFormats, formatStr}
 */

export const chFormatsProps = {
  Arrow: { nm: 'Arrow', in: true, out: true, dec: false },
  ArrowStream: { nm: 'ArrowStream', in: true, out: true, dec: false },
  Avro: { nm: 'Avro', in: true, out: true, dec: false },
  AvroConfluent: { nm: 'AvroConfluent', in: true, out: false, dec: false },
  CSV: { nm: 'CSV', in: true, out: true, dec: false },
  CSVWithNames: { nm: 'CSVWithNames', in: true, out: true, dec: false },
  CapnProto: { nm: 'CapnProto', in: true, out: false, dec: false },
  CustomSeparated: { nm: 'CustomSeparated', in: true, out: true, dec: false },
  JSON: { nm: 'JSON', in: false, out: true, dec: true },
  JSONAsString: { nm: 'JSONAsString', in: true, out: false, dec: true },
  JSONCompact: { nm: 'JSONCompact', in: false, out: true, dec: true },  // includes mmeta
  JSONCompactColumns: { nm: 'JSONCompactColumns', in: true, out: true, dec: true },
  JSONCompactEachRow: { nm: 'JSONCompactEachRow', in: true, out: true, dec: true },
  JSONCompactEachRowWithNamesAndTypes: { nm: 'JSONCompactEachRowWithNamesAndTypes', in: true, out: true, dec: false },
  JSONCompactStrings: { nm: 'JSONCompactStrings', in: false, out: true, dec: true },
  JSONCompactStringsEachRow: { nm: 'JSONCompactStringsEachRow', in: true, out: true, dec: true },
  JSONCompactStringsEachRowWithNamesAndTypes: { nm: 'JSONCompactStringsEachRowWithNamesAndTypes', in: true, out: true, dec: false },
  JSONEachRow: { nm: 'JSONEachRow', in: true, out: true, dec: true },
  JSONEachRowWithProgress: { nm: 'JSONEachRowWithProgress', in: false, out: true, dec: false },
  JSONStrings: { nm: 'JSONStrings', in: false, out: true, dec: true },
  JSONStringsEachRow: { nm: 'JSONStringsEachRow', in: true, out: true, dec: false },
  JSONStringsEachRowWithProgress: { nm: 'JSONStringsEachRowWithProgress', in: false, out: true, dec: false },
  LineAsString: { nm: 'LineAsString', in: true, out: false, dec: false },
  MsgPack: { nm: 'MsgPack', in: true, out: true, dec: false },
  Native: { nm: 'Native', in: true, out: true, dec: false },
  Null: { nm: 'Null', in: false, out: true, dec: false },
  ORC: { nm: 'ORC', in: true, out: true, dec: false },
  Parquet: { nm: 'Parquet', in: true, out: true, dec: false },
  Pretty: { nm: 'Pretty', in: false, out: true, dec: false },
  PrettyCompact: { nm: 'PrettyCompact', in: false, out: true, dec: false },
  PrettyCompactMonoBlock: { nm: 'PrettyCompactMonoBlock', in: false, out: true, dec: false },
  PrettyNoEscapes: { nm: 'PrettyNoEscapes', in: false, out: true, dec: false },
  PrettySpace: { nm: 'PrettySpace', in: false, out: true, dec: false },
  Protobuf: { nm: 'Protobuf', in: true, out: false, dec: false },                                           // out = true but requires schema
  ProtobufSingle: { nm: 'ProtobufSingle', in: true, out: false, dec: false },                               // out = true but requires schema
  RawBLOB: { nm: 'RawBLOB', in: true, out: true, dec: false },
  Regexp: { nm: 'Regexp', in: true, out: false, dec: false },
  RowBinary: { nm: 'RowBinary', in: true, out: true, dec: false },
  RowBinaryWithNamesAndTypes: { nm: 'RowBinaryWithNamesAndTypes', in: true, out: true, dec: false },
  TSKV: { nm: 'TSKV', in: true, out: true, dec: false },
  TabSeparated: { nm: 'TabSeparated', in: true, out: true, dec: false },
  TabSeparatedRaw: { nm: 'TabSeparatedRaw', in: true, out: true, dec: false },
  TabSeparatedWithNames: { nm: 'TabSeparatedWithNames', in: true, out: true, dec: false },
  TabSeparatedWithNamesAndTypes: { nm: 'TabSeparatedWithNamesAndTypes', in: true, out: true, dec: false },
  Template: { nm: 'Template', in: true, out: false, dec: false },                                         // out true but requires schena
  TemplateIgnoreSpaces: { nm: 'TemplateIgnoreSpaces', in: true, out: false, dec: false },
  Values: { nm: 'Values', in: true, out: true, dec: false },
  Vertical: { nm: 'Vertical', in: false, out: true, dec: false },
  XML: { nm: 'XML', in: false, out: true, dec: false },
};

export const filterFormats = (fn = (x) => x.dec === true) => Object.entries(chFormatsProps).filter(([, v]) => fn(v)).map(([k]) => k);

/**
 *
 * @typedef {Object} formatStr only k,v names  i.e: { CSV: 'CSV', CSVWithNames: 'CSVWithNames', JSON: 'JSON', .... }
 * usefull for typing autocomplete in code;
 */
// export const formatNm = Object.fromEntries(Object.entries(formatsObj).map(([k, v]) => [k, v.nm]));
export const formats = Object.fromEntries(Object.entries(chFormatsProps).map(([k, v]) => [k, v.nm]));

export const formatStr = {
  Arrow: 'Arrow',
  ArrowStream: 'ArrowStream',
  Avro: 'Avro',
  AvroConfluent: 'AvroConfluent',
  CSV: 'CSV',
  CSVWithNames: 'CSVWithNames',
  CapnProto: 'CapnProto',
  CustomSeparated: 'CustomSeparated',
  JSON: 'JSON',
  JSONAsString: 'JSONAsString',
  JSONCompact: 'JSONCompact',
  JSONCompactEachRow: 'JSONCompactEachRow',
  JSONCompactEachRowWithNamesAndTypes: 'JSONCompactEachRowWithNamesAndTypes',
  JSONCompactStrings: 'JSONCompactStrings',
  JSONCompactStringsEachRow: 'JSONCompactStringsEachRow',
  JSONCompactStringsEachRowWithNamesAndTypes: 'JSONCompactStringsEachRowWithNamesAndTypes',
  JSONEachRow: 'JSONEachRow',
  JSONEachRowWithProgress: 'JSONEachRowWithProgress',
  JSONStrings: 'JSONStrings',
  JSONStringsEachRow: 'JSONStringsEachRow',
  JSONStringsEachRowWithProgress: 'JSONStringsEachRowWithProgress',
  LineAsString: 'LineAsString',
  MsgPack: 'MsgPack',
  Native: 'Native',
  Null: 'Null',
  ORC: 'ORC',
  Parquet: 'Parquet',                   // will need https://github.com/ZJONSSON/parquetjs
  Pretty: 'Pretty',
  PrettyCompact: 'PrettyCompact',
  PrettyCompactMonoBlock: 'PrettyCompactMonoBlock',
  PrettyNoEscapes: 'PrettyNoEscapes',
  PrettySpace: 'PrettySpace',
  Protobuf: 'Protobuf',                 // https://github.com/protobufjs/protobuf.js
  ProtobufSingle: 'ProtobufSingle',
  RawBLOB: 'RawBLOB',
  Regexp: 'Regexp',
  RowBinary: 'RowBinary',
  RowBinaryWithNamesAndTypes: 'RowBinaryWithNamesAndTypes',
  TSKV: 'TSKV',
  TabSeparated: 'TabSeparated',
  TabSeparatedRaw: 'TabSeparatedRaw',
  TabSeparatedWithNames: 'TabSeparatedWithNames',
  TabSeparatedWithNamesAndTypes: 'TabSeparatedWithNamesAndTypes',
  Template: 'Template',
  TemplateIgnoreSpaces: 'TemplateIgnoreSpaces',
  Values: 'Values',
  Vertical: 'Vertical',
  XML: 'XML',
};
