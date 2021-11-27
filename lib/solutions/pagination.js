import { PageScroll } from 'acropolis-nd/lib/Euclid.js';

export class PageScrollExample extends PageScroll {
  constructor(
    {
      fnNext = (doc) => `(id > ${doc.id})`,
      fnPrev = (doc) => `(id < ${doc.id})`,
      vectorDefault = 10,
    } = {},
  ) {
    super({ fnNext, fnPrev, vectorDefault });
  }
}

export const scrollSelect = (where, vector) => {
  const order = (Math.sign(vector) === 1) ? 'ASC' : 'DESC';
  const limit = Math.abs(vector);
  return `
  SELECT
    toInt32(number) as id,
    toInt32(number % ${limit}) AS rowNumber,
    toInt32(number / ${limit}) AS pageNumber
  FROM numbers(0, 20)
  ${(where === undefined) ? '' : `WHERE ${where}`}
  ORDER BY id ${order}
  LIMIT ${limit}
  FORMAT JSON
`;
};
