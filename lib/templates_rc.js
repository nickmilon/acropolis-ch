/**
 * nameSpace = db Dot Table  or if table only = default.table
 */
const sqlTemplates = {
    insertJson: nameSpace => `INSERT INTO ${nameSpace} FORMAT JSONEachRow`,
    findUserExact: (username, FORMAT = 'JSON') => `SELECT * FROM rapchat.accounts WHERE username = '${username}' LIMIT 1 FORMAT ${FORMAT}`,

    friends: ({ userId } = {}) => {
      return `
      SELECT children 
      FROM (
        SELECT children FROM rapchat.accounts_follow
        WHERE (parent = '${userId}')
        AS usersChildren
        )
        INNER JOIN rapchat.accounts_follow 
        ON parent = children
    `
    },
    feedRaps: ({userId, FORMAT = 'Pretty', LIMIT} = {}) => {
      return `
      SELECT
      parent AS following_id,
      rapchat.raps_oid._id AS rap_id,
      rapchat.raps_oid.__createdAt AS rap_createdAt,
      toStringCutToZero(substring(rapchat.raps_oid.name, 1, 40)) AS rap_name
      FROM 
      (
        SELECT parent
        FROM rapchat.accounts_follow
        WHERE children = '${userId}'
      ) AS following
      INNER JOIN rapchat.raps_oid ON owner_id = parent AND isPublic = 'true'
      ORDER BY __createdAt DESC
      ${LIMIT}
      FORMAT ${FORMAT}
    `
    }
}

export {
  sqlTemplates,
};
