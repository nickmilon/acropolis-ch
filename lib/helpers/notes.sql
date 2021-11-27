<!--cSpell:disable -->

random dates https://altinity.com/blog/harnessing-the-power-of-clickhouse-arrays-part-3

44423
INDEX d1_null_idx assumeNotNull(d1_null) TYPE minmax GRANULARITY 1
INDEX idx_parent parent TYPE set(1000) GRANULARITY 8192
INDEX idx_parent parent TYPE minmax GRANULARITY 3  not bad  GRANULARITY 1 is better 
INDEX idx_bloom (parent, child)  TYPE bloom_filter  GRANULARITY  1 `
EXPLAIN PLAN indexes=1 SELECT　
-------------------------------------------------------------------
ALTER TABLE graph100k.Graph  DROP INDEX idx_bloom
ALTER TABLE graph100k.Graph ADD INDEX idx_parent parent TYPE minmax GRANULARITY
ALTER TABLE graph100k.Graph MATERIALIZE INDEX idx_parent

explain plan actions=1 SELECT
            parent,
            sum(sign) AS children
        FROM graph100k.Graph 
        GROUP BY parent  order by children DESC limit 10

-------------------------------------------------------------------
 
-------------------------------------------------------------------

---------------------------------------------------------------------------------

SELECT
    sum(sign) AS total, parent
FROM graph100k.Graph
GROUP BY parent
ORDER BY total DESC
LIMIT 10

SELECT
    child,
    sum(sign) AS parents
FROM graph100k.Graph
GROUP BY child
LIMIT 10


CREATE LIVE VIEW graph100k.LVparentCount AS
SELECT
    child,
    sum(sign) AS parents
FROM graph100k.Graph
GROUP BY child

CREATE LIVE VIEW graph100k.LVchildCount AS
SELECT
    parent,
    sum(sign) AS children
FROM graph100k.Graph
GROUP BY parent 


CREATE LIVE VIEW graph1m.LVparentCount AS
SELECT
    child,
    sum(sign) AS parents
FROM graph1m.Graph
GROUP BY child

 
--------------------------------
fucked 
SELECT
    parent,
    sum(sign) AS children
FROM graph1m.GraphBin
GROUP BY parent
ORDER BY children DESC
LIMIT 10

SELECT
    child,
    sum(sign) AS parents
FROM graph1m.GraphBin
GROUP BY child
ORDER BY parents DESC
LIMIT 10
------------------------------------
CREATE TABLE graph1m.GraphBin
(
    parent UInt32,
    child UInt32,
    sign Int8,
    relation UInt64,
    binRel UInt64,
    dtCr DateTime
)
ENGINE = MergeTree
ORDER BY (binRel)
SETTINGS index_granularity = 8192
---------------------------------------------
CREATE TABLE deltb (binRel UInt64,binRel2 UInt128 ) ENGINE = MergeTree


INSERT INTO graph1m.GraphV2 SELECT * FROM graph1m.Graph

INSERT INTO graph1m.GraphBin SELECT *, nm_Pack32(parent, child) AS binRel FROM graph1m.Graph

SELECT * FROM graph1m.GraphBin WHERE child = 943775
SELECT count(*) as count FROM graph1m.GraphBin WHERE parent = 943775

SELECT
    *,
    nm_Pack32(parent, child) AS pack
FROM graph1m.GraphBin
WHERE (parent = 943775) AND (child = 458146)

Query id: 5b6b9030-08ad-4008-89db-45fd141f7e46



CREATE TABLE graph1m.GraphV2
(
    parent UInt32,
    child UInt32,
    sign Int8,
    relation MATERIALIZED (parent + child),
    dtCr DateTime,
    binRel MATERIALIZED nm_Pack32(parent, child)
)
ENGINE = MergeTree
PRIMARY KEY (parent, child)
ORDER BY (parent, child)
SETTINGS index_granularity = 8192

INSERT INTO graph1m.GraphBin SELECT nm_Pack32(parent, child) AS binRel FROM graph1m.Graph WHERE (parent = 943775) AND (child = 458146)

SELECT DISTINCT(parent) as distParent, * FROM graph1m.GraphBin WHERE binRel >=  nm_Pack32(1000000,0)  and binRel < nm_Pack32(1000001,  0xFFFFFFFF) limit 1

-------------------------------------------------------
check savings by sum

CREATE TABLE graph1m.testRel
(
    `parent` UInt32,
    `child` UInt32,
    `relation` MATERIALIZED parent + child,
)
 
================================================================= loop

SELECT countDistinct([parent, child]) as distinct
FROM
(
    SELECT
        child,
        parent,
        child + parent AS rel
    FROM
    (
        SELECT number AS child
        FROM numbers(1, 10000)
    ) AS children,
    (
        SELECT number AS parent
        FROM numbers(1, 10000)
    ) AS parents
    WHERE child != parent
)

1000    999000    vs 1997
10000   99990000  vs 19997
100000            vs   199997
0.00199 of records
-----------------------------------------------------
DROP VIEW graph1m.graphTotals;

CREATE MATERIALIZED VIEW graph1m.graphTotals
ENGINE = SummingMergeTree
ORDER BY (child, parent)
POPULATE
AS SELECT
   child,
   parent,
   sum(sign) AS parents
   count() as rows
FROM graph1m.GraphV2
WHERE child < 10000 AND parent < 10000
GROUP BY
    child,parent

CREATE LIVE VIEW graph1m.LVparentCount AS
SELECT
    child,
    sum(sign) AS parents
FROM graph1m.Graph
GROUP BY child
--------------------------------------
CREATE LIVE VIEW graph100k.LVparentCount AS
SELECT
    child,
    sum(sign) AS parents
FROM graph100k.Graph
GROUP BY child

SELECT parent
FROM twitter.edgesV4
WHERE child = target
GROUP BY parent
HAVING sum(sign) = 1
ORDER BY parent ASC
LIMIT 10


SELECT *
    FROM
    ( 
      SELECT
      child,
      sum(sign) AS parents 
      FROM graph1m.GraphV2
      WHERE child < 4
      GROUP BY child 
    ) AS children,
    (
      SELECT
      parent,
      sum(sign) AS children 
      FROM graph1m.GraphV2
      WHERE child < 4
      GROUP BY parent  
    ) AS parents
    

SELECT *
    FROM
    ( 
      SELECT
      child,
      sum(sign) AS parents 
      FROM graph1m.GraphV2
      GROUP BY child 
    ) AS children 
    JOIN 
    (
      SELECT
      parent, child
      sum(sign) AS children 
      FROM graph1m.GraphV2
      GROUP BY parent, child  
    ) AS parents
    ON child = parent

--------------------------------------------
should give mutual but memory crash
SELECT *
FROM
(
    SELECT
        parent,
        child,
        sum(sign) AS rel
    FROM graph1m.GraphV2
    WHERE child < 700000
    GROUP BY nm_Pack32(parent,child)
        parent,
        child
)
--------------------------------------------
SELECT *
    FROM
    ( 
      SELECT
      child,
      sum(sign) AS parents 
      FROM graph1m.GraphV2
      GROUP BY child 
    ) AS children, 
      
    (
      SELECT
      any(child) as xxxxxxxxxxxxxx,
      sum(sign) AS children 
      FROM graph1m.GraphV2
      WHERE parent = child
    ) AS parents
   limit 50


SELECT * FROM (
    SELECT sum(sign) as parents
    FROM graph1m.GraphV2
    WHERE child = item,
    ) as parent,

    SELECT sum(sign) as children
    FROM graph1m.GraphV2
    WHERE parent = item
)
  SELECT
      child as child,
      sum(sign) AS parents 
      FROM graph1m.GraphV2
      WHERE child < 100
      GROUP BY child,
  SELECT
      parent as child,
      sum(sign) AS children 
      FROM graph1m.GraphV2
      WHERE parent < 100
      GROUP BY parent 
)

--------------------------------------------
WITH 20 as acoount
SELECT *
    FROM
    ( 
      SELECT
      sum(sign) AS parents 
      FROM graph1m.GraphV2
      WHERE child = acoount
    ) as p,
    (
      SELECT
      sum(sign) AS children 
      FROM graph1m.GraphV2
      WHERE parent = acoount
    ) as c
----------------------------------------



SELECT
    child,
    sum(sign) AS parents
FROM graph1m.GraphV2
WHERE child < 10
GROUP BY child

SELECT
    parent,
    sum(sign) AS children
FROM graph1m.GraphV2
GROUP BY parent
--------------------------- projections
https://blog.tinybird.co/2021/07/09/projections/
https://xie.infoq.cn/article/f5ef2cfb9cd41bd817f5d1075

ALTER TABLE graph1m.GraphV2 ADD PROJECTION proj_parents (SELECT child, sum(sign) AS parents GROUP BY child)
ALTER TABLE graph1m.GraphV2 MATERIALIZE PROJECTION proj_parents
ALTER TABLE graph1m.GraphV2  DROP PROJECTION proj_parents

ALTER TABLE graph1m.GraphV2 ADD PROJECTION proj_children (SELECT parent, sum(sign) AS children GROUP BY parent)



ALTER TABLE github_events ADD PROJECTION projection_user_sort ( SELECT * ORDER BY username );

 CREATE TABLE graph1m.GraphV2
(
    `parent` UInt32,
    `child` UInt32,
    `sign` Int8,
    `relation` UInt64 MATERIALIZED parent + child,
    `dtCr` DateTime,
    `binRel` UInt64 MATERIALIZED nm_Pack32(parent, child),
    PROJECTION proj_parents
    (
        SELECT 
            child,
            sum(sign) AS parents
        GROUP BY child
    )
)
ENGINE = MergeTree
PRIMARY KEY binRel
ORDER BY (binRel, dtCr)
SETTINGS index_granularity = 8192 
-----------------------------------------
drop cashes to check queries 
https://stackoverflow.com/questions/58653313/clickhouse-select-query-without-cache

sudo sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
SYSTEM DROP MARK CACHE
SYSTEM DROP UNCOMPRESSED CACHE
------------------------------------------------------------------------------------
select toDateTime(toUInt32(nm_RandWithin(1072915200, 1230768000)))
-----------------------
nm_ScaleWithin: 'CREATE FUNCTION IF NOT EXISTS nm_ScaleWithin AS (num, minAllowed, maxAllowed, min, max) -> (maxAllowed - minAllowed) * (num - min) / (max - min) + minAllowed',
  nm_RandWithin: 'CREATE FUNCTION IF NOT EXISTS nm_RandWithin AS (min, max) -> nm_ScaleWithin(rand32(), min, max, 0, 4294967295)',

nm_ScaleWithin(id,)

--------------------------------------------------------- create nodes Elapsed: 237.493 sec. Processed 2.94 billion rows, 11.75 GB (12.37 million rows/s., 49.46 MB/s.)

INSERT INTO graphTwitter.accounts
SELECT id, dtCr, randomPrintableASCII(8) as name FROM 
( 
SELECT toDateTime(toUInt32(nm_ScaleWithin(id, 1072915200, 1230768000, 1,  41652229))) as dtCr, id FROM 
 (
    SELECT DISTINCT child AS id
    FROM graphTwitter.Graph  
    UNION DISTINCT 
    SELECT DISTINCT parent AS id
    FROM graphTwitter.Graph  
 )
)
----------------------------------------------------------
CREATE TABLE graphTwitter.accounts
      (
        id UInt32,
        dtCr DateTime,
        name string(8)
      )
      ENGINE = MergeTree()
      PRIMARY KEY id
      ORDER BY (id, dtCr)
      
      SETTINGS index_granularity = 8192

---------------------------------



CREATE TABLE twitter.edgesV2
(
    parent UInt32,
    child UInt32,
    sign Int8,
    dtCr DateTime
)
ENGINE = MergeTree
PRIMARY KEY (parent, child)
ORDER BY (parent, child)
PARTITION BY (parent % 10)
SETTINGS index_granularity = 8192

-------------------------------  Elapsed: 2009.180 sec. Processed 1.47 billion rows, 19.09 GB (730.83 thousand rows/s., 9.50 MB/s.)
INSERT INTO twitter.edges SELECT *
FROM graphTwitter.Graph
------------------------------------------------
SELECT
    parent,
    sum(sign) AS children
FROM twitter.edgesV2
GROUP BY parent
ORDER BY children DESC 
LIMIT 30

SELECT
    child,
    sum(sign) AS parents
FROM twitter.edgesV2
GROUP BY child
ORDER BY parents DESC
LIMIT 30


SELECT b
FROM set_index
WHERE (a = 1) AND (b = 1) AND (a = 1)
SETTINGS force_data_skipping_indices = 'b_set', optimize_move_to_prewhere=0
------------------------ 
ALTER TABLE twitter.edgesV5 ADD COLUMN pcArr Array(UInt32) MATERIALIZED [parent, child]
ALTER TABLE twitter.edgesV5 MATERIALIZE COLUMN pcArr
ALTER TABLE twitter.edgesV5 ADD INDEX idx_bloom (parent, child)  TYPE bloom_filter  GRANULARITY 100
ALTER TABLE twitter.edgesV5 MATERIALIZE INDEX idx_bloom
-----------------------------
ALTER TABLE twitter.edgesV2 ADD PROJECTION proj_parents (SELECT child, sum(sign) AS parents GROUP BY child)

ALTER TABLE twitter.edgesV2 ADD PROJECTION proj_order_cp ( SELECT * ORDER BY (child,parent );

 explain actions=1 SELECT
            child,
            sum(sign) AS parents
        FROM twitter.edgesV2
        GROUP BY child
        ORDER BY parents DESC
        LIMIT 30

OPTIMIZE TABLE  FINAL

NSERT INTO twitter.edgesV2  
SELECT parent, child, -1 as sign, date_add(day,1, dtCr) AS dtCr  FROM twitter.edgesV2 SAMPLE 0.001 LIMIT 1000000

----------------------------------------- mutual fast but takes no account of sign ? 
WITH 21515805 AS target
SELECT count() AS count
FROM twitter.edgesV2 AS t1
WHERE (child = target) AND (parent IN (
    SELECT child
    FROM twitter.edgesV2
    WHERE parent = target
))
=> 105750 fastst

WITH 21515805 AS target
SELECT parent AS id
FROM twitter.edgesV2 AS t1
WHERE (child = target) AND (parent IN (
    SELECT child
    FROM twitter.edgesV2
    WHERE parent = target
))

------------------------------------------------- works but slow 
WITH 21515805 AS target
SELECT 
    relation as mutual
  FROM 
   (SELECT
    count (*) AS cnt,
    if (child = target, parent, child) as relation
    FROM twitter.edgesV2
    WHERE child = target OR parent = target
    GROUP BY relation
   )
 WHERE cnt > 1  
  => 105750


 arrayIntersect
--------------------------------------------------- very slow
WITH 21515805 AS target
SELECT * FROM 
(
    SELECT child as id
    FROM graphTwitter.Graph
    WHERE parent = target  
    INTERSECT
    SELECT parent as id
    FROM graphTwitter.Graph
    WHERE child = target  
 )

 => 105750  

 WITH 21515805 AS target
SELECT * FROM 
(
    ( SELECT child as id
    FROM graphTwitter.Graph
    WHERE parent = target) AS FOO,
    (  
    SELECT parent as id
    FROM graphTwitter.Graph
    WHERE child = target  
    ) as bar
 )

-------------------------------------------------------- with sign check
 
WITH 21515805 AS target
SELECT
    child,
    sum(sign) AS checkSum
FROM twitter.edgesV2
where parent = target 
GROUP BY child
HAVING checkSum = 1
 

WITH 21515805 AS target
SELECT
    parent,
    sum(sign) AS checkSum
FROM twitter.edgesV2
where child = target 
GROUP BY parent
HAVING checkSum = 1





 CREATE TABLE twitter.edgesV2
(
    `parent` UInt32,
    `child` UInt32,
    `sign` Int8,
    `dtCr` DateTime,
    PROJECTION proj_order_cp
    (
        SELECT *
        ORDER BY (child, parent)
    ),
    PROJECTION proj_parents
    (
        SELECT 
            child,
            sum(sign) AS parents
        GROUP BY child
    )
)
ENGINE = MergeTree
PARTITION BY parent % 10
PRIMARY KEY (parent, child)
ORDER BY (parent, child)
SAMPLE BY parent
SETTINGS index_granularity = 8192


ORDER BY parents DESC
LIMIT 30

WITH 21515805 AS target
SELECT
    child,
    sum(sign) AS parents
FROM twitter.edgesV2
where parent = target 
GROUP BY child
limit 3

SELECT
    child,
    sum(sign) AS parents
FROM twitter.tiny
GROUP BY child

SELECT
    groupArray(parent),
    sum(sign) AS children
FROM twitter.tiny
GROUP BY parent
 
WITH 1 AS target
SELECT count() AS count
FROM twitter.tiny AS t1
WHERE (parent = target) AND (child IN (
    SELECT child
    FROM twitter.tiny
    WHERE parent = child = target AND parent = child
))


WITH 4 AS target
SELECT *
FROM twitter.tiny
WHERE (parent = target) AND (child IN (
    WITH child AS cc
    SELECT parent
    FROM twitter.tiny
    WHERE (parent = 1) AND (child = target)
))

============================================================= 
WITH 1 AS target
SELECT
    child,
    sum(sign) AS checkSum
FROM twitter.tiny
where parent = target
GROUP BY child
HAVING checkSum = 1

WITH 1 AS target
SELECT
    parent,
    sum(sign) AS checkSum
FROM twitter.tiny
where child = target
GROUP BY parent
HAVING checkSum = 1

----------------------- mutual
WITH 1 AS target
SELECT
    child
FROM twitter.tiny
where parent = target
GROUP BY child
HAVING sum(sign) = 1
AS t1
INTERSECT 
SELECT
    parent
FROM twitter.tiny
where child = target
GROUP BY parent
HAVING sum(sign) = 1
 

WITH 1 AS target
SELECT parents, children, arrayIntersect(parents, children) as mutual
FROM
(
    SELECT
        (
            SELECT groupArray(parent)
            FROM
            (
                SELECT parent
                FROM twitter.tiny
                WHERE child = target
                GROUP BY parent
                HAVING sum(sign) = 1
            )
        ) AS parents,
        (
            SELECT groupArray(child)
            FROM
            (
                SELECT child
                FROM twitter.tiny
                WHERE parent = target
                GROUP BY child
                HAVING sum(sign) = 1
            )
        ) AS children
)

44444 all 
┌─parents─────────────┬─children────────────┬─mutual──────────────┐
│ [44445,44446,44447] │ [44445,44446,44447] │ [44445,44447,44446] │
└─────────────────────┴─────────────────────┴─────────────────────┘


WITH 1 AS target
SELECT parents, children, arrayIntersect(parents, children) as mutual
FROM
(
    SELECT
        (
            SELECT groupArray(parent)
            FROM
            (
                SELECT parent
                FROM twitter.tiny
                WHERE child = target
                GROUP BY parent
                HAVING sum(sign) = 1
            )
        ) AS parents,
        (
            SELECT groupArray(child)
            FROM
            (
                SELECT child
                FROM twitter.tiny
                WHERE parent = target
                GROUP BY child
                HAVING sum(sign) = 1
            )
        ) AS children
)


WITH 23934048 AS target
SELECT
    parents,
    children
FROM
(
    SELECT
        (
            SELECT count(parent)
            FROM
            (
                SELECT parent
                FROM twitter.edgesV2
                WHERE child = target
                GROUP BY parent
                HAVING sum(sign) = 1
            )
        ) AS parents,
        (
            SELECT count(child)
            FROM
            (
                SELECT child
                FROM twitter.edgesV2
                WHERE parent = target
                GROUP BY child
                HAVING sum(sign) = 1
            )
        ) AS children
)



=======================================================  parents 
SELECT
    parent,
    sum(sign) AS children
FROM twitter.edgesV4
GROUP BY parent
ORDER BY children DESC 
LIMIT 10
======================================================= children
SELECT
    child,
    sum(sign) AS parents
FROM twitter.edgesV4
GROUP BY child
ORDER BY parents DESC
LIMIT 10
==============================================================
WITH 23934048 AS target
SELECT parent
        FROM twitter.edgesV4
        WHERE child = target
        GROUP BY parent
        HAVING sum(sign) = 1
        LIMIT 10

Elapsed: 0.376 sec. Processed 442.37 thousand rows, 3.98 MB (1.18 million rows/s., 10.60 MB/s.)
cashed   0.120

WITH 23934048 AS target
SELECT child
        FROM twitter.edgesV4
        WHERE parent = target
        GROUP BY child
        HAVING sum(sign) = 1
        LIMIT 10

Elapsed: 1.873 sec. Processed 2.73 million rows, 24.55 MB (1.46 million rows/s., 13.11 MB/s.)
cashed   0.2
  PrimaryKey Keys:  parent  
--------------------------------------------------------------
CREATE TABLE twitter.edgesV6
(
    `feed` UInt16,
    `parent` UInt32,
    `child` UInt32,
    `sign` Int8,
    `dtCr` DateTime,
    PROJECTION proj_parent_child
    ( SELECT * ORDER BY (feed, parent, child)),
    PROJECTION proj_parents
    (
        SELECT 
            parent,
            sum(sign) AS children
        GROUP BY parent
    ),
    PROJECTION proj_children
    (
        SELECT 
            child,
            sum(sign) AS parents
        GROUP BY child
    )
)
ENGINE = MergeTree
PRIMARY KEY (feed, child, parent)
ORDER BY (feed, child, parent)
SAMPLE BY child
SETTINGS index_granularity = 8192

CREATE VIEW twitter.edgesV5vParents AS
SELECT
    child,
    sum(sign) AS parents
FROM twitter.edgesV4
GROUP BY child


-----------------------------------------------------------------
WITH 21513299 AS target
SELECT parent
FROM twitter.edgesV2
WHERE child = target
GROUP BY parent
HAVING sum(sign) = 1
LIMIT 10


WITH 23934048 AS target
SELECT child
FROM twitter.edgesV2
WHERE parent = target
GROUP BY child
HAVING sum(sign) = 1
LIMIT 10


SELECT
  (SELECT 1) AS a,
  (SELECT 2) AS b


  

CREATE TABLE twitter.edgesV5
(
    `child` UInt32,
    `parents` UInt32
) 
ENGINE = MergeTree
PRIMARY KEY (parents)
ORDER BY (parents)                                             
SAMPLE BY child
SETTINGS index_granularity = 8192

================================ children count top
WITH 23934132 AS target
┌─parents─┬─children─┐
│     183 │  2997469 │
└─────────┴──────────┘
23934196
┌───parent─┬─children─┐
│ 23934132 │  2997469 │
│ 23934049 │  2679639 │
│ 23934048 │  2674874 │
│ 23934050 │  2450749 │
│ 23934199 │  1994926 │
│ 23934137 │  1959708 │
│ 23934177 │  1885782 │
│ 21513299 │  1882889 │
│ 23934172 │  1844499 │
│ 23932899 │  1843561 │
│ 23932071 │  1790771 │
│ 23934133 │  1691919 │
│ 23934051 │  1668193 │
│ 23932065 │  1657119 │
│ 23934060 │  1651207 │
│ 23934063 │  1524048 │
│ 23934079 │  1517067 │
│ 24893630 │  1477423 │
│ 23934131 │  1380160 │
│ 23934144 │  1377332 │
│ 23934900 │  1318909 │
│ 23932900 │  1318378 │
│ 23934025 │  1278103 │
│ 23934044 │  1277163 │
│ 24891383 │  1269341 │
│ 23932069 │  1241331 │
│ 23934142 │  1213787 │
│ 23934136 │  1210996 │
│ 23934180 │  1200472 │
│ 23934201 │  1195089 │
└──────────┴──────────┘
┌────child─┬─parents─┐
│ 21513299 │  770155 │
│ 23933989 │  505613 │
│ 23933986 │  498700 │
│ 23937213 │  407705 │
│ 23934048 │  406238 │
│ 23934131 │  369569 │
│ 23934073 │  283435 │
│ 23934123 │  263317 │
│ 23934033 │  143408 │
│ 21515005 │  140903 │
│ 21511313 │  138045 │
│ 23934069 │  134788 │
│ 21515742 │  123051 │
│ 23934128 │  119716 │
│ 21515941 │  119596 │
│ 21515707 │  119531 │
│ 23934127 │  119279 │
│ 21515805 │  115293 │
│ 21515809 │  110772 │
│ 21511314 │  109377 │
│ 21515803 │  109279 │
│ 21515782 │  108615 │
│ 21512481 │  107580 │
│ 21515771 │  106294 │
│ 21515997 │  105289 │
│ 21515804 │  102827 │
│ 21515985 │   99668 │
│ 21512480 │   99184 │
│ 21523985 │   99102 │
│ 21515684 │   97050 │
└──────────┴─────────┘

top Mutual 
[
  23934048, 
  21513299,
  23934131, 
  23934128,
  23933986,
  23933989,
  23937213, 
  23934127,
  23934073, 
  23934123,
  23934069
]
mutual 23934048 38364033
LIMIT 3

==============================================

