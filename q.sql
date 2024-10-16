-- SELECT DISTINCT
--     CAST(NULL AS BIGINT),
--     'HJeihfbwei',
--     'HJeihfbwei' AS C1
-- FROM
--     PART,
--     SUPPLIER,
--     LINEITEM
-- ORDER BY
--     CAST(NULL AS DATE) NULLS FIRST
-- LIMIT
--     + CAST(NULL AS INTEGER)
-- (
--     (
--         SELECT
--             -3.213376226306912
--         FROM
--             CUSTOMER AS T1 (R1, R2, R3)
--         ORDER BY
--             CAST(NULL AS DATE) ASC NULLS LAST,
--             CAST(NULL AS TIMESTAMP) DESC
--         LIMIT
--             -5
--     )
-- )
-- LIMIT
--     @CAST(0 AS BIGINT)
-- SELECT DISTINCT
--     T1.L_COMMITDATE AS C2,
--     CAST(NULL AS TIMESTAMP) AS C3,
--     L_COMMITDATE
-- FROM
--     LINEITEM AS T1 (R1),
--     (
--         (
--             SELECT DISTINCT
--                 CAST(NULL AS INTERVAL) AS C1
--             FROM
--                 LINEITEM
--         )
--         ORDER BY
--             C1 ASC NULLS LAST,
--             C1 ASC,
--             C1 ASC NULLS FIRST
--     ) AS T2
-- ORDER BY
--     C3
(
    (
        SELECT DISTINCT
            CAST(NULL AS BIGINT),
            'HJeihfbwei',
            'HJeihfbwei' AS C1
        FROM
            PART,
            SUPPLIER,
            LINEITEM
        limit 1
    ) union
     (SELECT
            CAST(NULL AS BIGINT),
            'HJeihfbwei',
            'HJeihfbwei' AS C1
        FROM
            PART,
            SUPPLIER,
            LINEITEM
            limit 1)
)
order by c1 DESC
LIMIT 2