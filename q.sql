SELECT DISTINCT
    T4.*,
    (
        SELECT
            T4.*
        FROM
            REGION
        LIMIT
            1
    )
FROM
    (
        SELECT
            1 AS C4
        FROM
            LINEITEM
    ) AS T4
ORDER BY
    C4;

-- select
--     (
--         case
--             when false then date '01.01.2124'
--             else date '01.01.2024'
--         end
--     ) - date (
--         case
--             when false then date '01.01.2123'
--             else date '01.01.2023'
--         end
--     );
-- SELECT
--     1,
--     T1.*
-- FROM
--     (
--         SELECT
--             CAST(NULL AS TEXT)
--         FROM
--             NATION
--     ) AS T1
-- INTERSECT
-- SELECT
--     1.2,
--     CAST(NULL AS TEXT)
-- FROM
--     NATION
-- SELECT
--     1 AS C2,
--     T1.*
-- FROM
--     (
--         SELECT
--             CAST(NULL AS TEXT)
--         FROM
--             CUSTOMER
--     ) AS T1
-- INTERSECT
-- (
--     SELECT
--         4.993609530061892,
--         CAST(NULL AS TEXT) AS C3
--     FROM
--         NATION
-- )
-- SELECT
--     T1.*
-- FROM
--     (
--         SELECT
--             CAST(NULL AS BIGINT)
--         FROM
--             NATION
--     ) AS T1
-- INTERSECT
-- SELECT
--     T2.*
-- FROM
--     (
--         SELECT
--             CAST(NULL AS NUMERIC)
--         FROM
--             NATION
--     ) AS T2
-- SELECT
--     T1.*
-- FROM
--     (
--         SELECT
--             CAST(NULL AS INTEGER)
--         FROM
--             NATION
--     ) AS T1
-- INTERSECT
-- SELECT
--     CAST(NULL AS NUMERIC)
-- FROM
--     NATION
-- SELECT
--     CAST(NULL AS INTEGER)
-- FROM
--     NATION
-- INTERSECT
-- SELECT
--     CAST(NULL AS NUMERIC)
-- FROM
--     NATION