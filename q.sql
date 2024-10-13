SELECT
    CUSTOMER.C_PHONE
FROM
    CUSTOMER
    LEFT JOIN CUSTOMER AS T1 (R1, R2) ON false,
    SUPPLIER
    LEFT JOIN (
        SELECT DISTINCT
            CAST(NULL AS DATE) AS C1,
            0 AS C2
        FROM
            PARTSUPP
        WHERE
            (CAST(NULL AS BOOLEAN))
    ) AS T2 ON false
    LEFT JOIN (
        SELECT
            DATE '2023-08-27',
            CAST(NULL AS DATE)
        FROM
            REGION AS T4
        ORDER BY
            (CAST(NULL AS TEXT)) ASC NULLS FIRST
    ) AS T3 ON false
ORDER BY
    C_PHONE NULLS LAST,
    (
        SELECT
            CASE
                WHEN CAST(NULL AS BOOLEAN) THEN DATE '2023-08-27'
            END
        FROM
            SUPPLIER,
            NATION AS T5 (R3, R4, R5)
            JOIN NATION AS T6 ON CAST(3 AS BIGINT) IS NOT NULL
        HAVING
            EVERY (true)
    ) DESC,
    (
        SELECT DISTINCT
            INTERVAL '1' DAY
        FROM
            (
                SELECT
                    CUSTOMER.C_PHONE AS C3
                FROM
                    ORDERS AS T8 (R6, R7)
                ORDER BY
                    CAST(NULL AS INTERVAL) ASC
            ) AS T7
            JOIN PARTSUPP ON 3.6378537098378647 IS NOT NULL
            JOIN SUPPLIER ON true
            JOIN PARTSUPP AS T9 (R8, R9, R10, R11) ON (CAST(NULL AS BOOLEAN))
            JOIN ORDERS ON CASE
                WHEN CAST(NULL AS BOOLEAN) THEN CAST(NULL AS BOOLEAN)
            END
            JOIN SUPPLIER AS T10 (R12, R13, R14, R15, R16, R17) ON (
                SELECT
                    false AS C4
                FROM
                    ORDERS
                LIMIT
                    1
            ),
            PART
            JOIN CUSTOMER ON CASE CAST(NULL AS DATE)
                WHEN DATE '2023-08-27' THEN false
                ELSE CAST(NULL AS BOOLEAN)
            END,
            REGION
            JOIN (
                SELECT
                    T2.*,
                    CUSTOMER.C_CUSTKEY
                FROM
                    REGION
                ORDER BY
                    CAST(NULL AS DATE) NULLS FIRST
                LIMIT
                    -0.5469951196215224
            ) AS T11 (R18) ON CAST(NULL AS BOOLEAN)
            LEFT JOIN NATION ON true
            RIGHT JOIN (
                SELECT
                    CAST(NULL AS NUMERIC)
                FROM
                    PARTSUPP AS T13 (R19)
            ) AS T12 ON CASE 'HJeihfbwei'
                WHEN 'HJeihfbwei' THEN CAST(NULL AS BOOLEAN)
                WHEN 'HJeihfbwei' THEN CAST(NULL AS BOOLEAN)
                WHEN REGION.R_COMMENT THEN false
            END
            JOIN (
                SELECT
                    CAST(NULL AS DATE) AS C5
                FROM
                    NATION AS T15 (R20, R21)
                ORDER BY
                    C5
                LIMIT
                    CAST(NULL AS NUMERIC)
            ) AS T14 ON (
                SELECT
                    false
                FROM
                    NATION
                ORDER BY
                    "bool" DESC,
                    "bool" DESC NULLS FIRST,
                    "bool"
                LIMIT
                    1
            )
            JOIN (
                SELECT
                    CAST(NULL AS INTEGER),
                    CAST(NULL AS BIGINT)
                FROM
                    PARTSUPP AS T17 (R22, R23, R24)
                ORDER BY
                    "int8",
                    "int8" NULLS FIRST
                LIMIT
                    -3
            ) AS T16 (R25) ON CASE TIMESTAMP '2023-01-30 14:37:05'
                WHEN DATE '2023-08-27' THEN CAST(NULL AS BOOLEAN)
                WHEN TIMESTAMP '2023-01-30 14:37:05' THEN false
                WHEN DATE '2023-08-27' THEN false
                WHEN DATE '2023-08-27' THEN CAST(NULL AS BOOLEAN)
            END
        GROUP BY
            T1.C_NATIONKEY
        HAVING
            BOOL_AND (CAST(NULL AS BOOLEAN))
        ORDER BY
            "interval" NULLS FIRST
        LIMIT
            1
    ) DESC NULLS FIRST,
    (SUPPLIER.S_ACCTBAL) NULLS FIRST
LIMIT
    EXTRACT(
        YEAR
        FROM
            TIMESTAMP '2023-01-30 14:37:05'
    )









-- inner



SELECT DISTINCT
    INTERVAL '1' DAY
FROM
    (
        SELECT
            CUSTOMER.C_PHONE AS C3
        FROM
            ORDERS AS T8 (R6, R7)
        ORDER BY
            CAST(NULL AS INTERVAL) ASC
    ) AS T7
    JOIN PARTSUPP ON 3.6378537098378647 IS NOT NULL
    JOIN SUPPLIER ON true
    JOIN PARTSUPP AS T9 (R8, R9, R10, R11) ON (CAST(NULL AS BOOLEAN))
    JOIN ORDERS ON CASE
        WHEN CAST(NULL AS BOOLEAN) THEN CAST(NULL AS BOOLEAN)
    END
    JOIN SUPPLIER AS T10 (R12, R13, R14, R15, R16, R17) ON (
        SELECT
            false AS C4
        FROM
            ORDERS
        LIMIT
            1
    ),
    PART
    JOIN CUSTOMER ON CASE CAST(NULL AS DATE)
        WHEN DATE '2023-08-27' THEN false
        ELSE CAST(NULL AS BOOLEAN)
    END,
    REGION
    JOIN (
        SELECT
            T2.*,
            CUSTOMER.C_CUSTKEY
        FROM
            REGION
        ORDER BY
            CAST(NULL AS DATE) NULLS FIRST
        LIMIT
            -0.5469951196215224
    ) AS T11 (R18) ON CAST(NULL AS BOOLEAN)
    LEFT JOIN NATION ON true
    RIGHT JOIN (
        SELECT
            CAST(NULL AS NUMERIC)
        FROM
            PARTSUPP AS T13 (R19)
    ) AS T12 ON CASE 'HJeihfbwei'
        WHEN 'HJeihfbwei' THEN CAST(NULL AS BOOLEAN)
        WHEN 'HJeihfbwei' THEN CAST(NULL AS BOOLEAN)
        WHEN REGION.R_COMMENT THEN false
    END
    JOIN (
        SELECT
            CAST(NULL AS DATE) AS C5
        FROM
            NATION AS T15 (R20, R21)
        ORDER BY
            C5
        LIMIT
            CAST(NULL AS NUMERIC)
    ) AS T14 ON (
        SELECT
            false
        FROM
            NATION
        ORDER BY
            "bool" DESC,
            "bool" DESC NULLS FIRST,
            "bool"
        LIMIT
            1
    )
    JOIN (
        SELECT
            CAST(NULL AS INTEGER),
            CAST(NULL AS BIGINT)
        FROM
            PARTSUPP AS T17 (R22, R23, R24)
        ORDER BY
            "int8",
            "int8" NULLS FIRST
        LIMIT
            -3
    ) AS T16 (R25) ON CASE TIMESTAMP '2023-01-30 14:37:05'
        WHEN DATE '2023-08-27' THEN CAST(NULL AS BOOLEAN)
        WHEN TIMESTAMP '2023-01-30 14:37:05' THEN false
        WHEN DATE '2023-08-27' THEN false
        WHEN DATE '2023-08-27' THEN CAST(NULL AS BOOLEAN)
    END
GROUP BY
    T1.C_NATIONKEY
HAVING
    BOOL_AND (CAST(NULL AS BOOLEAN))