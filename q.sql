SELECT
    (
        SELECT DISTINCT
            T22.*
        FROM
            SUPPLIER AS T23 (R50, R51, R52, R53, R54),
            CUSTOMER
        WHERE
            CAST(NULL AS BOOLEAN)
        LIMIT
            1
    ) AS C10,
    (CAST(NULL AS TIMESTAMP)) AS C11
FROM
    SUPPLIER
    LEFT JOIN PARTSUPP ON (
        (
            CASE CAST(NULL AS DATE)
                WHEN CASE
                    WHEN true THEN DATE '2023-08-27'
                END THEN NOT EXISTS (
                    SELECT DISTINCT
                        -2
                    FROM
                        SUPPLIER
                )
            END
        )
    )
    JOIN PART ON DATE '2023-08-27' IS NOT DISTINCT FROM CASE PART.P_SIZE
        WHEN CAST(-4 AS BIGINT) THEN CAST(NULL AS DATE)
        ELSE CAST(NULL AS DATE)
    END,
    SUPPLIER AS T1 (R1, R2)
    JOIN REGION ON CASE CAST(NULL AS BOOLEAN)
        WHEN (
            (
                SELECT DISTINCT
                    CAST(NULL AS BOOLEAN)
                FROM
                    PARTSUPP
                LIMIT
                    1
            )
        ) THEN true
        WHEN (
            SELECT DISTINCT
                CAST(NULL AS BOOLEAN)
            FROM
                LINEITEM AS T2 (
                    R3,
                    R4,
                    R5,
                    R6,
                    R7,
                    R8,
                    R9,
                    R10,
                    R11,
                    R12,
                    R13,
                    R14
                )
            LIMIT
                1
        ) THEN CASE INTERVAL '1 day'
            WHEN INTERVAL '1 day' THEN false
            ELSE false
        END
        WHEN CASE DATE '2023-08-27'
            WHEN DATE '2023-08-27' THEN false
        END THEN true
        ELSE CAST(1 AS BIGINT) IS NOT NULL
    END
    JOIN ORDERS ON (
        SELECT
            T3.*
        FROM
            (
                SELECT DISTINCT
                    false AS C1
                FROM
                    SUPPLIER
                ORDER BY
                    C1 DESC
            ) AS T3
        WHERE
            T3.C1
        ORDER BY
            O_ORDERDATE
        LIMIT
            1
    )
    JOIN NATION ON CASE 'HJeihfbwei'
        WHEN CAST(NULL AS TEXT) THEN CAST(NULL AS TIMESTAMP)
        WHEN O_ORDERSTATUS THEN CAST(NULL AS DATE)
        ELSE O_ORDERDATE
    END IN (
        SELECT DISTINCT
            CAST(NULL AS DATE)
        FROM
            ORDERS
        GROUP BY
            true
        ORDER BY
            "date" DESC NULLS LAST,
            "date" DESC NULLS FIRST,
            "date" ASC,
            "date" NULLS FIRST,
            "date" ASC NULLS FIRST
    )
    LEFT JOIN LINEITEM ON (
        (
            (
                (
                    SELECT DISTINCT
                        CAST(NULL AS BOOLEAN) AS C2
                    FROM
                        LINEITEM AS T4 (R15)
                    ORDER BY
                        C2
                    LIMIT
                        1
                )
            )
        )
    )
    RIGHT JOIN (
        SELECT DISTINCT
            DATE '2023-08-27',
            C_NAME AS C4
        FROM
            CUSTOMER AS T6
            RIGHT JOIN NATION ON (
                SELECT DISTINCT
                    CAST(NULL AS BOOLEAN) AS C3
                FROM
                    ORDERS
                LIMIT
                    1
            )
            LEFT JOIN SUPPLIER ON CASE
                WHEN CAST(NULL AS BOOLEAN) THEN CAST(NULL AS BOOLEAN)
            END
        LIMIT
            (
                SELECT
                    CAST(-1 AS BIGINT) AS C5
                FROM
                    REGION
                ORDER BY
                    C5 DESC
                LIMIT
                    1
            )
    ) AS T5 ON (
        SELECT
            CAST(NULL AS BOOLEAN) AS C7
        FROM
            (
                SELECT
                    'HJeihfbwei'
                FROM
                    CUSTOMER AS T8 (R16, R17, R18, R19)
            ) AS T7
            LEFT JOIN (
                SELECT
                    CAST(NULL AS INTERVAL)
                FROM
                    NATION AS T10 (R20, R21)
                ORDER BY
                    "interval" ASC NULLS FIRST
            ) AS T9 ON true,
            ORDERS AS T11 (R22, R23, R24, R25, R26, R27)
            LEFT JOIN REGION AS T12 (R28, R29) ON CAST(NULL AS BOOLEAN)
            JOIN REGION ON true
            RIGHT JOIN (
                SELECT
                    INTERVAL '1 day' AS C6
                FROM
                    ORDERS
            ) AS T13 ON CAST(NULL AS BOOLEAN)
            LEFT JOIN NATION ON CAST(NULL AS BOOLEAN)
            RIGHT JOIN LINEITEM AS T14 (
                R30,
                R31,
                R32,
                R33,
                R34,
                R35,
                R36,
                R37,
                R38,
                R39,
                R40,
                R41,
                R42,
                R43,
                R44
            ) ON false
        WHERE
            CAST(NULL AS BOOLEAN)
        GROUP BY
            ROLLUP ((NATION.N_COMMENT, R37)),
            LINEITEM.L_DISCOUNT,
            GROUPING SETS (
                (),
                (L_ORDERKEY, L_SHIPINSTRUCT),
                (L_DISCOUNT, L_DISCOUNT)
            ),
            R32,
            N_NATIONKEY,
            L_ORDERKEY,
            T1.S_ACCTBAL,
            S_NATIONKEY,
            T14.R44,
            LINEITEM.L_DISCOUNT
        HAVING
            true
        ORDER BY
            C7 ASC
        LIMIT
            1
    )
    RIGHT JOIN (
        SELECT
            CAST(NULL AS BIGINT)
        FROM
            PARTSUPP AS T16
        WHERE
            TIMESTAMP '2023-01-30 14:37:05' IS NULL
        ORDER BY
            "int8" ASC NULLS LAST
        LIMIT
            (
                SELECT DISTINCT
                    -4.909749735534105 AS C8
                FROM
                    NATION AS T17 (R45)
                ORDER BY
                    C8 ASC NULLS FIRST,
                    C8 NULLS FIRST
                LIMIT
                    1
            )
    ) AS T15 ON (
        SELECT DISTINCT
            true
        FROM
            (
                SELECT DISTINCT
                    -3
                FROM
                    CUSTOMER
            ) AS T18
            RIGHT JOIN (
                SELECT DISTINCT
                    SUPPLIER.S_NAME AS C9
                FROM
                    SUPPLIER
            ) AS T19 ON false
            JOIN (
                SELECT DISTINCT
                    O_TOTALPRICE
                FROM
                    ORDERS
                ORDER BY
                    O_TOTALPRICE,
                    O_TOTALPRICE ASC NULLS LAST
            ) AS T20 ON CAST(NULL AS BOOLEAN)
            RIGHT JOIN ORDERS ON CAST(NULL AS BOOLEAN)
            LEFT JOIN PARTSUPP AS T21 (R46, R47, R48, R49) ON CAST(NULL AS BOOLEAN),
            CUSTOMER
        GROUP BY
            true
        LIMIT
            1
    )
    RIGHT JOIN (
        SELECT DISTINCT
            CAST(NULL AS INTERVAL)
        FROM
            PART
        WHERE
            false
        LIMIT
            CAST(NULL AS NUMERIC)
    ) AS T22 ON CAST(NULL AS BOOLEAN)
ORDER BY
    (
        SELECT DISTINCT
            CAST(NULL AS BIGINT)
        FROM
            NATION AS T24 (R55, R56, R57),
            LINEITEM,
            REGION
        WHERE
            NOT EXISTS (
                SELECT
                    4.133035376045177
                FROM
                    PARTSUPP AS T25
                ORDER BY
                    "int8"
            )
        LIMIT
            1
    ) DESC NULLS LAST,
    (CAST(NULL AS INTERVAL)) NULLS LAST,
    (
        (
            SELECT DISTINCT
                T22.*
            FROM
                PARTSUPP AS T26 (R58, R59, R60, R61)
            ORDER BY
                "interval" ASC
            LIMIT
                1
        ) + CAST(NULL AS DATE)
    ) NULLS FIRST
LIMIT
    CASE
        WHEN CASE CASE
                WHEN false THEN 4
                WHEN true THEN CAST(NULL AS INTEGER)
                ELSE CAST(NULL AS INTEGER)
            END
            WHEN CAST(0 AS BIGINT) THEN true
            WHEN CASE 'HJeihfbwei'
                WHEN 'HJeihfbwei' THEN CAST(NULL AS INTEGER)
                WHEN CAST(NULL AS TEXT) THEN CAST(NULL AS INTEGER)
                WHEN 'HJeihfbwei' THEN -1
                ELSE 4
            END THEN (
                SELECT
                    true AS C12
                FROM
                    ORDERS AS T27 (R62, R63)
                ORDER BY
                    C12 DESC,
                    C12 DESC NULLS FIRST
                LIMIT
                    1
            )
            WHEN @CAST(-5 AS BIGINT) THEN false
            WHEN + CAST(NULL AS INTEGER) THEN CAST(NULL AS BOOLEAN)
            ELSE CASE CAST(NULL AS INTEGER)
                WHEN -5 THEN true
                ELSE CAST(NULL AS BOOLEAN)
            END
        END THEN 2
        WHEN (
            SELECT
                CAST(NULL AS BOOLEAN) AS C13
            FROM
                ORDERS,
                LINEITEM
                JOIN PARTSUPP ON CAST(NULL AS BOOLEAN)
                JOIN REGION ON CAST(NULL AS BOOLEAN)
            GROUP BY
                GROUPING SETS ((O_COMMENT)),
                CUBE (REGION.R_REGIONKEY),
                PARTSUPP.PS_SUPPKEY
            HAVING
                CAST(NULL AS BOOLEAN)
            LIMIT
                1
        ) THEN + (
            SELECT
                CAST(NULL AS INTEGER)
            FROM
                LINEITEM
            ORDER BY
                "int4" DESC,
                "int4" ASC NULLS LAST
            LIMIT
                1
        )
        WHEN CASE CAST(NULL AS TIMESTAMP)
            WHEN DATE '2023-08-27' THEN NOT EXISTS (
                SELECT
                    DATE '2023-08-27' AS C14
                FROM
                    PART
                ORDER BY
                    C14 NULLS FIRST,
                    C14 ASC NULLS FIRST
            )
        END THEN -1
        WHEN (
            SELECT DISTINCT
                true AS C16
            FROM
                PARTSUPP AS T29 (R64, R65, R66)
                JOIN CUSTOMER ON true
            WHERE
                false
            HAVING
                CAST(NULL AS BOOLEAN)
            ORDER BY
                C16 DESC NULLS LAST
            LIMIT
                1
        ) THEN CASE CAST(NULL AS INTEGER)
            WHEN (
                SELECT DISTINCT
                    2 AS C15
                FROM
                    NATION AS T28
                ORDER BY
                    C15 DESC NULLS LAST
                LIMIT
                    1
            ) THEN 0
            WHEN (0) THEN CAST(NULL AS INTEGER)
            ELSE 5
        END
        ELSE CASE TIMESTAMP '2023-01-30 14:37:05'
            WHEN CASE
                WHEN CAST(NULL AS BOOLEAN) THEN CAST(NULL AS DATE)
            END THEN -4
            WHEN CAST(NULL AS DATE) THEN (
                SELECT DISTINCT
                    PS_AVAILQTY AS C17
                FROM
                    PARTSUPP
                LIMIT
                    1
            )
        END
    END