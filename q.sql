(
    SELECT DISTINCT
        ((DATE '2023-08-27') + CAST(NULL AS INTEGER)),
        MAX(LINEITEM.L_COMMITDATE),
        CAST(NULL AS INTERVAL) AS C1,
        CAST(NULL AS TIMESTAMP)
    FROM
        PART AS T1 (R1, R2, R3, R4, R5, R6, R7)
        JOIN REGION ON (CAST(NULL AS BOOLEAN))
        LEFT JOIN LINEITEM ON CASE
            WHEN CAST(NULL AS BOOLEAN) THEN CAST(NULL AS BOOLEAN)
            WHEN CAST(NULL AS BOOLEAN) THEN true
            WHEN true THEN CAST(NULL AS BOOLEAN)
        END
        JOIN SUPPLIER AS T2 (R8, R9, R10, R11, R12, R13) ON NOT CAST(NULL AS BOOLEAN)
    WHERE
        CASE
            WHEN true THEN CAST(NULL AS BOOLEAN)
            WHEN CAST(NULL AS BOOLEAN) THEN CAST(NULL AS BOOLEAN)
            WHEN CAST(NULL AS BOOLEAN) THEN CAST(NULL AS BOOLEAN)
            ELSE false
        END
    GROUP BY
        GROUPING SETS ((REGION.R_NAME, R1))
    HAVING
        CASE CAST(NULL AS INTEGER)
            WHEN 2 THEN CAST(NULL AS BOOLEAN)
            ELSE CAST(NULL AS BOOLEAN)
        END
    INTERSECT
    (
        SELECT
            CAST(NULL AS DATE),
            CAST(NULL AS TIMESTAMP),
            INTERVAL '1' DAY AS C5,
            TIMESTAMP '2023-01-30 14:37:05' AS C6
        FROM
            REGION
            RIGHT JOIN (
                SELECT DISTINCT
                    CAST(NULL AS BIGINT)
                FROM
                    NATION AS T4 (R14, R15)
                ORDER BY
                    "int8" NULLS FIRST,
                    "int8" DESC NULLS LAST
                LIMIT
                    -2
            ) AS T3 ON (true)
            RIGHT JOIN CUSTOMER AS T5 ON CASE
                WHEN false THEN true
            END
            RIGHT JOIN (
                (
                    SELECT
                        INTERVAL '1' DAY AS C2
                    FROM
                        SUPPLIER
                    LIMIT
                        CAST(NULL AS BIGINT)
                )
                ORDER BY
                    C2 NULLS LAST
            ) AS T6 ON CASE
                WHEN CAST(NULL AS BOOLEAN) THEN CAST(NULL AS BOOLEAN)
            END,
            (
                (
                    SELECT DISTINCT
                        CAST(NULL AS INTEGER)
                    FROM
                        REGION
                )
            ) AS T7
            RIGHT JOIN NATION ON (CAST(NULL AS BOOLEAN))
            RIGHT JOIN PART AS T8 (R16, R17, R18, R19, R20) ON (
                (
                    SELECT
                        false
                    FROM
                        PARTSUPP AS T9 (R21, R22)
                    LIMIT
                        1
                )
            )
            LEFT JOIN (
                SELECT DISTINCT
                    2 AS C3
                FROM
                    ORDERS AS T11 (R23, R24, R25, R26, R27, R28)
                LIMIT
                    CAST(NULL AS NUMERIC)
            ) AS T10 ON true
        WHERE
            (
                SELECT
                    CAST(NULL AS BOOLEAN)
                FROM
                    PARTSUPP AS T12 (R29, R30, R31, R32)
                ORDER BY
                    "bool"
                LIMIT
                    1
            )
        HAVING
            (
                SELECT DISTINCT
                    false AS C4
                FROM
                    CUSTOMER AS T13 (R33)
                ORDER BY
                    C4 DESC NULLS FIRST
                LIMIT
                    1
            )
        ORDER BY
            "date" NULLS FIRST,
            CAST(NULL AS INTERVAL) ASC
        LIMIT
            3.0972849815518053
    )
);





SELECT DISTINCT
    ((DATE '2023-08-27') + CAST(NULL AS INTEGER)),
    MAX(LINEITEM.L_COMMITDATE),
    CAST(NULL AS INTERVAL) AS C1,
    CAST(NULL AS TIMESTAMP)
FROM
    PART AS T1 (R1, R2, R3, R4, R5, R6, R7)
    JOIN REGION ON (CAST(NULL AS BOOLEAN))
    LEFT JOIN LINEITEM ON CASE
        WHEN CAST(NULL AS BOOLEAN) THEN CAST(NULL AS BOOLEAN)
        WHEN CAST(NULL AS BOOLEAN) THEN true
        WHEN true THEN CAST(NULL AS BOOLEAN)
    END
    JOIN SUPPLIER AS T2 (R8, R9, R10, R11, R12, R13) ON NOT CAST(NULL AS BOOLEAN)
WHERE
    CASE
        WHEN true THEN CAST(NULL AS BOOLEAN)
        WHEN CAST(NULL AS BOOLEAN) THEN CAST(NULL AS BOOLEAN)
        WHEN CAST(NULL AS BOOLEAN) THEN CAST(NULL AS BOOLEAN)
        ELSE false
    END
GROUP BY
    GROUPING SETS ((REGION.R_NAME, R1))
HAVING
    CASE CAST(NULL AS INTEGER)
        WHEN 2 THEN CAST(NULL AS BOOLEAN)
        ELSE CAST(NULL AS BOOLEAN)
    END
INTERSECT
(
    SELECT
        CAST(NULL AS DATE),
        CAST(NULL AS TIMESTAMP),
        INTERVAL '1' DAY AS C5,
        TIMESTAMP '2023-01-30 14:37:05' AS C6
    FROM
        REGION
        RIGHT JOIN (
            SELECT DISTINCT
                CAST(NULL AS BIGINT)
            FROM
                NATION AS T4 (R14, R15)
            ORDER BY
                "int8" NULLS FIRST,
                "int8" DESC NULLS LAST
            LIMIT
                -2
        ) AS T3 ON (true)
        RIGHT JOIN CUSTOMER AS T5 ON CASE
            WHEN false THEN true
        END
        RIGHT JOIN (
            (
                SELECT
                    INTERVAL '1' DAY AS C2
                FROM
                    SUPPLIER
                LIMIT
                    CAST(NULL AS BIGINT)
            )
            ORDER BY
                C2 NULLS LAST
        ) AS T6 ON CASE
            WHEN CAST(NULL AS BOOLEAN) THEN CAST(NULL AS BOOLEAN)
        END,
        (
            (
                SELECT DISTINCT
                    CAST(NULL AS INTEGER)
                FROM
                    REGION
            )
        ) AS T7
        RIGHT JOIN NATION ON (CAST(NULL AS BOOLEAN))
        RIGHT JOIN PART AS T8 (R16, R17, R18, R19, R20) ON (
            (
                SELECT
                    false
                FROM
                    PARTSUPP AS T9 (R21, R22)
                LIMIT
                    1
            )
        )
        LEFT JOIN (
            SELECT DISTINCT
                2 AS C3
            FROM
                ORDERS AS T11 (R23, R24, R25, R26, R27, R28)
            LIMIT
                CAST(NULL AS NUMERIC)
        ) AS T10 ON true
    WHERE
        (
            SELECT
                CAST(NULL AS BOOLEAN)
            FROM
                PARTSUPP AS T12 (R29, R30, R31, R32)
            ORDER BY
                "bool"
            LIMIT
                1
        )
    HAVING
        (
            SELECT DISTINCT
                false AS C4
            FROM
                CUSTOMER AS T13 (R33)
            ORDER BY
                C4 DESC NULLS FIRST
            LIMIT
                1
        )
    ORDER BY
        "date" NULLS FIRST,
        CAST(NULL AS INTERVAL) ASC
    LIMIT
        3.0972849815518053
)