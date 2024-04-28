SELECT
    DISTINCT COUNT(*),
    CASE
        WHEN CAST(NULL AS BOOLEAN) THEN (
            SELECT
                CAST(NULL AS BOOLEAN) AS C1
            FROM
                REGION AS T1 (R1, R2)
            ORDER BY
                CAST(NULL AS TIMESTAMP) ASC NULLS FIRST
            LIMIT
                1
        )
        ELSE false
    END,
    BIT_OR(DISTINCT (CAST(NULL AS INTEGER))) AS C2,
    DATE '2023-08-27',
    (
        -(INTERVAL '1' DAY) + (
            SELECT
                DISTINCT CAST(NULL AS INTERVAL)
            FROM
                REGION
            LIMIT
                1
        )
    )
FROM
    PART,
    REGION,
    ORDERS
WHERE
    false
ORDER BY
    "date" ASC,
    COUNT DESC NULLS FIRST,
    COUNT ASC,
    "case" ASC;