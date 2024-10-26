SELECT
    CASE
        WHEN NOT false THEN C_NATIONKEY
    END AS C2,
    T1.*
FROM
    (
        SELECT DISTINCT
            CAST(NULL AS TEXT)
        FROM
            (
                (
                    SELECT DISTINCT
                        CAST(NULL AS TEXT) AS C1
                    FROM
                        CUSTOMER
                    ORDER BY
                        C1 ASC NULLS LAST
                )
            ) AS T2
        GROUP BY
            true
        ORDER BY
            "text",
            "text" ASC NULLS FIRST,
            "text" DESC
    ) AS T1,
    CUSTOMER AS T3 (R1)
WHERE
    true
INTERSECT
(
    (
        SELECT
            4.993609530061892,
            CAST(NULL AS TEXT) AS C3
        FROM
            NATION,
            PARTSUPP
            JOIN PART ON CASE
                WHEN false THEN true
            END,
            SUPPLIER
        WHERE
            false
        ORDER BY
            C3 DESC NULLS LAST,
            (
                CASE CAST(NULL AS TEXT)
                    WHEN CAST(NULL AS TEXT) THEN TIMESTAMP '2023-01-30 14:37:05'
                    WHEN 'HJeihfbwei' THEN CAST(NULL AS DATE)
                END
            )
    )
    LIMIT
        (- CAST(NULL AS INTEGER))
)
LIMIT
    (-1)