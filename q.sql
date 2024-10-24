(
    (
        (
            SELECT DISTINCT
                CAST(NULL AS DATE) AS C1
            FROM
                PART
            ORDER BY
                C1 DESC NULLS LAST
            LIMIT
                CAST(NULL AS INTEGER)
        )
    )
)