SELECT T1.company_name
FROM Third_Party_Companies AS T1
JOIN Maintenance_Contracts AS T2
ON T1.company_id  =  T2.maintenance_contract_company_id
JOIN Ref_Company_Types AS T3
ON T1.company_type_code = T3.company_type_code
ORDER BY T2.contract_end_date DESC
LIMIT 1

SELECT count(*)
FROM (
    SELECT city FROM airports GROUP BY city HAVING count(*) > 3
)

SELECT document_name
FROM documents
GROUP BY document_type_code
ORDER BY count(*) DESC
LIMIT 3
INTERSECT
SELECT document_name
FROM documents
GROUP BY document_structure_code
ORDER BY count(*) DESC
LIMIT 3

SELECT DISTINCT
    CAST(NULL AS DATE) AS C2,
    CAST(NULL AS TIMESTAMP) AS C3,
    CAST(NULL AS INTERVAL),
    TIMESTAMP '2023-01-30 14:37:05' AS C4
FROM
    PART,
    REGION AS T1 (R1)
        RIGHT JOIN
            REGION AS T2 (R2, R3)
        ON CAST(NULL AS INTEGER) IS NOT NULL,
    (
        SELECT
            DISTINCT false AS C1
        FROM SUPPLIER
        ORDER BY C1 DESC NULLS FIRST, C1 NULLS FIRST, C1 DESC
    ) AS T3
WHERE false