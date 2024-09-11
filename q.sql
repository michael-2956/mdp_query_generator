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

