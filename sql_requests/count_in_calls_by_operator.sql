SELECT to_dn AS Operator_ID, count (*) AS Calls_by_Operator

FROM callcent_queuecalls 
WHERE ts_servicing != '00:00:00' AND time_start AT TIME ZONE 'UTC+3' > '2021-08-01' AND to_dn != '1000' AND to_dn != '1001' AND to_dn != '9999'

GROUP BY to_dn
ORDER BY to_dn ASC