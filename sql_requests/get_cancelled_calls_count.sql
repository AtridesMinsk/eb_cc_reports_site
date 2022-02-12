SELECT count (*) AS Call_count, ag_num

FROM callcent_ag_dropped_calls 

WHERE time_start AT TIME ZONE 'UTC+3' > '2021-08-01' AND reason_noanswerdesc = 'Cancelled'
GROUP BY ag_num
ORDER BY ag_num
