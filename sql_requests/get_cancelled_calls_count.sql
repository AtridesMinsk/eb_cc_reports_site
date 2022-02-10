SELECT count (*) AS Call_count

FROM callcent_ag_dropped_calls 

WHERE time_start AT TIME ZONE 'UTC+3' > '2022-02-02' AND time_end AT TIME ZONE 'UTC+3' < '2022-02-03' AND reason_noanswerdesc = 'Cancelled'