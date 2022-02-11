SELECT count (*) AS Call_count, 
DATE_TRUNC('day', time_start) AS Date_call,
DATE_TRUNC('second', AVG (ts_servicing + interval '500 millisecond')) AS Call_time, 
DATE_TRUNC('second', AVG (ts_waiting + interval '500 millisecond')) AS IVR_time,
DATE_TRUNC('second', AVG (ts_polling + interval '500 millisecond')) AS Ringing_time
FROM callcent_queuecalls 
WHERE ts_servicing != '00:00:00' AND time_start AT TIME ZONE 'UTC+3' > '2021-08-01'
GROUP BY DATE_TRUNC('day', time_start)
ORDER BY DATE_TRUNC('day', time_start) DESC