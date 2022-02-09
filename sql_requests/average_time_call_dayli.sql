SELECT count (*) AS Call_count, 
DATE_TRUNC('second', AVG (ts_servicing + interval '500 millisecond')) AS Call_time, 
DATE_TRUNC('second', AVG (ts_waiting + interval '500 millisecond')) AS IVR_time,
DATE_TRUNC('second', AVG (ts_polling + interval '500 millisecond')) AS Ringing_time
FROM public.callcent_queuecalls 

WHERE time_start AT TIME ZONE 'UTC+3' > '2022-02-02' AND time_start AT TIME ZONE 'UTC+3' < '2022-02-03' AND ts_servicing != '00:00:00'