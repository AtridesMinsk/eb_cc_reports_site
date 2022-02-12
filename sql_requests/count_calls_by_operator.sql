WITH 
Canceled_calls AS (
	SELECT count (*) AS Call_count, ag_num
	FROM callcent_ag_dropped_calls 
	WHERE time_start AT TIME ZONE 'UTC+3' > '2021-08-01' AND reason_noanswerdesc = 'Cancelled' AND ag_num != '1000' AND ag_num != '1001' AND ag_num != '9999'
	GROUP BY ag_num
	ORDER BY ag_num
	),
Call_in AS (
    SELECT to_dn AS Operator_ID_in, count (*) AS Calls_by_Operator_in
	FROM callcent_queuecalls 
	WHERE ts_servicing != '00:00:00' AND time_start AT TIME ZONE 'UTC+3' > '2021-08-01' AND to_dn != '1000' AND to_dn != '1001' AND to_dn != '9999'
	GROUP BY to_dn
	ORDER BY to_dn ASC
   ),
Call_out AS (SELECT count (*) AS Calls_by_Operator_out, si.dn AS Operator_ID_out
	FROM ((((((cl_segments s
	JOIN cl_participants sp ON ((sp.id = s.src_part_id)))
	JOIN cl_participants dp ON ((dp.id = s.dst_part_id)))
	JOIN cl_party_info si ON ((si.id = sp.info_id)))
	JOIN cl_party_info di ON ((di.id = dp.info_id)))
	LEFT JOIN cl_participants ap ON ((ap.id = s.action_party_id)))
	LEFT JOIN cl_party_info ai ON ((ai.id = ap.info_id)))
	WHERE s.start_time AT TIME ZONE 'UTC-3' > '2021-08-01' 
	AND s.action_id = 1 AND si.dn_type = 0 AND seq_order = 1 AND si.dn != '1000' AND si.dn != '1001'
	GROUP BY si.dn
	ORDER BY si.dn ASC)
SELECT Call_in.Operator_ID_in, Call_in.Calls_by_Operator_in, Call_out.Calls_by_Operator_out, Canceled_calls.Call_count
FROM Call_in
INNER JOIN Call_out ON Call_in.Operator_ID_in = Call_out.Operator_ID_out
INNER JOIN Canceled_calls ON Call_in.Operator_ID_in = Canceled_calls.ag_num