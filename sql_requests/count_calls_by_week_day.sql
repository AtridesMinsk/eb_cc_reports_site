                WITH 
                Canceled_calls AS (
                    SELECT count (*) AS Call_count, to_char(time_start, 'ID') AS Day_of_the_week
                    FROM callcent_ag_dropped_calls 
                    WHERE time_start AT TIME ZONE 'UTC' > '2021-08-01' 
                    AND reason_noanswerdesc != 'Answered' AND reason_noanswerdesc = 'Poll expired'
                    AND ag_num != '1000' AND ag_num != '1001' AND ag_num != '9999'
                    GROUP BY to_char(time_start, 'ID')
                    ),
                Call_in_count AS (
                    SELECT count (*) AS Calls_count_in, to_char(time_start, 'ID') AS Day_of_the_week
                    FROM callcent_queuecalls 
                    WHERE ts_servicing != '00:00:00' AND time_start AT TIME ZONE 'UTC+3' > '2021-08-01' 
                    AND to_dn != '1000' AND to_dn != '1001' AND to_dn != '9999'
                    GROUP BY to_char(time_start, 'ID')
                   ),
                Call_out_count AS (SELECT count (*) AS Calls_count_out, 
                to_char(s.start_time, 'ID') AS Day_of_the_week
                    FROM ((((((cl_segments s
                    JOIN cl_participants sp ON ((sp.id = s.src_part_id)))
                    JOIN cl_participants dp ON ((dp.id = s.dst_part_id)))
                    JOIN cl_party_info si ON ((si.id = sp.info_id)))
                    JOIN cl_party_info di ON ((di.id = dp.info_id)))
                    LEFT JOIN cl_participants ap ON ((ap.id = s.action_party_id)))
                    LEFT JOIN cl_party_info ai ON ((ai.id = ap.info_id)))
                    WHERE s.start_time AT TIME ZONE 'UTC-3' > '2021-08-01' 
                    AND s.action_id = 1 AND si.dn_type = 0 AND seq_order = 1 
                    AND si.dn != '1000' AND si.dn != '1001' AND di.dn_type = 13
                    GROUP BY to_char(s.start_time, 'ID'))
                SELECT Call_out_count.Day_of_the_week, Call_in_count.Calls_count_in, 
                Call_out_count.Calls_count_out, Canceled_calls.Call_count
                FROM Call_in_count
                INNER JOIN Call_out_count ON Call_in_count.Day_of_the_week = Call_out_count.Day_of_the_week
                INNER JOIN Canceled_calls ON  Call_in_count.Day_of_the_week = Canceled_calls.Day_of_the_week
                ORDER BY Call_in_count.Day_of_the_week ASC