SELECT si.dn AS Operator_ID, count (*) AS Calls_by_Operator 
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
	 ORDER BY si.dn ASC