SELECT 
callcent_ag_dropped_calls.ag_num, 
callcent_ag_dropped_calls.time_start, 
callcent_ag_dropped_calls.time_end, DATE_TRUNC('second', callcent_ag_dropped_calls.ts_polling + interval '500 millisecond'), 
callcent_ag_dropped_calls.reason_noanswerdesc,
callcent_ag_dropped_calls.q_call_history_id, 
callcent_queuecalls.from_userpart

FROM callcent_ag_dropped_calls
LEFT JOIN callcent_queuecalls ON callcent_queuecalls.call_history_id = q_call_history_id

WHERE q_call_history_id = '0000017EDAE4E87B_120'
ORDER BY idcallcent_ag_dropped_calls ASC