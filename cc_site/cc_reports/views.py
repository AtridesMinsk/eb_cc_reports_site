import csv

import psycopg2
from django.shortcuts import render
from django.core.paginator import Paginator
from connect_db import prod_password as password, prod_host as host, user, database, port


def about(request):
    return render(request, 'cc_reports/about.html', {'title': 'О нас'})


def average_call_rep(request):
    csv_data = []
    with open('cc_data.csv', 'rU') as csv_file:
        reader = csv.reader(csv_file, dialect='excel')
        for row in reader:
            csv_data.append(row)
    paginator = Paginator(csv_data, 8)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    return render(request, 'cc_reports/calls_rep.html',
                  {'title': 'Звонки по дням', 'reader': page_obj.object_list, 'page_obj': page_obj})


def get_data_drop_call(call_id):
    sql_request = (f'SELECT callcent_ag_dropped_calls.ag_num, '
                   f'callcent_ag_dropped_calls.time_start AT TIME ZONE \'UTC+3\', '
                   f'callcent_ag_dropped_calls.time_end AT TIME ZONE \'UTC+3\', '
                   f'DATE_TRUNC(\'second\', callcent_ag_dropped_calls.ts_polling + interval \'500 millisecond\'), '
                   f'callcent_ag_dropped_calls.reason_noanswerdesc, callcent_ag_dropped_calls.q_call_history_id, '
                   f'callcent_queuecalls.from_userpart FROM callcent_ag_dropped_calls LEFT JOIN callcent_queuecalls '
                   f'ON callcent_queuecalls.call_history_id = q_call_history_id WHERE q_call_history_id = '
                   f'\'{call_id}\' ORDER BY idcallcent_ag_dropped_calls ASC')

    try:
        connection = psycopg2.connect(database=database,
                                      user=user,
                                      password=password,
                                      host=host,
                                      port=port
                                      )

        cursor_call_count = connection.cursor()
        cursor_call_count.execute(str(sql_request))

        drop_calls_rep = cursor_call_count.fetchall()

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)

    finally:
        if connection:
            cursor_call_count.close()
            connection.close()

    return drop_calls_rep


def drop_call(request):
    call_id = request.GET.get('object')
    drop_call_data = get_data_drop_call(call_id)
    return render(request, 'cc_reports/calls_drop.html',
                  {'title': 'Детализация потеряного звонка', 'reader': drop_call_data})


def get_data_all_drop_call():
    sql_request = (
        f'SELECT callcent_ag_dropped_calls.ag_num, callcent_ag_dropped_calls.time_start AT TIME ZONE \'UTC+3\', '
        f'callcent_ag_dropped_calls.time_end AT TIME ZONE \'UTC+3\', '
        f'DATE_TRUNC(\'second\', callcent_ag_dropped_calls.ts_polling + interval '
        f'\'500 millisecond\'), callcent_ag_dropped_calls.reason_noanswerdesc, '
        f'callcent_ag_dropped_calls.q_call_history_id FROM callcent_ag_dropped_calls WHERE reason_noanswerdesc = '
        f'\'Poll expired\' AND time_start AT TIME ZONE \'UTC+3\' > \'2021-08-01\' ORDER BY '
        f'idcallcent_ag_dropped_calls DESC '
    )

    try:
        connection = psycopg2.connect(database=database,
                                      user=user,
                                      password=password,
                                      host=host,
                                      port=port
                                      )

        cursor_call_count = connection.cursor()
        cursor_call_count.execute(str(sql_request))

        drop_calls_rep = cursor_call_count.fetchall()

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)

    finally:
        if connection:
            cursor_call_count.close()
            connection.close()

    return drop_calls_rep


def all_drop_call(request):
    dropped_calls = get_data_all_drop_call()
    paginator = Paginator(dropped_calls, 8)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    return render(request, 'cc_reports/all_calls_drop.html',
                  {'title': 'Все потерянные звонки', 'reader': page_obj.object_list, 'page_obj': page_obj})


def get_data_calls_by_operator():
    sql_request = (
        f'WITH '
        f'Canceled_calls AS ( '
        f'SELECT count (*) AS Call_count, ag_num '
        f'FROM callcent_ag_dropped_calls '
        f'WHERE time_start AT TIME ZONE \'UTC+3\' > \'2021-08-01\' AND reason_noanswerdesc = \'Cancelled\' AND ag_num '
        f'!= \'1000\' AND ag_num != \'1001\' AND ag_num != \'9999\' '
        f'GROUP BY ag_num '
        f'ORDER BY ag_num '
        f'), '
        f'Call_in AS ( '
        f'SELECT to_dn AS Operator_ID_in, count (*) AS Calls_by_Operator_in '
        f'FROM callcent_queuecalls '
        f'WHERE ts_servicing != \'00:00:00\' AND time_start AT TIME ZONE \'UTC+3\' > \'2021-08-01\' AND to_dn != '
        f'\'1000\' AND to_dn != \'1001\' AND to_dn != \'9999\' '
        f'GROUP BY to_dn '
        f'ORDER BY to_dn ASC '
        f'), '
        f'Call_out AS (SELECT count (*) AS Calls_by_Operator_out, si.dn AS Operator_ID_out '
        f'FROM ((((((cl_segments s '
        f'JOIN cl_participants sp ON ((sp.id = s.src_part_id))) '
        f'JOIN cl_participants dp ON ((dp.id = s.dst_part_id))) '
        f'JOIN cl_party_info si ON ((si.id = sp.info_id))) '
        f'JOIN cl_party_info di ON ((di.id = dp.info_id))) '
        f'LEFT JOIN cl_participants ap ON ((ap.id = s.action_party_id))) '
        f'LEFT JOIN cl_party_info ai ON ((ai.id = ap.info_id))) '
        f'WHERE s.start_time AT TIME ZONE \'UTC-3\' > \'2021-08-01\' '
        f'AND s.action_id = 1 AND si.dn_type = 0 AND di.dn_type = 13 AND seq_order = 1 '
        f'AND si.dn != \'1000\' AND si.dn != \'1001\' '
        f'GROUP BY si.dn '
        f'ORDER BY si.dn ASC) '
        f'SELECT Call_in.Operator_ID_in, Call_in.Calls_by_Operator_in, Call_out.Calls_by_Operator_out, '
        f'Canceled_calls.Call_count '
        f'FROM Call_in '
        f'INNER JOIN Call_out ON Call_in.Operator_ID_in = Call_out.Operator_ID_out '
        f'INNER JOIN Canceled_calls ON Call_in.Operator_ID_in = Canceled_calls.ag_num '
    )

    try:
        connection = psycopg2.connect(database=database,
                                      user=user,
                                      password=password,
                                      host=host,
                                      port=port
                                      )

        cursor_call_count = connection.cursor()
        cursor_call_count.execute(str(sql_request))

        in_calls = cursor_call_count.fetchall()

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)

    finally:
        if connection:
            cursor_call_count.close()
            connection.close()

    return in_calls


def index(request):
    data_calls_by_operator = get_data_calls_by_operator()
    call_by_operator = render(request, 'cc_reports/index.html', {'title': 'Звонки по операторам',
                                                                 'reader': data_calls_by_operator})
    return call_by_operator
