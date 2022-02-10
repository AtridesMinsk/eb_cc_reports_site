import csv

import psycopg2
from django.shortcuts import render, redirect
from django.core.paginator import Paginator
from connect_db import prod_password as password, prod_host as host, user, database, port


def index(request):
    return render(request, 'cc_reports/index.html')


def about(request):
    return render(request, 'cc_reports/about.html', {'title': 'О нас'})


def average_call_rep(request):
    csv_data = []
    with open('cc_data.csv', 'rU') as csv_file:
        reader = csv.reader(csv_file, dialect='excel')
        for row in reader:
            csv_data.append(row)
    objects = csv_data
    p = Paginator(objects, 20)
    page1 = p.page(1)
    page2 = p.page(2)
    return render(request, 'cc_reports/calls_rep.html', {'title': 'Звонки по дням', 'reader': page1.object_list})


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


def cor_data_drop_call(drop_calls_rep):
    results = drop_calls_rep
    row_count = len(results)

    with open("cc_drop_call.csv", "w", newline='') as file:
        writer = csv.writer(file)

        writer.writerow(
            ("Оператор ID:",
             "Начало вызова:",
             "Конец вызова:",
             "Длительность вызова:",
             "Статус вызова:",
             "Идентификатор вызова:",
             "Номер абонента:"
             )
        )
    for row in range(0, row_count):
        user_id = results[row][0]
        ringing_start = results[row][1]
        ringing_stop = results[row][2]
        ringing_duration = results[row][3]
        call_result = results[row][4]
        call_id = results[row][5]
        subscriber_number = results[row][6]

        with open("cc_drop_call.csv", "a", newline='') as file:
            writer = csv.writer(file)

            writer.writerow(
                (
                    user_id,
                    ringing_start.strftime('%m.%d.%Y %H:%M'),
                    ringing_stop.strftime('%m.%d.%Y %H:%M'),
                    ringing_duration,
                    call_result,
                    call_id,
                    subscriber_number
                )
            )


def drop_call(request):
    call_id = request.GET.get('object')
    drop_call_data = get_data_drop_call(call_id)
    print("Найдено записей в базе:", len(drop_call_data))
    cor_data_drop_call(drop_call_data)
    svc_data = []
    with open('cc_drop_call.csv', 'rU') as csv_file:
        reader = csv.reader(csv_file, dialect='excel')
        for row in reader:
            svc_data.append(row)
    return render(request, 'cc_reports/calls_drop.html', {'title': 'Детализация потеряного звонка', 'reader': svc_data})


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


def cor_data_all_drop_call(date_from_db):
    results = date_from_db
    row_count = len(results)

    with open("cc_all_drop_call.csv", "w", newline='') as file:
        writer = csv.writer(file)

        writer.writerow(
            ("Оператор ID:",
             "Начало вызова:",
             "Конец вызова:",
             "Продолжительность вызова:",
             "Статус вызова:",
             "Идентификатор вызова:",
             )
        )

    for row in range(0, row_count):
        user_id = results[row][0]
        ringing_start = results[row][1]
        ringing_stop = results[row][2]
        ringing_duration = results[row][3]
        call_result = results[row][4]
        call_id = results[row][5]

        with open("cc_all_drop_call.csv", "a", newline='') as file:
            writer = csv.writer(file)

            writer.writerow(
                (
                    user_id,
                    ringing_start.strftime('%m.%d.%Y %H:%M'),
                    ringing_stop.strftime('%m.%d.%Y %H:%M'),
                    ringing_duration,
                    call_result,
                    call_id,
                )
            )


def all_drop_call(request):
    dropped_calls = get_data_all_drop_call()
    print("Найдено записей в базе:", len(dropped_calls))
    cor_data_all_drop_call(dropped_calls)
    svc_data = []
    with open('cc_all_drop_call.csv', 'rU') as csv_file:
        reader = csv.reader(csv_file, dialect='excel')
        for row in reader:
            svc_data.append(row)
    return render(request, 'cc_reports/all_calls_drop.html', {'title': 'Все потерянные звонки','reader': svc_data})
