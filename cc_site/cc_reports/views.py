import csv

import psycopg2
from django.shortcuts import render, redirect
from .models import Tasks
from .forms import TaskForm
from connect_db import prod_password as password, prod_host as host, user, database, port


# Create your views here.
def index(request):
    tasks = Tasks.objects.all()
    return render(request, 'cc_reports/index.html', {'title': 'Главная страница сайта', 'tasks': tasks})


def about(request):
    return render(request, 'cc_reports/about.html')


def average_call_rep(request):
    svc_data = []
    with open('cc_data.csv', 'rU') as csv_file:
        reader = csv.reader(csv_file, dialect='excel')
        for row in reader:
            svc_data.append(row)
    return render(request, 'cc_reports/calls_rep.html', {'title': 'Статистика', 'reader': svc_data})


def get_data_from_database(call_id):
    sql_request = (f'SELECT callcent_ag_dropped_calls.ag_num, callcent_ag_dropped_calls.time_start, '
                   f'callcent_ag_dropped_calls.time_end, DATE_TRUNC(\'second\', callcent_ag_dropped_calls.ts_polling '
                   f'+ interval \'500 millisecond\'), '
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

        drop_calls_rep = cursor_call_count.fetchone()
        drop_calls_rep2 = cursor_call_count.fetchone()

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)

    finally:
        if connection:
            cursor_call_count.close()
            connection.close()

    return drop_calls_rep, drop_calls_rep2


def correct_data(drop_calls_rep):
    results = drop_calls_rep

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

    user_id = results[0][0]
    ringing_start = results[0][1]
    ringing_stop = results[0][2]
    ringing_duration = results[0][3]
    call_result = results[0][4]
    call_id = results[0][5]
    subscriber_number = results[0][6]

    with open("cc_drop_call.csv", "a", newline='') as file:
        writer = csv.writer(file)

        writer.writerow(
            (
                user_id,
                ringing_start,
                ringing_stop,
                ringing_duration,
                call_result,
                call_id,
                subscriber_number
            )
        )

    user_id = results[1][0]
    ringing_start = results[1][1]
    ringing_stop = results[1][2]
    ringing_duration = results[1][3]
    call_result = results[1][4]
    call_id = results[1][5]
    subscriber_number = results[1][6]

    with open("cc_drop_call.csv", "a", newline='') as file:
        writer = csv.writer(file)

        writer.writerow(
            (
                user_id,
                ringing_start,
                ringing_stop,
                ringing_duration,
                call_result,
                call_id,
                subscriber_number
            )
        )


def call_drop(request):
    call_history_id = "0000017EBB056D7E_390"
    data = get_data_from_database(call_history_id)
    correct_data(data)
    svc_data = []
    with open('cc_drop_call.csv', 'rU') as csv_file:
        reader = csv.reader(csv_file, dialect='excel')
        for row in reader:
            svc_data.append(row)
    return render(request, 'cc_reports/calls_drop.html', {'reader': svc_data})


def create(request):
    error = ''
    if request.method == 'POST':
        form = TaskForm(request.POST)
        if form.is_valid():
            form.save()
            return redirect('home')
        else:
            error = 'Неверная форма'

    form = TaskForm()
    context = {
        'form': form,
        'error': error
    }
    return render(request, 'cc_reports/create.html', context)
