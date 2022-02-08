import csv
import time
import psycopg2
from connect_db import prod_password as password, prod_host as host, user, database, port

from psycopg2 import Error
from datetime import timedelta, date


def database_connect(start_date, end_date):
    sql_request = (f'SELECT count (*) AS Call_count, DATE_TRUNC(\'second\', AVG (ts_servicing + interval \'500 '
                   f'millisecond\')), DATE_TRUNC(\'second\', AVG (ts_waiting + interval \'500 millisecond\')), '
                   f'DATE_TRUNC(\'second\', AVG (ts_polling + interval \'500 millisecond\')) FROM '
                   f'public.callcent_queuecalls WHERE time_start AT TIME ZONE \'UTC+3\' > \'{start_date}\' AND '
                   f'time_start AT TIME ZONE \'UTC+3\' < \'{end_date}\' AND ts_servicing != \'00:00:00\'')

    try:
        connection = psycopg2.connect(database=database,
                                      user=user,
                                      password=password,
                                      host=host,
                                      port=port
                                      )

        cursor_call_count = connection.cursor()
        cursor_call_count.execute(str(sql_request))

        call_count = cursor_call_count.fetchone()

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)

    finally:
        if connection:
            cursor_call_count.close()
            connection.close()

    return call_count


def create_csv_file():
    with open("cc_data.csv", "w", newline='') as file:
        writer = csv.writer(file)

        writer.writerow(
            ("Дата",
             "Кол-во вх.звонков (Answered)",
             "Ср.время разговора (Av. talking time)",
             "Среднее время IVR",
             "Ср.время ожидания у оператора (Av.ringing time)")
        )


def get_data_from_database(days_swap):
    for i in range(0, days_swap):
        today_date = date.today() - timedelta(days=i)
        yesterday_date = date.today() - timedelta(days=i + 1)

        date_start = time.strptime(f'{yesterday_date}', '%Y-%m-%d')
        date_start_text = time.strftime('%Y-%m-%d', date_start)

        date_end = time.strptime(f'{today_date}', '%Y-%m-%d')
        date_end_text = time.strftime('%Y-%m-%d', date_end)

        data_to_csv = []

        results = database_connect(date_start_text, date_end_text)
        print("\n"  "Дата:", date_start_text,
              "\n" "Кол-во вх.звонков (Answered):", results[0],
              "\n" "Ср.время разговора (Av. talking time):", results[1],
              "\n" "Среднее время IVR:", results[2],
              "\n" "Ср.время ожидания у оператора (Av.ringing time):", results[3], "\n"
              )

        call_count = results[0]
        average_call_time = results[1]
        average_time_ivr = results[2]
        average_ringing_time = results[3]

        data_to_csv.append(
            {"Дата:": date_start_text,
             "Кол-во вх.звонков (Answered):": call_count,
             "Ср.время разговора (Av. talking time):": average_call_time,
             "Среднее время IVR:": average_time_ivr,
             "Ср.время ожидания у оператора (Av.ringing time):": average_ringing_time
             }
        )

        with open("cc_data.csv", "a", newline='') as file:
            writer = csv.writer(file)

            writer.writerow(
                (
                    date_start_text,
                    call_count,
                    average_call_time,
                    average_time_ivr,
                    average_ringing_time
                )
            )


def calculate_days_count():
    date_end = '2021-08-01'
    date_end = date_end.split('-')
    end_date = date(int(date_end[0]), int(date_end[1]), int(date_end[2]))
    days_swap = date.today() - end_date
    days_swap = str(days_swap)
    days_swap = days_swap.split()[0]
    days_swap = int(days_swap)
    return days_swap


def main():
    create_csv_file()
    days_swap = calculate_days_count()
    get_data_from_database(days_swap)


if __name__ == '__main__':
    main()
