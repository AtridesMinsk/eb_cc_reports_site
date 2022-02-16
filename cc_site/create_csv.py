import csv
import json
import time
import datetime
import psycopg2
import schedule
from connect_db import prod_password as password, prod_host as host, user, database, port
from psycopg2 import Error
from datetime import timedelta, date, datetime


def create_csv_file():
    with open("cc_data.csv", "w", newline='') as file:
        csv.writer(file)


def get_data_cancelled_call(start_date, end_date):
    sql_request = (f"""
            SELECT 
                count (*) AS Call_count
            FROM callcent_ag_dropped_calls
            WHERE time_start AT TIME ZONE 'UTC' > '{start_date}'
                AND time_end AT TIME ZONE 'UTC' < '{end_date}'
                AND reason_noanswerdesc != 'Answered' AND reason_noanswerdesc != 'Poll expired'
            """)

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


def get_data_outgoing_call(start_date, end_date):
    sql_request = (f"""
            SELECT 
                count (*) AS Call_count
            FROM ((((((cl_segments s
                JOIN cl_participants sp ON ((sp.id = s.src_part_id)))
                JOIN cl_participants dp ON ((dp.id = s.dst_part_id)))
                JOIN cl_party_info si ON ((si.id = sp.info_id)))
                JOIN cl_party_info di ON ((di.id = dp.info_id)))
                LEFT JOIN cl_participants ap ON ((ap.id = s.action_party_id)))
                LEFT JOIN cl_party_info ai ON ((ai.id = ap.info_id)))
            WHERE s.start_time AT TIME ZONE 'UTC-3' > '{start_date}'
                AND s.end_time AT TIME ZONE 'UTC-3' < '{end_date}' 
                AND s.action_id = 1 AND si.dn_type = 0 AND seq_order = 1 AND di.dn_type = 13
            """)

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


def get_data_average_call(start_date, end_date):
    sql_request = (f"""
            SELECT 
                count (*) AS Call_count, 
                DATE_TRUNC('second', AVG (ts_servicing + interval '500 millisecond')), 
                DATE_TRUNC('second', AVG (ts_waiting + interval '500 millisecond')),
                DATE_TRUNC('second', AVG (ts_polling + interval '500 millisecond')) 
            FROM public.callcent_queuecalls 
            WHERE time_start AT TIME ZONE 'UTC+3' > '{start_date}' 
                AND time_start AT TIME ZONE 'UTC+3' < '{end_date}' 
                AND ts_servicing != '00:00:00'
            """)

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


def cor_data_average_call(days_swap):
    for i in range(0, days_swap):
        today_date = date.today() - timedelta(days=i)
        yesterday_date = date.today() - timedelta(days=i + 1)

        date_start = time.strptime(f'{yesterday_date}', '%Y-%m-%d')
        date_start_text = time.strftime('%Y-%m-%d', date_start)

        date_end = time.strptime(f'{today_date}', '%Y-%m-%d')
        date_end_text = time.strftime('%Y-%m-%d', date_end)

        results_in = get_data_average_call(date_start_text, date_end_text)
        results_out = get_data_outgoing_call(date_start_text, date_end_text)
        results_can = get_data_cancelled_call(date_start_text, date_end_text)

        incoming_call_count = results_in[0]
        average_call_time = results_in[1]
        average_time_ivr = results_in[2]
        average_ringing_time = results_in[3]
        outgoing_call_count = results_out[0]
        cancelled_call_count = results_can[0]

        with open("cc_data.csv", "a", newline='') as file:
            writer = csv.writer(file)

            writer.writerow(
                (
                    date_start_text,
                    incoming_call_count,
                    average_call_time,
                    average_time_ivr,
                    average_ringing_time,
                    outgoing_call_count,
                    cancelled_call_count
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


def csv_to_json(days_swap):
    data_in = []
    data_out = []
    data_can = []
    for i in range(0, days_swap):
        today_date = date.today() - timedelta(days=i)
        yesterday_date = date.today() - timedelta(days=i + 1)

        date_start = time.strptime(f'{yesterday_date}', '%Y-%m-%d')
        date_start_text = time.strftime('%Y-%m-%d', date_start)

        date_end = time.strptime(f'{today_date}', '%Y-%m-%d')
        date_end_text = time.strftime('%Y-%m-%d', date_end)

        results_in = get_data_average_call(date_start_text, date_end_text)
        results_out = get_data_outgoing_call(date_start_text, date_end_text)
        results_can = get_data_cancelled_call(date_start_text, date_end_text)

        incoming_call_count = results_in[0]
        outgoing_call_count = results_out[0]
        cancelled_call_count = results_can[0]
        dt = time.mktime(date_start)
        dt = int(dt)
        dt = str(dt)
        dt = dt + "000"
        dt = int(dt)
        data_in.insert(0, [dt, incoming_call_count])
        data_out.insert(0, [dt, outgoing_call_count])
        data_can.insert(0, [dt, cancelled_call_count])
    with open(f"static/incoming_c.json", "w") as file:
        json.dump(data_in, file, indent=4, ensure_ascii=False)
    with open(f"static/outgoing_c.json", "w") as file:
        json.dump(data_out, file, indent=4, ensure_ascii=False)
    with open(f"static/canceled_c.json", "w") as file:
        json.dump(data_can, file, indent=4, ensure_ascii=False)


def get_data():
    create_csv_file()
    days_swap = calculate_days_count()
    cor_data_average_call(days_swap)
    csv_to_json(days_swap)
    print(datetime.now())


def main():
    get_data()
    schedule.every().day.at('00:10').do(get_data)

    while True:
        schedule.run_pending()


if __name__ == '__main__':
    main()
