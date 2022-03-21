import csv
import datetime
from datetime import datetime, timedelta

from mysql.connector import connect, Error
from connect_db import core_password, core_host, core_user, core_db, core_port


def get_userid_list(end_date):
    sql_request = (f"""
                    SELECT distinct UserID 
                    FROM Core.usertransactions
                    WHERE BeforeBalance IS NOT null 
                    AND BeforeBalance / 100 != 0 
                    AND DateCreated between '2021-01-01' AND '{end_date}'
                    """
                   )

    try:
        with connect(
                host=core_host,
                user=core_user,
                password=core_password,
                db=core_db,
                port=core_port,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_request)
                result = cursor.fetchall()
    except Error as e:
        print(e)

    userid_list = []

    for i in range(0, len(result)):
        userid = str(result[i][0])
        userid_list.append(userid)

    return userid_list


def get_balance_by_id(user_id, end_date):
    sql_request = (f"""
                SELECT UserID, BeforeBalance / 100, DateCreated 
                FROM Core.usertransactions AS ab
                WHERE UserID = {user_id} AND 
                DateCreated = (SELECT MAX(DateCreated) FROM Core.usertransactions AS ab2 
                WHERE BeforeBalance IS NOT null 
                AND DateCreated between '2021-01-01' 
                AND '{end_date}' AND UserID = {user_id})
                """
                   )
    # print(sql_request)
    try:
        with connect(
                host=core_host,
                user=core_user,
                password=core_password,
                db=core_db,
                port=core_port,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_request)
                result = cursor.fetchall()
    except Error as e:
        print(e)

    return result


def create_csv_file(date):
    with open(f"balance_on_{date}.csv", "w", newline='') as file:
        csv.writer(file)


def get_data(last_month_date):
    date = datetime.strptime(last_month_date, '%Y-%m-%d')
    time_start = datetime.now()
    create_csv_file(last_month_date)
    userid = get_userid_list(last_month_date)

    for i in userid:
        data = get_balance_by_id(i, last_month_date)
        user_id = data[0][0]
        user_balance = float(data[0][1])
        last_date = data[0][2]
        # print(user_id, user_balance, last_date)

        if user_balance != 0:
            with open(f"balance_on_{last_month_date}.csv", "a", newline='') as file:
                writer = csv.writer(file)

                writer.writerow(
                    (
                        user_id,
                        user_balance,
                        last_date,
                    )
                )
    print('Всего найдено записей на конец', date.strftime("%B"), ':',
          len(userid), '\n' 'Последний пользователь:', userid[-1])
    with open(f"balance_on_{last_month_date}.csv", "r") as file:
        data = list(file)
        print('Всего записей с не нулевым балансом:', len(data))
        print('Отфильтровано записей с нулевым балансом:', len(userid) - len(data))

    time_stop = datetime.now()
    work_time: timedelta = time_stop - time_start
    print('Затрачено времени на обработку:', work_time, '\n')


def main():
    last_month_date = ('2021-08-31', '2021-09-30', '2021-10-31', '2021-11-30', '2021-12-31', '2022-01-31', '2022-02-28')
    for date in last_month_date:
        get_data(date)


if __name__ == '__main__':
    main()
