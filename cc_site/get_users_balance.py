import csv
from mysql.connector import connect, Error
from datetime import datetime
from connect_db import core_password, core_host, core_user, core_db, core_port


def get_userid_list():
    sql_request = (f"""
                    SELECT distinct UserID 
                    FROM Core.usertransactions
                    WHERE BeforeBalance IS NOT null 
                    AND BeforeBalance / 100 != 0 
                    AND DateCreated between '2021-01-01' AND '2021-12-31'
                    """
                   )

    try:
        with connect(
                host=core_host,
                user=core_user,
                password=core_password,
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


def get_balance_by_id(user_id):
    sql_request = (f"""
                SELECT UserID, BeforeBalance / 100, DateCreated 
                FROM Core.usertransactions AS ab
                WHERE UserID = {user_id} AND 
                DateCreated = (SELECT MAX(DateCreated) FROM Core.usertransactions AS ab2 
                WHERE BeforeBalance IS NOT null 
                AND DateCreated between '2021-01-01' 
                AND '2021-12-31' AND UserID = {user_id})
                """
                   )
    # print(sql_request)
    try:
        with connect(
                host=core_host,
                user=core_user,
                password=core_password,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_request)
                result = cursor.fetchall()
    except Error as e:
        print(e)

    return result


def main():
    userid = get_userid_list()
    print('Всего найдено пользователей:', len(userid), '\n' 'Последний пользователь:', userid[-1])

    for i in userid:
        data = get_balance_by_id(i)
        user_id = data[0][0]
        user_balance = float(data[0][1])
        last_date = data[0][2]
        print(user_id, user_balance, last_date)

        if user_balance != 0:
            with open("balance.csv", "a", newline='') as file:
                writer = csv.writer(file)

                writer.writerow(
                    (
                        user_id,
                        user_balance,
                        last_date,
                    )
                )


if __name__ == '__main__':
    main()
