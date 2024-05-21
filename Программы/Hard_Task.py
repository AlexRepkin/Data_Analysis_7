#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import typing as t
import psycopg2


'''Аналогично индивидуальному заданию, данные о людях хранятся в БД.
Однако здесь используется не SQLite3, а сервер PostrgeSQL.
В данном файле имеется две таблицы – people и surnames'''


def create_db():
    """Создать базу данных."""
    # Подключение к серверу PostgreSQL
    with psycopg2.connect(
        dbname="people",
        user="postgres",
        password="password",
        host="127.0.0.1",
        port=5432,
        options="-c client_encoding=UTF8"  # Попытка решить проблему UTF-8
    ) as conn:
        with conn.cursor() as cursor:
            # Создание таблицы surnames, если она не существует.
            cursor.execute(
                """
                    CREATE TABLE IF NOT EXISTS surnames (
                        surname_id SERIAL PRIMARY KEY,
                        surname TEXT NOT NULL
                    )
                    """
            )
            # Создание таблицы people, если она не существует.
            cursor.execute(
                """
                    CREATE TABLE IF NOT EXISTS people (
                        human_id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL,
                        surname_id INTEGER NOT NULL,
                        telephone TEXT NOT NULL,
                        birthday DATE NOT NULL,
                        FOREIGN KEY(surname_id) REFERENCES surnames(surname_id)
                    )
                    """
            )
            conn.commit()


def display_people(people):
    """Отобразить список людей."""
    if people:
        line = "├-{}-⫟-{}⫟-{}-⫟-{}-⫟-{}-┤".format(
            "-" * 5, "-" * 25, "-" * 25, "-" * 25, "-" * 18)
        print(line)
        print("| {:^5} | {:^24} | {:^25} | {:^25} | {:^18} |".format(
            "№", "Name", "Surname", "Telephone", "Birthday"))
        print(line)
        for number, human in enumerate(people, 1):
            print("| {:^5} | {:<24} | {:<25} | {:<25} | {:<18} |".format(number, human.get(
                "name", ""), human.get("surname", ""),
                human.get("telephone", ""),
                human.get("birthday", "")))
        print(line)
    else:
        print("There are no people in list!")


def new_human(name: str, surname: str, telephone: str, birthday: str) -> None:
    """Добавить данные о человеке."""
    # Подключение к серверу PostgreSQL.
    with psycopg2.connect(
        dbname="people",
        user="postgres",
        password="password",
        host="127.0.0.1",
        port=5432,
        options="-c client_encoding=UTF8"
    ) as conn:
        with conn.cursor() as cursor:
            # Проверка, существует ли фамилия в таблице surnames.
            cursor.execute(
                """
                    SELECT surname_id FROM surnames WHERE surname = %s
                    """,
                (surname,)
            )
            row = cursor.fetchone()
            if row is None:
                # Если фамилии нет, то добавляется новая запись и получиется её ID.
                cursor.execute(
                    """
                        INSERT INTO surnames (surname) VALUES (%s) RETURNING surname_id
                        """,
                    (surname,)
                )
                surname_id = cursor.fetchone()[0]
            else:
                surname_id = row[0]
            # Добавление записи в таблицу people.
            cursor.execute(
                """
                    INSERT INTO people (name, surname_id, telephone, birthday)
                    VALUES (%s, %s, %s, %s)
                    """,
                (name, surname_id, telephone, birthday)
            )
            conn.commit()


def select_all() -> t.List[t.Dict[str, t.Any]]:
    """Выбрать всех людей."""
    # Подключение к серверу PostgreSQL.
    with psycopg2.connect(
        dbname="people",
        user="postgres",
        password="password",
        host="127.0.0.1",
        port=5432,
        options="-c client_encoding=UTF8"
    ) as conn:
        with conn.cursor() as cursor:
            # Запрос на получение всех записей из таблиц people и surnames.
            cursor.execute(
                """
                    SELECT people.name, surnames.surname, people.telephone, people.birthday
                    FROM people
                    INNER JOIN surnames ON surnames.surname_id = people.surname_id
                    """
            )
            rows = cursor.fetchall()
            # Преобразование результатов в список словарей. Так, в соотношении к именам будет весь первый столбец,
            # Фамилии будут ассоциироваться со 2 столбцом и так далее.
            return [
                {
                    "name": row[0],
                    "surname": row[1],
                    "telephone": row[2],
                    "birthday": row[3]
                }
                for row in rows
            ]


def select_by_month(month: int) -> t.List[t.Dict[str, t.Any]]:
    """Выбрать людей, родившихся в требуемом месяце."""
    # Подключение к серверу PostgreSQL.
    with psycopg2.connect(
        dbname="people",
        user="postgres",
        password="password",
        host="127.0.0.1",
        port=5432,
        options="-c client_encoding=UTF8"
    ) as conn:
        with conn.cursor() as cursor:
            # Запрос на выбор записей из таблиц people и surnames по месяцу рождения.
            cursor.execute(
                """
                    SELECT people.name, surnames.surname, people.telephone, people.birthday
                    FROM people
                    INNER JOIN surnames ON surnames.surname_id = people.surname_id
                    WHERE EXTRACT(MONTH FROM people.birthday) = %s
                    """,
                # %s - используется для параметров, которые предоставят позднее.
                (month,)
            )
            rows = cursor.fetchall()
            # Преобразование результатов в список словарей, аналогично позапрошлому комментарию.
            return [
                {
                    "name": row[0],
                    "surname": row[1],
                    "telephone": row[2],
                    # Преобразование даты в символьный тип для отображения.
                    "birthday": row[3].strftime('%d.%m.%Y')
                }
                for row in rows
            ]


def main(command_line=None):
    file_parser = argparse.ArgumentParser(add_help=False)
    parser = argparse.ArgumentParser("people")
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 0.1.0"
    )
    subparsers = parser.add_subparsers(dest="command")

    add = subparsers.add_parser(
        "add",
        parents=[file_parser],
        help="Add a new human"
    )
    add.add_argument(
        "-n",
        "--name",
        action="store",
        required=True,
        help="The human's name"
    )
    add.add_argument(
        "-s",
        "--surname",
        action="store",
        required=True,
        help="The human's surname."
    )
    add.add_argument(
        "-t",
        "--telephone",
        action="store",
        required=True,
        help="The human's telephone number."
    )
    add.add_argument(
        "-b",
        "--birthday",
        action="store",
        required=True,
        help="The human's birthday."
    )

    _ = subparsers.add_parser(
        "display",
        parents=[file_parser],
        help="Display all people."
    )

    select = subparsers.add_parser(
        "select",
        parents=[file_parser],
        help="Select people."
    )
    select.add_argument(
        "-m",
        "--month",
        action="store",
        type=int,
        required=True,
        help="The required month."
    )

    args = parser.parse_args(command_line)
    create_db()
    if args.command == "add":
        new_human(
            args.name,
            args.surname,
            args.telephone,
            args.birthday
        )
    elif args.command == "display":
        display_people(select_all())
    elif args.command == "select":
        display_people(select_by_month(args.month))


if __name__ == "__main__":
    main()
