#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import sqlite3
import typing as t
from pathlib import Path


'''Данные о людях хранятся в файле, создаваемом при помощи SQLite3 – people.db.
По умолчанию, файл создаётся в домашнем каталоге пользователя.
В данном файле имеется две таблицы – people и surnames'''


def create_db(database_path: Path) -> None:
    """Создать базу данных."""
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    # Создать таблицу с информацией о фамилиях.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS surnames (
            surname_id INTEGER PRIMARY KEY AUTOINCREMENT,
            surname TEXT NOT NULL
        )
        """
    )
    # Создать таблицу с информацией о людях.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS people (
            human_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            surname_id INTEGER NOT NULL,
            telephone TEXT NOT NULL,
            birthday TEXT NOT NULL,
            FOREIGN KEY(surname_id) REFERENCES surnames(surname_id)
        )
        """
    )
    conn.close()


def display_people(people):
    """Отобразить список людей."""
    # Проверка, что в списке есть люди.
    if people:
        # Заголовок таблицы.
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


def new_human(database_path: Path, name: str, surname: str, telephone: str, birthday: str) -> None:
    """Добавить данные о человеке."""
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    # Получить идентификатор фамилии в базе данных.
    # Если такой записи нет, то добавить информацию о новой фамилии.
    cursor.execute(
        """
        SELECT surname_id FROM surnames WHERE surname = ?
        """,
        (surname,)
    )
    row = cursor.fetchone()
    if row is None:
        cursor.execute(
            """
            INSERT INTO surnames (surname) VALUES (?)
            """,
            (surname,)
        )
        surname_id = cursor.lastrowid
    else:
        surname_id = row[0]

    # Добавить информацию о новом человеке. ? используются для безопасности, значения параметров экранируются.
    cursor.execute(
        """
        INSERT INTO people (name, surname_id, telephone, birthday)
        VALUES (?, ?, ?, ?)
        """,
        (name, surname_id, telephone, birthday)
    )
    conn.commit()
    conn.close()


def select_all(database_path: Path) -> t.List[t.Dict[str, t.Any]]:
    """Выбрать всех людей."""
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT people.name, surnames.surname, people.telephone, people.birthday
        FROM people
        INNER JOIN surnames ON surnames.surname_id = people.surname_id
        """
    )
    rows = cursor.fetchall()
    conn.close()
    return [
        {
            "name": row[0],
            "surname": row[1],
            "telephone": row[2],
            "birthday": row[3]
        }
        for row in rows
    ]


def select_by_month(database_path: Path, month: int) -> t.List[t.Dict[str, t.Any]]:
    """Выбрать людей, родившихся в требуемом месяце."""
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT people.name, surnames.surname, people.telephone, people.birthday
        FROM people
        INNER JOIN surnames ON surnames.surname_id = people.surname_id
        WHERE strftime('%m', people.birthday) = ?
        """,
        # strftime('%m', people.birthday) - SQLLite функция, берущая месяц из people.birthday в виде строки.
        # = ? - В SQLite, ? используется для параметров, которые предоставят позднее.
        (str(month).zfill(2),)
    )
    rows = cursor.fetchall()
    conn.close()
    return [
        {
            "name": row[0],
            "surname": row[1],
            "telephone": row[2],
            "birthday": row[3]
        }
        for row in rows
    ]


def main(command_line=None):
    # Создать родительский парсер для определения имени файла.
    file_parser = argparse.ArgumentParser(add_help=False)
    file_parser.add_argument(
        "--db",
        action="store",
        required=False,
        default=str(Path.home() / "people.db"),
        help="The database file name"
    )

    # Создать основной парсер командной строки.
    parser = argparse.ArgumentParser("people")
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 0.1.0"
    )
    subparsers = parser.add_subparsers(dest="command")

    # Создать субпарсер для добавления человека.
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

    # Создать субпарсер для отображения всех работников.
    _ = subparsers.add_parser(
        "display",
        parents=[file_parser],
        help="Display all people."
    )

    # Создать субпарсер для выбора работников.
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

    # Выполнить разбор аргументов командной строки.
    args = parser.parse_args(command_line)
    # Получить путь к файлу базы данных.
    db_path = Path(args.db)
    create_db(db_path)
    # Добавить человека.
    if args.command == "add":
        new_human(
            db_path,
            args.name,
            args.surname,
            args.telephone,
            args.birthday
        )
    # Отобразить всех людей.
    elif args.command == "display":
        display_people(select_all(db_path))
    # Выбрать требуемых людей.
    elif args.command == "select":
        display_people(select_by_month(db_path, args.month))
        pass


if __name__ == "__main__":
    main()
