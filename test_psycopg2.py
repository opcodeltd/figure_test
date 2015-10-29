from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Table, MetaData
import csv
import os, sys
import psycopg2

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

DBI = "postgresql+psycopg2://richard@localhost:5432/testfigure"

IMPORT_PATH = "csv"

connection = psycopg2.connect(database='testfigure',user='richard',host='localhost', port=5432)
print("Auto ",connection.autocommit)


def process_csv(fn, file_path):
    global connection
    cursor = connection.cursor()
    full_count = 0
    with open(file_path, newline='', encoding='Windows-1252') as csvfh:
        buffer = []

        reader = csv.reader(csvfh)
        first_row = next(reader)
        keys = first_row[3:]

        for count, row in enumerate(reader):


            if count > 100:
                break

            area = row[0]
            for i, k in enumerate(keys):
                full_count += 1
                value = row[3 + i]
                buffer.append((fn, area, k, value))

            if not count % 10:
                print("\t",count)

        cursor.executemany("INSERT INTO census (file, area, key, value) VALUES (%s, %s, %s, %s)", buffer)
        connection.commit()


def import_all():
    for root, dirs, files in os.walk(IMPORT_PATH):
        for fn in files:
            file_path = os.path.join(root, fn)
            print(file_path)
            process_csv(fn, file_path)

import_all()
