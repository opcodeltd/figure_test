from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Table, MetaData
import csv
import os, sys

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

DBI = "postgresql+psycopg2://richard@localhost:5432/testfigure"

IMPORT_PATH = "/Users/richard/Downloads/datasets"

engine = create_engine(DBI)

Base = declarative_base()


class Census(Base):
    __tablename__ = 'census'
    id = Column(Integer, primary_key = True)
    file = Column(String, index = True)
    area = Column(String, index = True)
    key = Column(String, index = True)
    value = Column(String)


Base.metadata.create_all(engine)

Session = sessionmaker()
Session.configure(bind = engine)
session = Session()


def process_csv(fn, file_path):
    global engine
    ins = Census.__table__.insert()
    full_count = 0
    with open(file_path, newline='', encoding='Windows-1252') as csvfh:
        buffer = []

        reader = csv.reader(csvfh)
        first_row = next(reader)
        keys = first_row[3:]

        for count, row in enumerate(reader):
            if not count % 10:
                print("\t%d %d (%d)" % (count, full_count, len(buffer)))
                engine.execute(ins, buffer)
                buffer = []

            if count > 100:
                break

            area = row[0]
            for i, k in enumerate(keys):
                full_count += 1
                value = row[3 + i]
                buffer.append({
                    'file': fn,
                    'area': area,
                    'key': k,
                    'value': value
                })


def import_all():
    for root, dirs, files in os.walk(IMPORT_PATH):
        for fn in files:
            file_path = os.path.join(root, fn)
            print(file_path)
            process_csv(fn, file_path)

import_all()
