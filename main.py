from datetime import date

from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine, Date
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy_utils import database_exists, create_database, drop_database

Base = declarative_base()
engine = None


# This is the entity (i.e., model class)
class Students(Base):
    __tablename__ = "students"
    id = Column(Integer, primary_key=True, nullable=False)
    first_name = Column(String)
    last_name = Column(String)
    enrollment_date = Column(Date)


def setup_database():
    global engine

    # The following is for SQLite
    #engine = create_engine('sqlite:///orm_demo.sqlite')

    # The following is for PostgreSQL
    # make sure the username, password and server are valid
    # use echo=True to get a verbose output
    engine = create_engine("postgresql://swen344:swen344@localhost/orm_demo")

    # create the database
    try:
        if database_exists(engine.url):
            drop_database(engine.url)
        create_database(engine.url)
    except OperationalError:
        pass

    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


def populate_table():
    # Insert a single record
    student = Students(first_name='John', last_name='Smith', enrollment_date=date.fromisoformat('2021-08-13'))
    session = Session(bind=engine)
    session.add(student)
    session.commit()
    print('Saved first student record. The database generated PK value \'%s\' for this record' % student.id)

    # Insert multiple records
    student_list = [
        Students(first_name='Alice', last_name='Black', enrollment_date=date.fromisoformat('2021-08-01')),
        Students(first_name='Peter', last_name='Brown', enrollment_date=date.fromisoformat('2021-07-28')),
        Students(first_name='Fred', last_name='Green', enrollment_date=date.fromisoformat('2021-08-13')),
        Students(first_name='Sarah', last_name='Silver', enrollment_date=date.fromisoformat('2021-08-23')),
        Students(first_name='Jack', last_name='White', enrollment_date=date.fromisoformat('2021-07-12'))
    ]
    session.bulk_save_objects(student_list, return_defaults=True)
    session.commit()
    print('The following PK values were generated for the bulk insertion of records:')
    for student in student_list:
        print('%s %s: %s' % (student.first_name, student.last_name, student.id))


def fetch_all_students():
    session = Session(bind=engine)
    students = session.query(Students).all()
    print('Total number of student records in database: %s' % len(students))
    print('\n')

    print('Student records sorted by FirstName:')
    sorted_list = sorted(students, key=lambda x: x.first_name, reverse=False)
    for student in sorted_list:
        print('%s %s: %s' % (student.first_name, student.last_name, student.id))
    print('\n')

    print('Student enrolled in August:')
    students_august = list(filter(lambda x: x.enrollment_date.month == 8, students))
    sorted_list = sorted(students_august, key=lambda x: x.enrollment_date, reverse=False)
    for student in sorted_list:
        print('%s %s: %s' % (student.first_name, student.last_name, student.enrollment_date))


print('----Database setup----')
setup_database()
print('\n\n')

print('----Table population----')
populate_table()
print('\n\n')

print('----Table query----')
fetch_all_students()
print('\n\n')
