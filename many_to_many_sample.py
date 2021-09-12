from datetime import date
from sqlite3 import OperationalError

from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy import create_engine, Date
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, relationship
from sqlalchemy_utils import database_exists, create_database, drop_database

Base = declarative_base()
engine = None


class Student(Base):
    __tablename__ = "students"
    id = Column(Integer, primary_key=True, nullable=False)
    first_name = Column(String)
    last_name = Column(String)
    enrollment_date = Column(Date)
    courses = relationship('Course', secondary="studentscourses")


class Course(Base):
    __tablename__ = "courses"
    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String)
    students = relationship("Student", secondary="studentscourses", viewonly=True)


class StudentCourse(Base):
    __tablename__ = "studentscourses"
    id = Column(Integer, primary_key=True, nullable=False)
    student_id = Column(Integer, ForeignKey('students.id'))
    course_id = Column(Integer, ForeignKey('courses.id'))


def setup_database():
    global engine

    # The following is for SQLite
    # engine = create_engine('sqlite:///orm_demo.sqlite')

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
    session = Session(bind=engine)

    student_list = [
        Student(first_name='Alice', last_name='Black', enrollment_date=date.fromisoformat('2021-08-01'), courses=[]),
        Student(first_name='Peter', last_name='Brown', enrollment_date=date.fromisoformat('2021-07-28'), courses=[]),
        Student(first_name='Fred', last_name='Green', enrollment_date=date.fromisoformat('2021-08-13'), courses=[]),
        Student(first_name='Sarah', last_name='Silver', enrollment_date=date.fromisoformat('2021-08-23'), courses=[]),
        Student(first_name='Jack', last_name='White', enrollment_date=date.fromisoformat('2021-07-12'), courses=[])
    ]

    session.bulk_save_objects(student_list, return_defaults=True)
    session.commit()
    print('The following PK values were generated for the bulk insertion of records:')
    for student in student_list:
        print('%s %s: %s' % (student.first_name, student.last_name, student.id))

    course_list = [
        Course(name='History', students=[]),
        Course(name='French', students=[]),
        Course(name='English', students=[]),
        Course(name='Physics', students=[]),
        Course(name='Biology', students=[]),
        Course(name='Chemistry', students=[])
    ]
    session = Session(bind=engine)
    session.bulk_save_objects(course_list, return_defaults=True)
    session.commit()
    print('The following PK values were generated for the bulk insertion of records:')
    for course in course_list:
        print('%s: %s' % (course.name, course.id))

    session.close()


def assign_student_course():
    session = Session(bind=engine)
    courses = session.query(Course).all()

    student = session.query(Student).filter(Student.id == 1).first()
    student.courses.append([item for item in courses if item.name == 'English'][0])
    student.courses.append([item for item in courses if item.name == 'French'][0])
    session.commit()

    student = session.query(Student).filter(Student.id == 2).first()
    student.courses.append([item for item in courses if item.name == 'English'][0])
    student.courses.append([item for item in courses if item.name == 'History'][0])
    student.courses.append([item for item in courses if item.name == 'Biology'][0])
    session.commit()

    student = session.query(Student).filter(Student.id == 3).first()
    student.courses.append([item for item in courses if item.name == 'English'][0])
    student.courses.append([item for item in courses if item.name == 'French'][0])
    student.courses.append([item for item in courses if item.name == 'Biology'][0])
    student.courses.append([item for item in courses if item.name == 'Physics'][0])

    session.commit()

    student = session.query(Student).filter(Student.id == 4).first()
    student.courses.append([item for item in courses if item.name == 'Biology'][0])
    session.commit()

    session.close()


def get_all_students_by_course():
    session = Session(bind=engine)
    courses = session.query(Course).all()

    course_names = {course.name for course in courses}
    for course_name in course_names:
        course = [item for item in courses if item.name == course_name][0]
        print('Total count of students in the %s course: %s' % (course_name, len(course.students)))

    session.close()


def get_students_course(student_id):
    session = Session(bind=engine)
    student = session.query(Student).filter(Student.id == student_id).first()
    print('Following courses are assigned to student: %s %s' % (student.first_name, student.last_name))
    for course in student.courses:
        print('\t %s' % course.name)

    session.close()


print('----Database setup----')
setup_database()
print('\n\n')

print('----Table population----')
populate_table()
assign_student_course()
print('\n\n')

print('----Table query----')
get_all_students_by_course()
print('\n')
get_students_course(3)
print('\n\n')
