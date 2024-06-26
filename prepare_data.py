# connect to postgresql database and prepare data for analysis

import psycopg2
# make directory generated
import os

os.makedirs("generated", exist_ok=True)

if __name__ == '__main__':

    # connect to postgresql database
    conn = psycopg2.connect(
        host="localhost",
        database="etl",
        user="etl",
        password="devpass",
        port=5432
    )

    # create exam_grades table
    with conn.cursor() as cur:
        cur.execute("""
CREATE TABLE IF NOT EXISTS "public"."exam_grades" (
  "semester" varchar(255),
  "sex" varchar(255),
  "exam1" numeric,
  "exam2" numeric,
  "exam3" numeric,
  "course_grade" numeric
)
""")
        conn.commit()

    # load data from csv to database
    with conn.cursor() as cur:
        with open("data/exam_grades.csv", "r") as f:
            # skip header
            next(f)
            cur.copy_from(f, "exam_grades", sep=",")
            conn.commit()


    # select all rows from exam_data table and export to csv
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM exam_grades")
        with open("generated/exam_grades-from-db.csv", "w") as f:
            # write header
            f.write("semester,sex,exam1,exam2,exam3,course_grade\n")
            for row in cur:
                row = list(row)
                f.write(",".join(str(cell) for cell in row) + "\n")

    print("Data exported to data/exam_grades.csv")

    with conn.cursor() as cur:
        cur.execute("SELECT * FROM exam_grades")
        with open("generated/exam_grades-from-db-tf.csv", "w") as f:
            # write csv header
            f.write("year,term,gender,exam1,exam2,exam3,course_grade\n")
            for row in cur:
                row = list(row)
                semester = row[0]
                sex = row[1]
                exam1 = row[2]
                exam2 = row[3]
                exam3 = row[4]
                course_grade = row[5]

                year, term = semester.split("-", 1)
                gender = "Male" if sex == "Man" else "Female"
                new_row = [year, term, gender, exam1, exam2, exam3, course_grade]
                f.write(",".join(str(cell) for cell in new_row) + "\n")

    conn.close()