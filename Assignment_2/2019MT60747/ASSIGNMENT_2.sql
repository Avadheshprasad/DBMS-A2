DROP TABLE IF EXISTS student_dept_change CASCADE;
DROP TABLE IF EXISTS valid_entry CASCADE;
DROP TABLE IF EXISTS student_courses CASCADE;
DROP TABLE IF EXISTS course_offers CASCADE;
DROP TABLE IF EXISTS professor CASCADE;
DROP TABLE IF EXISTS courses CASCADE;
DROP TABLE IF EXISTS student CASCADE;
DROP TABLE IF EXISTS department CASCADE;

CREATE TABLE department (
    dept_id CHAR(3),
    dept_name VARCHAR(40) NOT NULL UNIQUE,
    PRIMARY KEY(dept_id)
);

CREATE TABLE student (
    first_name VARCHAR(40) NOT NULL,
    last_name VARCHAR(40),
    student_id CHAR(11) NOT NULL,
    address VARCHAR(100),
    contact_number CHAR(10) NOT NULL UNIQUE,
    email_id VARCHAR(50) UNIQUE,
    tot_credits INTEGER NOT NULL,
    dept_id CHAR(3),
    PRIMARY KEY (student_id),
    CHECK(tot_credits >= 0),
    FOREIGN KEY (dept_id) REFERENCES department (dept_id) 
);

CREATE TABLE courses (
    course_id CHAR(6) NOT NULL,
    course_name VARCHAR(20) NOT NULL UNIQUE,
    course_desc TEXT,
    credits NUMERIC NOT NULL,
    dept_id CHAR(3),
    PRIMARY KEY (course_id),
    CHECK (credits > 0),
    FOREIGN KEY (dept_id) REFERENCES department (dept_id),
    CHECK (
        SUBSTRING(course_id FROM 1 FOR 3) = dept_id
        AND SUBSTRING(course_id FROM 4 FOR 3) ~ '[0-9][0-9][0-9]'
    )
);

CREATE TABLE professor (
    professor_id VARCHAR(10),
    professor_first_name VARCHAR(40) NOT NULL,
    professor_last_name VARCHAR(40) NOT NULL,
    office_number VARCHAR(20),
    contact_number CHAR(10) NOT NULL,
    start_year INTEGER,
    resign_year INTEGER,
    dept_id CHAR(3),
    CHECK (start_year <= resign_year),
    PRIMARY KEY (professor_id),
    FOREIGN KEY (dept_id) REFERENCES department (dept_id)
);

CREATE TABLE course_offers (
    course_id CHAR(6),
    session VARCHAR(9),
    semester INTEGER NOT NULL,
    professor_id VARCHAR(10),
    capacity INTEGER,
    enrollments INTEGER,
    CHECK (semester = 1 OR semester = 2),
    PRIMARY KEY (course_id, session, semester),
    FOREIGN KEY (professor_id) REFERENCES professor (professor_id),
    FOREIGN KEY (course_id) REFERENCES courses (course_id)
);

CREATE TABLE student_courses (
    student_id CHAR(11),
    course_id CHAR(6),
    session VARCHAR(9),
    semester INTEGER,
    grade NUMERIC NOT NULL,
    CHECK (grade >= 0 AND grade <= 10),
    CHECK (semester = 1 OR semester = 2),
    FOREIGN KEY (student_id) REFERENCES student (student_id) ON UPDATE CASCADE,
    FOREIGN KEY (course_id, session, semester) REFERENCES course_offers (course_id, session, semester)
);

CREATE TABLE valid_entry (
    dept_id CHAR(3),
    entry_year INTEGER NOT NULL,
    seq_number INTEGER NOT NULL,
    FOREIGN KEY (dept_id) REFERENCES department (dept_id)
);

CREATE OR REPLACE FUNCTION f1()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM valid_entry
        WHERE dept_id = SUBSTRING(NEW.student_id FROM 5 FOR 3)
        AND entry_year = CAST(SUBSTRING(NEW.student_id FROM 1 FOR 4) AS INTEGER)
        AND seq_number = CAST(SUBSTRING(NEW.student_id FROM 8 FOR 3) AS INTEGER)
        AND NEW.email_id = SUBSTRING(NEW.student_id FROM 1 FOR 10) || '@' || SUBSTRING(NEW.student_id FROM 5 FOR 3) || '.iitd.ac.in'
    ) THEN
        RETURN NEW;
    ELSE
        RAISE EXCEPTION 'invalid';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER validate_student_id
BEFORE INSERT ON student
FOR EACH ROW
EXECUTE PROCEDURE f1();

CREATE OR REPLACE FUNCTION f2()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE valid_entry
    SET seq_number = seq_number + 1
    WHERE dept_id = SUBSTRING(NEW.student_id FROM 5 FOR 3)
    AND entry_year = CAST(SUBSTRING(NEW.student_id FROM 1 FOR 4) AS INTEGER);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_seq_number
AFTER INSERT ON student
FOR EACH ROW
EXECUTE PROCEDURE f2();

CREATE TABLE student_dept_change (
    old_student_id CHAR(11) NOT NULL,
    old_dept_id CHAR(3) NOT NULL,
    new_dept_id CHAR(3) NOT NULL,
    new_student_id CHAR(11) NOT NULL,
    FOREIGN KEY (old_dept_id) REFERENCES department (dept_id),
    FOREIGN KEY (new_dept_id) REFERENCES department (dept_id)
);


CREATE OR REPLACE FUNCTION f3()
RETURNS TRIGGER AS $$
DECLARE 
    avg_grade NUMERIC;
BEGIN
    IF NEW.dept_id <> OLD.dept_id THEN
        IF EXISTS (
            SELECT 1
            FROM student_dept_change
            WHERE new_student_id = OLD.student_id
        ) THEN
            RAISE EXCEPTION 'Department can be changed only once';
        END IF;

        IF CAST(SUBSTRING(OLD.student_id FROM 1 FOR 4) AS INTEGER) < 2022 THEN
            RAISE EXCEPTION 'Entry year must be >= 2022';
        END IF;


        SELECT AVG(grade) INTO avg_grade
        FROM student_courses
        WHERE student_id = OLD.student_id;


        IF (avg_grade IS NULL OR avg_grade <= 8.5) THEN
            RAISE EXCEPTION 'Low Grade';
        END IF;

        IF EXISTS (
            SELECT 1
            FROM valid_entry
            WHERE dept_id = SUBSTRING(NEW.student_id FROM 5 FOR 3)
            AND entry_year = CAST(SUBSTRING(NEW.student_id FROM 1 FOR 4) AS INTEGER)
            AND seq_number = CAST(SUBSTRING(NEW.student_id FROM 8 FOR 3) AS INTEGER)
            AND NEW.email_id = SUBSTRING(NEW.student_id FROM 1 FOR 10) || '@' || SUBSTRING(NEW.student_id FROM 5 FOR 3) || '.iitd.ac.in'
        ) THEN
            RETURN NEW;
        ELSE
            RAISE EXCEPTION 'invalid';
        END IF;

        UPDATE valid_entry
        SET seq_number = seq_number + 1
        WHERE dept_id = SUBSTRING(NEW.student_id FROM 5 FOR 3)
        AND entry_year = CAST(SUBSTRING(NEW.student_id FROM 1 FOR 4) AS INTEGER);
        
        INSERT INTO student_dept_change VALUES
            (OLD.student_id,OLD.dept_id,NEW.dept_id,NEW.student_id);

        RETURN NEW;

    ELSE
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER log_student_dept_change
BEFORE UPDATE ON student
FOR EACH ROW
EXECUTE PROCEDURE f3(); 


CREATE MATERIALIZED VIEW course_eval AS
SELECT
    course_id,
    session,
    semester,
    COUNT(student_id) AS number_of_students,
    AVG(grade) AS average_grade,
    MAX(grade) AS max_grade,
    MIN(grade) AS min_grade
FROM
    student_courses
GROUP BY
    course_id,
    session,
    semester;

CREATE MATERIALIZED VIEW student_semester_summary AS
SELECT
    student_id,
    session,
    semester,
    1.0 * SUM(credits * grade) / SUM(credits) AS sgpa,
    SUM(credits) AS credits
FROM
    student_courses
JOIN
    courses ON student_courses.course_id = courses.course_id
WHERE
    grade >= 5.0
GROUP BY
    student_id,
    session,
    semester;

CREATE OR REPLACE FUNCTION f4()
RETURNS TRIGGER AS $$
DECLARE
    course_credits NUMERIC;
BEGIN
    SELECT credits INTO course_credits
    FROM courses
    WHERE course_id = NEW.course_id;

    -- Update the student's tot_credits by adding the credits for the new course
    UPDATE student
    SET tot_credits = tot_credits + course_credits
    WHERE student_id = NEW.student_id;

    UPDATE course_offers
    SET enrollments = enrollments + 1
    WHERE course_id = NEW.course_id
    AND session = NEW.session
    AND semester = NEW.semester;

    REFRESH MATERIALIZED VIEW course_eval;
    REFRESH MATERIALIZED VIEW student_semester_summary;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER insert_student_courses
AFTER INSERT ON student_courses
FOR EACH ROW
EXECUTE PROCEDURE f4();

CREATE OR REPLACE FUNCTION f5()
RETURNS TRIGGER AS $$
DECLARE
    course_credits NUMERIC;
BEGIN
    IF (TG_OP = 'DELETE') THEN
        SELECT credits INTO course_credits
        FROM courses
        WHERE course_id = OLD.course_id;

        UPDATE student
        SET tot_credits = tot_credits - course_credits
        WHERE student_id = OLD.student_id;

        UPDATE course_offers
        SET enrollments = enrollments - 1
        WHERE course_id = OLD.course_id
        AND session = OLD.session
        AND semester = OLD.semester;

        REFRESH MATERIALIZED VIEW student_semester_summary;
        RETURN OLD;
        
    ELSIF (TG_OP = 'UPDATE') THEN
        IF (OLD.grade <> NEW.grade) THEN
            REFRESH MATERIALIZED VIEW student_semester_summary;
        END IF;
        REFRESH MATERIALIZED VIEW course_eval;
        RETURN NEW;
    END IF;

    
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_delete_student_courses
AFTER UPDATE OR DELETE ON student_courses
FOR EACH ROW
EXECUTE FUNCTION f5();

CREATE OR REPLACE FUNCTION f6()
RETURNS TRIGGER AS $$
DECLARE 
    course_credits NUMERIC;
    total_credits_per_sem NUMERIC;
    total_courses INTEGER;
    total_credits NUMERIC;
    enroll INTEGER;
    capac INTEGER;
BEGIN
    SELECT credits INTO course_credits
    FROM courses
    WHERE course_id = NEW.course_id;

    SELECT SUM(credits), COUNT(courses.course_id) INTO total_credits_per_sem, total_courses
    FROM student_courses
    JOIN courses ON student_courses.course_id = courses.course_id
    WHERE student_id = NEW.student_id
    AND session = NEW.session
    AND semester = NEW.semester;

    SELECT tot_credits INTO total_credits
    FROM student
    WHERE student_id = NEW.student_id;

    IF ((total_credits + course_credits) > 60 OR (total_credits_per_sem + course_credits) > 26 OR total_courses > 4) THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    IF (course_credits = 5 AND SUBSTRING(NEW.student_id FROM 1 FOR 4) <> SUBSTRING(NEW.session FROM 1 FOR 4)) THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    SELECT enrollments, capacity INTO enroll, capac
    FROM course_offers
    WHERE course_id = NEW.course_id
    AND session = NEW.session
    AND semester = NEW.semester;


    IF (enroll < capac) THEN
        UPDATE course_offers
        SET enrollments = enrollments + 1
        WHERE course_id = NEW.course_id
        AND session = NEW.session
        AND semester = NEW.semester;
    ELSE 
        RAISE EXCEPTION 'course is full';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_insert_student_courses
BEFORE INSERT ON student_courses
FOR EACH ROW
EXECUTE procedure f6();




-- 
-- 2.3
CREATE OR REPLACE FUNCTION f7()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM student_courses
    WHERE course_id = OLD.course_id
    AND session = OLD.session
    AND semester = OLD.semester;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER delete_course_offers
AFTER DELETE ON course_offers
FOR EACH ROW
EXECUTE PROCEDURE f7();

CREATE OR REPLACE FUNCTION f8()
RETURNS TRIGGER AS $$
BEGIN
-- check

    IF NOT EXISTS (SELECT 1 FROM professor WHERE professor_id = NEW.professor_id) THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM courses WHERE course_id = NEW.course_id) THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM professor
        WHERE professor_id = NEW.professor_id
        AND resign_year < CAST(SUBSTRING(NEW.session FROM 6 FOR 4) AS INTEGER)
    ) THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    IF (
        SELECT COUNT(*)
        FROM course_offers
        WHERE session = NEW.session
        AND professor_id = NEW.professor_id
    ) >= 4 THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_insert_course_offers
BEFORE INSERT ON course_offers
FOR EACH ROW
EXECUTE PROCEDURE f8();

CREATE OR REPLACE FUNCTION f9()
RETURNS TRIGGER AS $$
BEGIN
    IF (OLD.dept_id <> NEW.dept_id) THEN

        UPDATE student_dept_change
        SET old_dept_id=NEW.dept_id
        WHERE old_dept_id=OLD.dept_id;

        UPDATE student_dept_change
        SET new_dept_id=NEW.dept_id
        WHERE new_dept_id=OLD.dept_id;

        UPDATE students
        SET dept_id = NEW.dept_id
        WHERE dept_id = OLD.dept_id;

        UPDATE student_courses
        SET dept_id = NEW.dept_id,
        course_id = CONCAT(NEW.dept_id, RIGHT(course_id, 3))
        WHERE LEFT(course_id, 3) = OLD.dept_id;

        UPDATE course_offers
        SET course_id = CONCAT(NEW.dept_id, RIGHT(course_id, 3))
        WHERE LEFT(course_id, 3) = OLD.dept_id;

        UPDATE professor
        SET dept_id = NEW.dept_id
        WHERE dept_id = OLD.dept_id;

        UPDATE courses
        SET dept_id = NEW.dept_id,
        course_id = CONCAT(NEW.dept_id, RIGHT(course_id, 3))
        WHERE dept_id = OLD.dept_id;

    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_deptartment
BEFORE UPDATE ON department
FOR EACH ROW
EXECUTE PROCEDURE f9();


CREATE OR REPLACE FUNCTION f10()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM student WHERE dept_id = OLD.dept_id
    ) THEN
        RAISE EXCEPTION 'Department has students';
    ELSE
        DELETE FROM course_offers WHERE LEFT(course_id, 3) = OLD.dept_id;
        DELETE FROM professor WHERE LEFT(course_id, 3) = OLD.dept_id;
        DELETE FROM courses WHERE dept_id = OLD.dept_id;
    END IF;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER delete_department
BEFORE DELETE ON department
FOR EACH ROW
EXECUTE PROCEDURE f10();
