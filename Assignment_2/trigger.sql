drop table if exists department;
create table department (
    dept_id char(3),
    dept_name varchar(40) not null
);

CREATE OR REPLACE FUNCTION validate_student_id_function()
RETURNS TRIGGER AS $$
BEGIN
    If(old.dept_id='abd') then
    raise exception 'invalid';
    end if;
    return old;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER validate_student_id
BEFORE DELETE ON department
FOR EACH ROW
EXECUTE FUNCTION validate_student_id_function();

insert into department values
    ('abc','avadhesh');

select * from department;
delete from department where dept_name='avadhesh';

select * from department;