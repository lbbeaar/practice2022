--ТАБЛИЦЫ ДЛЯ СБОРКИ
--БЮДЖЕТ
select
    stud_id
    ,spec_id
    ,fio
    ,spec_id
    ,filial_name
    ,spec_name
    ,form_id
    ,finance
    ,current_date
    ,code
    ,total_semester
    ,total_pay_semester
from result_2
where finance = 'Б';

--ДОГОВОРНИКИ
select
    stud_id
    ,spec_id
    ,fio
    ,spec_id
    ,filial_name
    ,spec_name
    ,form_id
    ,finance
    ,current_date
    ,code
    ,total_semester
    ,total_pay_semester
    ,cost / 2 * total_pay_semester as pay
from result_2
where finance = 'Д';

--ОБУЧАЮЩИЕСЯ
with decl as (
select 
    stud_id
    ,fio
    ,spec_id
    ,spec_name
    ,filial_name
    ,form_id
    ,finance
    ,total_course
    ,total_semester
    ,status
    ,code
    ,current_date
    ,row_number() over (partition by stud_id order by date_apply desc) as rn
where code <> 4
and total_semester <> max_semester
from result_2
)

select *
from decl
where rn = 1;

--ОТЧИСЛИВШИЕСЯ И ЗАКОНЧИВШИЕ
select 
    stud_id
    ,fio
    ,spec_id
    ,spec_name
    ,filial_name
    ,form_id
    ,finance
    ,total_course
    ,total_semester
    ,status
    ,code
    ,current_date
where  code = 4
or total_semester = max_semester
from result_2;

