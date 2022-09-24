--ВЕДУЩАЯ ТАБЛИЦА
with 

form_1 as (
    select 
        stud_id
        ,form_id
        ,form_of_education
        ,finance
        ,course
        ,cast(education_start as date)
        ,cast(date_apply as date)
        ,current_date --Найди функцию текущей даты
        ,code
    from form
),

form_2 as (
    select 
        stud_id
        ,form_id
        ,form_of_education
        ,finance
        ,course
        ,education_start
        ,date_apply
        ,current_date
        ,code

        ,case
            when month(current_date) <= 6 then 2 else 1 
        end as current_date_semester

        ,case
            when month(date_apply) <= 6 then 0 else 1
        end as date_apply_semester
    from form_1
),

form_3 as (
    select 
        stud_id
        ,form_id
        ,form_of_education
        ,finance
        ,course
        ,education_start
        ,date_apply
        ,current_date
        ,code
        ,current_date_semester
        ,date_apply_semester

        ,case
            when code = 4
                then (year(date_apply) - year(education_start)) * 2 + date_apply_semester
        end as total_semester

        ,case 
            when finance = 'Д' then
                case when year(date_apply) = year(education_start) then 1
                else total_semester - (year(current_date) - year(date_apply)) * 2 + date_apply_semester
                end
            else null
        end as total_pay_semester
    from form_2
),

allfinal as (
    select 
        s.spec_id
        ,s.filial_id
        ,f.filial_name
        ,s.spec_name
        ,s.years
        ,s.cost
    from spec s
    inner join (
        select 
            filial_id
            ,filial_name
        from filial
    ) f on s.filial_id = f.filial_id
),

result_1 as (
    select 
        s.stud_id
        ,s.spec_id
        ,s.last_name||' '||name||' '||middle_name fio
        ,a.filial_id
        ,a.filial_name
        ,a.spec_name
        ,a.years
        ,a.cost
        ,f.form_id
        ,f.form_of_education
        ,f.finance
        ,f.course
        ,f.education_start
        ,f.date_apply
        ,f.current_date
        ,f.code
        ,f.current_date_semester
        ,f.date_apply_semester
        ,case
            when f.total_semester > a.years * 2 then a.years * 2
            else f.total_semester
        end as total_semester
        ,f.total_pay_semester
        ,case
            when f.form_of_education = 'Очная' then 8
            else 10            
        end as max_semester
        
    from stud s 

    inner join (
        select *
        from allfinal
    ) a on s.spec_id = a.spec_id

    left join (
        select *
        from form_3
    ) f s.stud_id = f.stud_id
),

result_2 as (
    select 
        stud_id
        ,spec_id
        ,fio
        ,filial_id
        ,filial_name
        ,spec_name
        ,years
        ,cost
        ,form_id
        ,form_of_education
        ,finance
        ,course
        ,education_start
        ,date_apply
        ,current_date
        ,code
        ,current_date_semester
        ,date_apply_semester
        ,total_semester
        ,max_semester
        ,total_pay_semester
        ,ceiling(total_semester / 2) as total_course
        ,case
            when code = 4
                then "Отчислен на "||total_course||" курсе, "||total_semester||" семестре."
            when total_semester >= max_semester
                then "Закончил обучение на "||years||" курсе, "||max_semester||" семестре."
            else "Обучается на "||total_course||" курсе, "||total_semester||" семестре."
        end as status
        ,max(date_apply) over() as max_date 

    from result_1 r
),