select * from costs_view
order by recorddatetime desc;

/* Kippen */
select * from costs_view
         where lower(comment) like '%kippen%'
order by recorddatetime desc;

/* Kippen gesamt */
select (sum((costs)::DOUBLE PRECISION)) kippencosts
from costs_view
where 1 = 1
  and lower(comment) like '%kippen%';