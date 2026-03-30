/* Anzahl an Einträgen */
select count(*)
from costs_view;

select count(*)
from income_view;

/* sonstige Kosten absteigend nach preis */
select *
from costs_view
where 1 = 1
  and type = 'sonst'
order by to_number(costs, '9999999.99') desc;