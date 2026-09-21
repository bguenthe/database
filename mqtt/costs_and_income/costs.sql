select * from mqtt.public.costs_view
where 1=1
and lower(comment) like '%kippen%';

select sum(costs) from mqtt.public.costs_view
where 1=1
  and lower(comment) like '%kippen%';

/* Urlaubskosten
select * from mqtt.public.costs_view
where 1=1
and to_date(recordDateTime, 'YYYY-MM-DD') >= to_date('2026-09-08', 'YYYY-MM-DD')
--and to_date(recordDateTime, 'YYYY-MM-DD') <= to_date('2026-09-25', 'YYYY-MM-DD')
order by costs_view.recorddatetime desc;