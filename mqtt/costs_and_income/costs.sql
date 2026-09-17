select * from mqtt.public.costs_view
where 1=1
and lower(comment) like '%kippen%';

select sum(costs) from mqtt.public.costs_view
where 1=1
  and lower(comment) like '%kippen%';

select * from mqtt.public.costs_view
where 1=1
order by costs_view.recorddatetime desc;

