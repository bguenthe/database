select * from mqtt.public.costs_view
where 1=1
and comment like '%Kippen%';

select sum(costs_view.costs) from mqtt.public.costs_view
where 1=1
and comment like '%Kippen%';

