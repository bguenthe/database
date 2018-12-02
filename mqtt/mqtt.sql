select * from mqtt_logger where topic in ('monthlycosts/costs/new', 'monthlycosts/costs/delete') order by logtime desc;

update mqtt_logger set payload=replace(payload, 'trinken_gehen', 'drinks') where topic in ('monthlycosts/costs/new', 'monthlycosts/costs/delete');

update mqtt_logger set payload=replace(payload, 'essen_gehen', 'rest') where topic in ('monthlycosts/costs/new', 'monthlycosts/costs/delete');

delete from mqtt_logger where id in (481121,481120);

delete from mqtt_logger where topic in ('monthlycosts/costs/new', 'monthlycosts/costs/delete');