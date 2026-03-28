insert into fixcosts (creation_time, type, yearly_costs, comment)
values ('2026-01-01','Haftpflicht', 64.69,null);

insert into fixcosts (creation_time, type, yearly_costs, comment)
values ('2026-01-01','Hausrat', 170.81,null);

insert into fixcosts (creation_time, type, yearly_costs, comment)
values ('2026-01-01','Wasser/Abwasser', 168.00,null);


select * from fixcosts where type = 'Haftpflicht';