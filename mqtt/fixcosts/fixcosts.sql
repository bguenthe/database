Select * from fixcosts_view order by type;
Select * from fixcosts order by type;

delete from fixcosts;

select sum(yearly_costs) / 12 as monatlich, sum(yearly_costs) as jährlich from fixcosts_view;

alter table fixcosts
    alter column creation_time type timestamp with time zone using creation_time::timestamp with time zone;

show timezone;

drop database fixcosts