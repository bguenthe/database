Select * from fixcosts_view order by type;

select sum(yearly_costs) / 12 as monatlich, sum(yearly_costs) as jährlich from fixcosts_view;
