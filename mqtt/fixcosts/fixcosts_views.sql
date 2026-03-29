drop view if exists fixcosts_view;
CREATE OR REPLACE VIEW fixcosts_view AS
WITH ranked_costs AS (
    SELECT
        id,
        creation_time,
        type,
        yearly_costs::DOUBLE PRECISION,
        yearly_costs::DOUBLE PRECISION / 12 as monthly_costs,
        comment,
        ROW_NUMBER() OVER(PARTITION BY type ORDER BY creation_time DESC) as rn
    FROM fixcosts
)
SELECT
    id,
    creation_time,
    type,
    yearly_costs,
    monthly_costs,
    comment
FROM ranked_costs
WHERE rn = 1;