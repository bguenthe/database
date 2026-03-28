CREATE OR REPLACE VIEW fixcosts_current AS
WITH ranked_costs AS (
    SELECT
        id,
        creation_time,
        type,
        yearly_costs,
        comment,
        ROW_NUMBER() OVER(PARTITION BY type ORDER BY creation_time DESC) as rn
    FROM fixcosts
)
SELECT
    id,
    creation_time,
    type,
    yearly_costs,
    comment
FROM ranked_costs
WHERE rn = 1;