drop view if exists invested_funds_view;

CREATE OR REPLACE VIEW invested_funds_view AS
WITH ranked_funds AS (SELECT id,
                             creation_time,
                             name,
                             type,
                             account,
                             amount,
                             interest_rate,
                             comment,
                             ROW_NUMBER() OVER (PARTITION BY name, type, account ORDER BY creation_time DESC) as rn
                      FROM public.invested_funds)
SELECT id,
       creation_time,
       name,
       type,
       account,
       amount,
       interest_rate,
       comment
FROM ranked_funds
WHERE rn = 1;