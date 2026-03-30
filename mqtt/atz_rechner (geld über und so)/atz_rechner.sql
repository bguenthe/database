/* Durchschnittliche, monatliche Ausgaben
   SOLLTE AM Besten am Monatsanfang ausgeührt werden (wegen Monatsabrundung)
   */
--create table assets_and_costs_and_montly_average_atz as (
insert into assets_and_costs_and_montly_average_atz (;

--insert into assets_and_costs_and_montly_average (
with atz_rente
         as (select 2701.84 as atz_rente),       -- ATZ Rente aus heydorn und den Zahlen von HR (ohne Berücksichtigung der Abmilderungszahlung)
     atz_netto as (select 3084.37 as atz_netto), -- ATZ netto aus Simulation und Abzug der Enggeldumwandlung
     leistungsrate as (select 508 as leistungsrate),
     fixcosts as (select sum(monthly_costs) as fixcosts from fixcosts_view),
     income as (select sum(income_view.income) as income
                from income_view),
     income_last_year as (select sum(income) as income_last_year -- Besser Werter, da es ja gestiegen ist
                          from income_view
                          where 1 = 1
                            and recordDateTime::date >=
                                date_trunc('year', CURRENT_DATE) - INTERVAL '1 year'
                            and recordDateTime::date < date_trunc('year', CURRENT_DATE)),
     min_max as
         (select min(recordDateTime::date) kleinstes_datum,
                 max(recordDateTime::date) größtes_datum
          from costs_view),
     monate_tage as (select extract(YEAR from age(größtes_datum, kleinstes_datum)) * 12 +
                            extract(MONTH from age(größtes_datum, kleinstes_datum)) + 1 monate,
                            größtes_datum - kleinstes_datum                         tage
                     from min_max),
     costs_ohne_allo as (select sum(costs) costs_ohne_allo -- da ich ja ewig nicht mehr trinke
                         from costs_view
                         where type != 'allo'),
     min_max_kippen as
         (select min(recordDateTime)::date kleinstes_datum,
                 max(recordDateTime)::date größtest_datum
          from costs_view
          where 1 = 1
            and lower(comment) like '%kippen%'),
     monate_tage_kippen as (select extract(YEAR from age(größtest_datum, kleinstes_datum)) * 12 +
                                   extract(MONTH from age(größtest_datum, kleinstes_datum)) +1 monate_kippen,
                                   größtest_datum - kleinstes_datum                         tage_kippen
                            from min_max_kippen),
     kippencosts as (select sum(costs) kippencosts
                     from costs_view
                     where lower(comment) like '%kippen%'),
     monatliches_extrageld_bis_ich_85_bin
         as (select 200000 / (extract(YEAR from age('2045-09-01'::DATE, now()::DATE)) * 12 +
                              extract(MONTH from age('2045-09-01'::DATE, now()::DATE))) monatliches_extrageld_bis_ich_85_bin)
select now(),
       monate,
       leistungsrate,
       fixcosts,
       costs_ohne_allo / monate                              "Kosten pro Monat (Ohne Allo mit Kippen)",
       (costs_ohne_allo / monate) + fixcosts                 "Kosten pro Monat (zusätzlich Fixkosten)",
       (costs_ohne_allo / monate) + fixcosts + leistungsrate "Kosten pro Monat (zusätzlich Fixkosten und Leistungsrate)",
       kippencosts / monate_tage_kippen.monate_kippen        "Monatliche Kosten für Kippen",
       -- ATZ_Netto
       atz_netto,
       (atz_netto) - ((costs_ohne_allo / monate) + fixcosts + leistungsrate) "Monatliches Geld über mit ATZ Netto",
       -- ATZ-Rente
       atz_rente,
       (atz_rente) - ((costs_ohne_allo / monate) + fixcosts) "Monatliches Geld über mit ATZ Rente",
       -- Nichtraucher
       (atz_netto) - ((costs_ohne_allo / monate) + fixcosts + leistungsrate) +
       (kippencosts / monate_tage_kippen.monate_kippen)   "Monatliches Geld über mit ATZ Netto und Nichtraucher",
       (atz_rente) - ((costs_ohne_allo / monate) + fixcosts) +
       (kippencosts / monate_tage_kippen.monate_kippen)   "Monatliches Geld über mit ATZ Rente und Nichtraucher",
       monatliches_extrageld_bis_ich_85_bin as               monatliches_extrageld_bis_ich_85_bin
from min_max,
     costs_ohne_allo,
     monate_tage,
     income,
     income_last_year,
     fixcosts,
     leistungsrate,
     atz_rente,
     atz_netto,
     monate_tage_kippen,
     kippencosts,
     monatliches_extrageld_bis_ich_85_bin;