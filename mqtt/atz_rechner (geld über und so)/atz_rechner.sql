/* Durchschnittliche, monatliche Ausgaben
   SOLLTE AM Besten am Monatsanfang ausgeührt werden (wegen Monatsabrundung)
   */
--create table assets_and_costs_and_montly_average_atz as (
insert into assets_and_costs_and_montly_average_atz (;

--insert into assets_and_costs_and_montly_average (
with
    atz_rente as (select 2701.84 as atz_rente), -- ATZ Rente aus heydorn und den Zahlen von HR (ohne Berücksichtigung der Abmilderungszahlung
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
                           extract(MONTH from age(größtes_datum, kleinstes_datum)) monate,
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
                                  extract(MONTH from age(größtest_datum, kleinstes_datum)) monate_kippen,
                                  größtest_datum - kleinstes_datum                         tage_kippen
                           from min_max_kippen),
    kippencosts as (select sum(costs) kippencosts
                    from costs_view
                    where lower(comment) like '%kippen%'),
    monatliches_extrageld_bis_ich_85_bin
        as (select 200000 / (extract(YEAR from age('2045-09-01'::DATE, now()::DATE)) * 12 +
                             extract(MONTH from age('2045-09-01'::DATE, now()::DATE))) monatliches_extrageld_bis_ich_85_bin)
select now(),
       leistungsrate,
       fixcosts,
       income / monate                                                   "Monatliche Einnahmen (über gesamte Laufzeit der app)",
       income_last_year / 12                                             "Monatliche Einnahmen der letzen 12 Monate",
       costs_ohne_allo / monate                                          "Kosten pro Monat",
       (costs_ohne_allo / monate) + fixcosts                             "Kosten pro Monat (inkl. Fixkosten)",
       (costs_ohne_allo / monate) + fixcosts + leistungsrate             "Kosten pro Monat (inkl. Fixkosten und Leistungsrate)",
       kippencosts / monate_tage_kippen.tage_kippen * 30                 "Monatliche Kosten für Kippen",
       (income_last_year / 12) -
       ((costs_ohne_allo / monate) + fixcosts + leistungsrate)           "Monatliches Geld über (Fixkosten und Leistungsrate)",
       (income_last_year / 12) - ((costs_ohne_allo / monate) + fixcosts) "Monatliches Geld über mit 65 Jahren (ohne Leistungsrate)",
       (atz_rente) - ((costs_ohne_allo / monate) + fixcosts)            "Monatliches Geld über mit regulärer Rente",
       (atz_rente) - ((costs_ohne_allo / monate) + fixcosts) +
       (kippencosts / monate_tage_kippen.tage_kippen * 30) "Monatliches Geld über mit regulärer Rente (und Nichtraucher)",
       monatliches_extrageld_bis_ich_85_bin as monatliches_extrageld_bis_ich_85_bin
from min_max,
     costs_ohne_allo,
     monate_tage,
     income,
     income_last_year,
     fixcosts,
     leistungsrate,
     atz_rente,
     monate_tage_kippen,
     kippencosts,
     monatliches_extrageld_bis_ich_85_bin;