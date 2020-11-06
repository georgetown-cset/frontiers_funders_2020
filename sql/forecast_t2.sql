/* Number of publications in AI clusters */
select sum(ROUND( EU_affiliation_share * NP)) as eu,    sum(ROUND( USA_affiliation_share * NP)) as us,
 sum(ROUND( China_affiliation_share * NP)) as ch,    sum(ROUND( Japan_affiliation_share * NP)) as jap, sum(NP) as world
from frontiers_forecasting.funders_forecast_2020



/* publications in high growth clusters */

select sum(ROUND( EU_affiliation_share * NP)) as eu,    sum(ROUND( USA_affiliation_share * NP)) as us,
 sum(ROUND( China_affiliation_share * NP)) as ch,    sum(ROUND( Japan_affiliation_share * NP)) as jap, sum(NP) as world
from frontiers_forecasting.funders_forecast_2020 where x_growth = 1

/* Funded number of papers */

select
ROUND(sum(ec_share * NP)) as EC,
ROUND(sum(erc_share * NP)) as ERC,
ROUND(sum(jap_sps_share * NP)) as JPS,
ROUND(sum(nih_share * NP)) as NIH,
ROUND(sum(nnsf_china_share * NP)) as NNSF,
ROUND(sum(nsf_share * NP)) as NSF
from frontiers_forecasting.funders_forecast_2020



/* median share of funded papers */
WITH
perc_tab as ( select
100*percentiles_erc[offset(50)] as erc, 100*percentiles_ec[offset(50)] as ec , 100*percentiles_jap_sps[offset(50)] as jps,  100*percentiles_nih[offset(50)] as nih,
100*percentiles_nnsf[offset(50)] as nnsf, 100*percentiles_nsf[offset(50)] as nsf

from
(
select *  from frontiers_forecasting.funders_forecast_2020 ,
(select APPROX_QUANTILES(nnsf_china_share,100) as percentiles_nnsf from frontiers_forecasting.funders_forecast_2020 where ai_share > 0.5) ,
(select APPROX_QUANTILES(nsf_share,100) as percentiles_nsf from frontiers_forecasting.funders_forecast_2020 where ai_share > 0.5) ,
(select APPROX_QUANTILES(nih_share,100) as percentiles_nih from frontiers_forecasting.funders_forecast_2020 where ai_share > 0.5) ,
(select APPROX_QUANTILES(erc_share,100) as percentiles_erc from frontiers_forecasting.funders_forecast_2020 where ai_share > 0.5) ,
(select APPROX_QUANTILES(ec_share,100) as percentiles_ec from frontiers_forecasting.funders_forecast_2020 where ai_share > 0.5) ,
(select APPROX_QUANTILES(eu_share,100) as percentiles_eu from frontiers_forecasting.funders_forecast_2020 where ai_share > 0.5) ,
(select APPROX_QUANTILES(jap_sps_share,100) as percentiles_jap_sps from frontiers_forecasting.funders_forecast_2020 where ai_share > 0.5) where ai_share > 0.5
)
)

select * from perc_tab

/* funded high grwoth papers */

select
ROUND(sum(ec_share * NP)) as EC,
ROUND(sum(erc_share * NP)) as ERC,
ROUND(sum(jap_sps_share * NP)) as JPS,
ROUND(sum(nih_share * NP)) as NIH,
ROUND(sum(nnsf_china_share * NP)) as NNSF,
ROUND(sum(nsf_share * NP)) as NSF,
from frontiers_forecasting.funders_forecast_2020 where x_growth = 1