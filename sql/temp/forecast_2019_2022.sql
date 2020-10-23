/* base year 3 years before forecasting year */
create or  replace table frontiers_forecasting.forecast_2019_2020 as
WITH
num_obs as  (
select count(*)  as N from frontiers_forecasting.reg_data_2019
),
pred as (
select cluster_id, (0.473 * paper_vit_std + 0.113* log_N_top250_std + 0.292 * growth_stage_std + 0.1 * cit_vit_4thr_std)
 as pred_x_gr  from frontiers_forecasting.reg_data_2019
),
rank_pred as (
select *, IF(gr_rank < (select N from num_obs) *  0.02, 1, 0) as y_pred from
(select cluster_id as id, ROW_NUMBER() OVER (ORDER BY pred_x_gr DESC) AS
 gr_rank from pred)
),
CSI_tab as (
select cluster_id, paper_vit_std, growth_stage_std, log_N_top250_std, cit_vit_4thr_std, N_clust,
 y_pred  from  (select *,  count(cluster_id) OVER() as N_clust from
frontiers_forecasting.reg_data_2019 )
  s inner join rank_pred rank_pred ON s.cluster_id = rank_pred.id
)
select * from CSI_tab