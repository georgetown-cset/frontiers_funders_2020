WITH
num_obs as  (
select count(*)  as N from frontiers_forecasting.reg_data_{year}
),
pred as (
select cluster_id, (0.473 * paper_vit_std + 0.113* log_N_top250_std + 0.292 * growth_stage_std + 0.1 * cit_vit_4thr_std)
 as pred_x_gr  from frontiers_forecasting.reg_data_{year}
),
rank_pred as (
select *, IF(gr_rank < (select N from num_obs) *  0.07, 1, 0) as y_pred  from
(select cluster_id as id, ROW_NUMBER() OVER (ORDER BY pred_x_gr DESC) AS
 gr_rank from pred)
),
CSI_tab as (
select cluster_id, gr_rank, chinese_share, ai_share, N_clust, growth_per_year_since_peak, IF(growth_per_year_since_peak > 0.08,1,0) as x_growth,
 y_pred  from  (select *,  count(cluster_id) OVER() as N_clust from
frontiers_forecasting.reg_data_{year})
  s inner join rank_pred ON s.cluster_id = rank_pred.id
)

select * from CSI_tab