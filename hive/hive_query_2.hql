set hive.exec.mode.local.auto=false;
set hive.exec.parallel=true;
set mapred.job.queue.name=${usecase_test2_queue_name};

use ${usecase_test2_opr_db};

drop table sample_region_ranked;

create table sample_region_ranked as
select nm_tx,concat(trim(fk_fieldabc_id),'-',trim(rgn_id)) as rgn_id,eff_dt,end_dt,fk_fieldabc_id,row_number() over (partition by fk_fieldabc_id,nm_tx order by eff_dt,
end_dt desc)
rnk from ${usecase_test2_src_db}.sample_region where hadoop_feed_key in (select max(hadoop_feed_key) from ${usecase_test2_src_db}.sample_region) and fk_fieldabc_id in
('123','156','198','456','789','741');

drop table sample_region_ranked_1;
create table sample_region_ranked_1 as select * from sample_region_ranked where rnk!=1;

drop table sample_region_ranked_not_1;
create table sample_region_ranked_not_1 as select * from sample_region_ranked where rnk!=1;

drop table sample_region_ranked_compared;
create table sample_region_ranked_compared (old_rgn_id string, new_rgn_id string,old_eff_dt string, old_end_dt string, new_eff_dt string, new_end_dt string) stored as parquet
location '${usecase_test2_opr_db_path}/sample_region_ranked_compared';

insert overwrite table sample_region_ranked_compared
select old_rgn_id,new_rgn_id,old_eff_dt,old_end_dt,new_eff_dt,new_end_dt from (
select A.rgn_id as old_rgn_id, B.rgn_id as new_rgn_id, A.eff_dt as old_eff_dt, A.end_dt as old_end_dt,B.eff_dt as new_eff_dt, B.end_dt as new_end_dt from
sample_region_ranked_not_1 A join sample_region_ranked_1 B on (A.nm_tx=B.nm_tx and A.fk_fieldabc_id=B.fk_fieldabc_id))t where new_end_dt>=old_end_dt;