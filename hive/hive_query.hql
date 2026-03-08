USE ${testusecase_opr_db};

--HBase table
DROP TABLE IF EXISTS INFO_BASE;

CREATE EXTERNAL TABLE IF NOT EXISTS INFO_BASE (KEY string, NM String, MER_PROP_IN String, BCC_CD String, CNTRY_CD String)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES("hbase.columns.mapping" = ":key,d:nm,d:mer_prop_in,d:bcc_cd,d:cntry_cd")
TBLPROPERTIES("hbase.table.name"="${testusecase_reference_hbase_tbl}");

DROP TABLE IF EXISTS test_ff_table;
CREATE TABLE test_ff_table (KEY String) STORED AS PARQUET;

INSERT OVERWRITE TABLE test_ff_table
select A.KEY
from INFO_BASE A LEFT JOIN
    (
    select TRIM(CAST(${testusecase_ref_tb_key_col} AS STRING)) AS KEY
        from ${testusecase_src_db}.${testusecase_hive_ref_tb} I
        WHERE ${testusecase_ref_tb_key_col} IS NOT NULL
            AND LENGTH (TRIM(CAST(${testusecase_ref_tb_key_col} AS STRING))) > 0
            AND cstone_feed_key in (select max(cstone_feed_key) cstone_feed_key from ${testusecase_src_db}.${testusecase_hive_ref_tb})
            AND (TRIM(${testusecase_ref_tb_mer_prop_in_col}) <> 'NON-PROP' OR
                (trim(${testusecase_ref_tb_bcc_cd_col}) IN ('090', '020') AND
                trim((${testusecase_ref_tb_mer_prop_in_col})) = 'NON-PROP'))) B
    ON A.KEY=B.KEY
    WHERE B.KEY IS NULL;

--History table
CREATE TABLE IF NOT EXISTS test_ff_table_hist(key String) PARTITIONED BY (rundate String) STORED AS PARQUET;

INSERT OVERWRITE TABLE test_ff_table_hist PARTITION (rundate)
SELECT KEY, date_format(current_date, 'dd-MM-yyyy') AS rundate from test_ff_table;

DROP TABLE IF EXISTS INFO_BASE;