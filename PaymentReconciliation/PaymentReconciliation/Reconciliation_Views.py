# Databricks notebook source
# MAGIC %sql
# MAGIC -- View: Confirmed Transactions Only
# MAGIC CREATE OR REPLACE VIEW vw_confirmed_transactions AS
# MAGIC SELECT 
# MAGIC     t.transaction_id,
# MAGIC     u.user_id,
# MAGIC     m.merchant_id,
# MAGIC     t.amount,
# MAGIC     t.txn_time,
# MAGIC     t.status,
# MAGIC     t.created_at    
# MAGIC FROM reconciliation.gold.fact_transactions t
# MAGIC JOIN reconciliation.gold.dim_users u ON t.user_id = u.user_id
# MAGIC JOIN reconciliation.gold.dim_merchants m ON t.merchant_id = m.merchant_id
# MAGIC WHERE t.status = 'SUCCESS';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_confirmed_transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View: Settled Transactions Only
# MAGIC CREATE OR REPLACE VIEW vw_settled_transactions AS
# MAGIC SELECT *
# MAGIC FROM reconciliation.gold.fact_settlements 
# MAGIC WHERE status = 'SETTLED';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_settled_transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View: Reconciliation Match Analysis
# MAGIC CREATE OR REPLACE VIEW vw_reconciliation_analysis AS
# MAGIC WITH matched_txns AS (
# MAGIC     SELECT 
# MAGIC         vct.transaction_id AS txn_id,
# MAGIC         vct.amount AS Transaction_Amount,
# MAGIC         vct.txn_time,
# MAGIC         vst.settlement_id,
# MAGIC         vst.amount AS Settlement_Amount,
# MAGIC         vst.settled_time,
# MAGIC         DATEDIFF(DAY, vct.txn_time, vst.settled_time) AS settle_delay_days,
# MAGIC         ABS(vct.amount - vst.amount) AS amount_variance
# MAGIC     FROM vw_confirmed_transactions vct
# MAGIC     LEFT JOIN vw_settled_transactions vst 
# MAGIC         ON vct.transaction_id = vst.transaction_id
# MAGIC         AND ABS(vct.amount - vst.amount) <= 0.05
# MAGIC )
# MAGIC SELECT 
# MAGIC     *,
# MAGIC     CASE 
# MAGIC         WHEN settlement_id IS NULL THEN 'NO_MATCH_FOUND'
# MAGIC         WHEN settle_delay_days > 2 THEN 'DELAYED_SETTLEMENT'
# MAGIC         WHEN amount_variance > 0.01 THEN 'AMOUNT_MISMATCH'
# MAGIC         ELSE 'MATCHED'
# MAGIC     END AS reconciliation_status
# MAGIC FROM matched_txns;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_reconciliation_analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     txn_id,
# MAGIC     settlement_id,
# MAGIC     Transaction_Amount,
# MAGIC     Settlement_Amount,
# MAGIC     settle_delay_days,
# MAGIC     amount_variance,
# MAGIC     reconciliation_status
# MAGIC FROM vw_reconciliation_analysis
# MAGIC ORDER BY reconciliation_status, txn_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     reconciliation_status,
# MAGIC     COUNT(*) as transaction_count,
# MAGIC     SUM(Transaction_Amount) as total_txn_amount,
# MAGIC     SUM(Settlement_Amount) as total_settlement_amount,
# MAGIC     SUM(amount_variance) as total_variance,
# MAGIC     AVG(settle_delay_days) as avg_delay_days,
# MAGIC     MIN(settle_delay_days) as min_delay_days,
# MAGIC     MAX(settle_delay_days) as max_delay_days
# MAGIC FROM vw_reconciliation_analysis
# MAGIC GROUP BY reconciliation_status
# MAGIC ORDER BY transaction_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     COUNT(*) as total_transactions,
# MAGIC     SUM(CASE WHEN reconciliation_status = 'MATCHED' THEN 1 ELSE 0 END) as matched_count,
# MAGIC     ROUND(
# MAGIC         CAST(SUM(CASE WHEN reconciliation_status = 'MATCHED' THEN 1 ELSE 0 END) AS FLOAT) / 
# MAGIC         CAST(COUNT(*) AS FLOAT) * 100, 2
# MAGIC     ) as match_rate_percentage,
# MAGIC     SUM(CASE WHEN reconciliation_status = 'NO_MATCH_FOUND' THEN Transaction_Amount ELSE 0 END) as unmatched_amount,
# MAGIC     SUM(CASE WHEN reconciliation_status = 'DELAYED_SETTLEMENT' THEN 1 ELSE 0 END) as delayed_count,
# MAGIC     SUM(CASE WHEN reconciliation_status = 'AMOUNT_MISMATCH' THEN 1 ELSE 0 END) as mismatch_count,
# MAGIC     AVG(CASE WHEN settlement_id IS NOT NULL THEN settle_delay_days ELSE NULL END) as avg_settlement_delay
# MAGIC FROM vw_reconciliation_analysis;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     CAST(txn_time AS DATE) as transaction_date,
# MAGIC     COUNT(*) as daily_transactions,
# MAGIC     SUM(CASE WHEN reconciliation_status = 'MATCHED' THEN 1 ELSE 0 END) as daily_matched,
# MAGIC     ROUND(
# MAGIC         CAST(SUM(CASE WHEN reconciliation_status = 'MATCHED' THEN 1 ELSE 0 END) AS FLOAT) / 
# MAGIC         CAST(COUNT(*) AS FLOAT) * 100, 2
# MAGIC     ) as daily_match_rate,
# MAGIC     SUM(Transaction_Amount) as daily_total_amount
# MAGIC FROM vw_reconciliation_analysis
# MAGIC GROUP BY CAST(txn_time AS DATE)
# MAGIC ORDER BY transaction_date DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE reconciliation.gold.reconciliation_results(
# MAGIC     transaction_id STRING,
# MAGIC     settlement_id  STRING,
# MAGIC     Transaction_Amount DECIMAL(28,2),
# MAGIC     Settlement_Amount DECIMAL(28,2),
# MAGIC     settlement_delay_days INT,
# MAGIC     reconciliation_status VARCHAR(255),
# MAGIC     amount_variance DECIMAL(28,2)
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO reconciliation.gold.reconciliation_results 
# MAGIC (transaction_id, settlement_id, Transaction_amount, Settlement_Amount, settlement_delay_days, reconciliation_status, amount_variance)
# MAGIC SELECT 
# MAGIC     txn_id,
# MAGIC     settlement_id,
# MAGIC     Transaction_Amount,
# MAGIC     Settlement_Amount,
# MAGIC     settle_delay_days,
# MAGIC     reconciliation_status,
# MAGIC     amount_variance
# MAGIC FROM vw_reconciliation_analysis;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from reconciliation.gold.reconciliation_results

# COMMAND ----------

