"""
Customer Orders Pipeline - Main PySpark Pipeline Module.

This module orchestrates the complete customer orders processing pipeline,
including data loading, validation, and output generation to result_table
and holding_table.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from .models import AccountSchema, OrderSchema, TransactionSchema
from .rag_calculator import OrderValidator, PipelineOutput


class CustomerOrdersPipeline:
    """
    Main pipeline for processing customer orders and validating records.
    
    BUSINESS_RULE: PIPELINE_OVERVIEW
    This pipeline processes customer orders to:
    1. Load and validate account, order, and transaction data
    2. Join datasets to create a unified view
    3. Calculate monthly aggregations
    4. Apply validation rules
    5. Output result_table (valid records) and holding_table (invalid records)
    """
    
    def __init__(self, spark: SparkSession = None):
        """Initialize the pipeline with a Spark session."""
        self.spark = spark or SparkSession.builder \
            .appName("CustomerOrdersPipeline") \
            .getOrCreate()
        self.validator = OrderValidator(self.spark)
    
    def load_accounts(self, path: str) -> DataFrame:
        """
        Load account data from CSV file.
        
        BUSINESS_RULE: ACCOUNT_DATA_LOADING
        Account data is loaded with schema validation. Accounts define customer
        information and their order_limit for validation.
        
        Args:
            path: Path to accounts CSV file
            
        Returns:
            DataFrame containing validated account data
        """
        return self.spark.read.schema(AccountSchema.schema).option("header", True).csv(path)
    
    def load_orders(self, path: str) -> DataFrame:
        """
        Load order data from CSV file.
        
        BUSINESS_RULE: ORDER_DATA_LOADING
        Order data is loaded with schema validation. Only orders with status
        'COMPLETED' or 'PENDING' are considered valid.
        Orders with status 'CANCELLED' are excluded from aggregations.
        
        Args:
            path: Path to orders CSV file
            
        Returns:
            DataFrame containing validated order data
        """
        orders_df = self.spark.read.schema(OrderSchema.schema).option("header", True).csv(path)
        return orders_df.filter(
            F.col("status").isin(["COMPLETED", "PENDING"])
        )
    
    def load_transactions(self, path: str) -> DataFrame:
        """
        Load transaction data from CSV file.
        
        BUSINESS_RULE: TRANSACTION_DATA_LOADING
        Transaction data links payments to orders. Only transactions with
        status 'SUCCESS' are included in calculations.
        
        Args:
            path: Path to transactions CSV file
            
        Returns:
            DataFrame containing validated transaction data
        """
        transactions_df = self.spark.read.schema(TransactionSchema.schema).option("header", True).csv(path)
        return transactions_df.filter(F.col("status") == "SUCCESS")
    
    def join_orders_with_transactions(
        self, 
        orders_df: DataFrame, 
        transactions_df: DataFrame
    ) -> DataFrame:
        """
        Join orders with their associated transactions.
        
        BUSINESS_RULE: ORDER_TRANSACTION_JOIN
        Orders are joined with transactions to enrich order data with payment
        information. This is a left join to include orders without transactions.
        
        Args:
            orders_df: DataFrame containing orders
            transactions_df: DataFrame containing transactions
            
        Returns:
            DataFrame with orders enriched with transaction data
        """
        transaction_summary = transactions_df.groupBy("order_id").agg(
            F.sum("amount").alias("total_paid"),
            F.count("transaction_id").alias("transaction_count")
        )
        
        return orders_df.join(
            transaction_summary,
            on="order_id",
            how="left"
        ).fillna({"total_paid": 0.0, "transaction_count": 0})
    
    def run_pipeline(
        self,
        accounts_path: str,
        orders_path: str,
        transactions_path: str,
        target_month: str = None
    ) -> PipelineOutput:
        """
        Execute the complete orders processing pipeline.
        
        BUSINESS_RULE: PIPELINE_EXECUTION
        The complete pipeline:
        1. Loads all data sources with validation
        2. Filters out cancelled orders and failed transactions
        3. Calculates monthly aggregations
        4. Applies validation rules to split records
        5. Outputs result_table (valid) and holding_table (invalid records)
        
        BUSINESS_RULE: HOLDING_TABLE_ROUTING
        Records are routed to holding_table if they fail any validation rule:
        - MISSING_ACCOUNT_ID: account_id is null
        - MISSING_CUSTOMER_NAME: customer_name is null
        - NEGATIVE_AMOUNT: monthly_total < 0
        - INVALID_ORDER_COUNT: order_count <= 0
        - MISSING_ORDER_LIMIT: order_limit is null
        
        Args:
            accounts_path: Path to accounts data
            orders_path: Path to orders data
            transactions_path: Path to transactions data
            target_month: Optional specific month to analyze
            
        Returns:
            PipelineOutput with result_table and holding_table DataFrames
        """
        accounts_df = self.load_accounts(accounts_path)
        orders_df = self.load_orders(orders_path)
        transactions_df = self.load_transactions(transactions_path)
        
        enriched_orders = self.join_orders_with_transactions(orders_df, transactions_df)
        
        pipeline_output = self.validator.validate_orders(
            enriched_orders, 
            accounts_df,
            target_month
        )
        
        return pipeline_output
    
    def get_result_summary(self, result_table: DataFrame) -> dict:
        """
        Generate a summary of records in the result table.
        
        BUSINESS_RULE: RESULT_SUMMARY_GENERATION
        Provides aggregate metrics:
        - Total valid records
        - Total monthly amount
        - Total order count
        
        Args:
            result_table: DataFrame containing valid records
            
        Returns:
            Dictionary containing result summary metrics
        """
        total_records = result_table.count()
        
        if total_records == 0:
            return {
                "total_records": 0,
                "total_amount": 0.0,
                "total_orders": 0
            }
        
        agg_result = result_table.agg(
            F.sum("monthly_total").alias("total_amount"),
            F.sum("order_count").alias("total_orders")
        ).collect()[0]
        
        return {
            "total_records": total_records,
            "total_amount": agg_result["total_amount"] or 0.0,
            "total_orders": agg_result["total_orders"] or 0
        }
    
    def get_holding_summary(self, holding_table: DataFrame) -> dict:
        """
        Generate a summary of records in the holding table.
        
        BUSINESS_RULE: HOLDING_SUMMARY_GENERATION
        Provides aggregate counts of:
        - Total records held
        - Records by hold reason
        
        Args:
            holding_table: DataFrame containing held records
            
        Returns:
            Dictionary containing holding summary metrics
        """
        total_held = holding_table.count()
        
        if total_held == 0:
            return {
                "total_held": 0,
                "by_reason": {}
            }
        
        reason_counts = holding_table.groupBy("hold_reason").count().collect()
        reason_summary = {row["hold_reason"]: row["count"] for row in reason_counts}
        
        return {
            "total_held": total_held,
            "by_reason": reason_summary
        }


if __name__ == "__main__":
    pipeline = CustomerOrdersPipeline()
    
    output = pipeline.run_pipeline(
        accounts_path="data/sample/accounts.csv",
        orders_path="data/sample/orders.csv",
        transactions_path="data/sample/transactions.csv"
    )
    
    print("=== Result Table (Valid Records) ===")
    output.result_table.show()
    
    print("=== Holding Table (Invalid Records) ===")
    output.holding_table.show()
    
    result_summary = pipeline.get_result_summary(output.result_table)
    print(f"Result Summary: {result_summary}")
    
    holding_summary = pipeline.get_holding_summary(output.holding_table)
    print(f"Holding Summary: {holding_summary}")
