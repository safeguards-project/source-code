"""
Customer Orders Pipeline - Main PySpark Pipeline Module.

This module orchestrates the complete customer orders processing pipeline,
including data loading, transformation, RAG calculation, and output generation.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from .models import AccountSchema, OrderSchema, TransactionSchema
from .rag_calculator import RAGCalculator


class CustomerOrdersPipeline:
    """
    Main pipeline for processing customer orders and calculating business metrics.
    
    BUSINESS_RULE: PIPELINE_OVERVIEW
    This pipeline processes customer orders to:
    1. Load and validate account, order, and transaction data
    2. Join datasets to create a unified view
    3. Calculate monthly aggregations
    4. Determine RAG (Red/Amber/Green) status for each customer
    5. Identify customers exceeding their order limits
    """
    
    def __init__(self, spark: SparkSession = None):
        """Initialize the pipeline with a Spark session."""
        self.spark = spark or SparkSession.builder \
            .appName("CustomerOrdersPipeline") \
            .getOrCreate()
        self.rag_calculator = RAGCalculator(self.spark)
    
    def load_accounts(self, path: str) -> DataFrame:
        """
        Load account data from CSV file.
        
        BUSINESS_RULE: ACCOUNT_DATA_LOADING
        Account data is loaded with schema validation. Accounts define customer
        information and their order_limit for monthly order cap enforcement.
        
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
        'COMPLETED' or 'PENDING' are considered for RAG calculations.
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
    
    def create_customer_order_summary(
        self,
        accounts_df: DataFrame,
        orders_df: DataFrame
    ) -> DataFrame:
        """
        Create a summary of orders per customer with account details.
        
        BUSINESS_RULE: CUSTOMER_ORDER_SUMMARY
        Aggregates all orders per customer and joins with account information
        to provide a complete view of customer ordering behavior.
        
        Args:
            accounts_df: DataFrame containing account data
            orders_df: DataFrame containing order data
            
        Returns:
            DataFrame with customer order summary
        """
        order_summary = orders_df.groupBy("account_id").agg(
            F.count("order_id").alias("total_orders"),
            F.sum("total_amount").alias("total_spend"),
            F.avg("total_amount").alias("avg_order_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date")
        )
        
        return accounts_df.join(
            order_summary,
            on="account_id",
            how="left"
        )
    
    def run_rag_analysis(
        self,
        accounts_path: str,
        orders_path: str,
        transactions_path: str,
        target_month: str = None
    ) -> DataFrame:
        """
        Execute the complete RAG analysis pipeline.
        
        BUSINESS_RULE: RAG_ANALYSIS_EXECUTION
        The complete analysis pipeline:
        1. Loads all data sources with validation
        2. Filters out cancelled orders and failed transactions
        3. Calculates monthly aggregations
        4. Computes month-over-month changes
        5. Assigns RAG status based on defined thresholds
        6. Checks order limit violations
        
        RAG Thresholds:
        - RED: >= 50% increase from previous month
        - AMBER: >= 30% and < 50% increase from previous month
        - GREEN: < 30% increase from previous month
        
        Args:
            accounts_path: Path to accounts data
            orders_path: Path to orders data
            transactions_path: Path to transactions data
            target_month: Optional specific month to analyze
            
        Returns:
            DataFrame with RAG analysis results
        """
        accounts_df = self.load_accounts(accounts_path)
        orders_df = self.load_orders(orders_path)
        transactions_df = self.load_transactions(transactions_path)
        
        enriched_orders = self.join_orders_with_transactions(orders_df, transactions_df)
        
        rag_results = self.rag_calculator.calculate_rag(
            enriched_orders, 
            accounts_df,
            target_month
        )
        
        return rag_results
    
    def get_risk_summary(self, rag_results: DataFrame) -> dict:
        """
        Generate a summary of risk indicators from RAG results.
        
        BUSINESS_RULE: RISK_SUMMARY_GENERATION
        Provides aggregate counts of:
        - Accounts by RAG status (RED, AMBER, GREEN)
        - Accounts exceeding order limits
        - Total accounts analyzed
        
        Args:
            rag_results: DataFrame with RAG analysis results
            
        Returns:
            Dictionary containing risk summary metrics
        """
        rag_counts = rag_results.groupBy("rag_status").count().collect()
        rag_summary = {row["rag_status"]: row["count"] for row in rag_counts}
        
        limit_exceeded = rag_results.filter(
            F.col("limit_exceeded") == "YES"
        ).count()
        
        total_accounts = rag_results.count()
        
        return {
            "total_accounts": total_accounts,
            "red_count": rag_summary.get("RED", 0),
            "amber_count": rag_summary.get("AMBER", 0),
            "green_count": rag_summary.get("GREEN", 0),
            "limit_exceeded_count": limit_exceeded
        }


if __name__ == "__main__":
    pipeline = CustomerOrdersPipeline()
    
    results = pipeline.run_rag_analysis(
        accounts_path="data/sample/accounts.csv",
        orders_path="data/sample/orders.csv",
        transactions_path="data/sample/transactions.csv"
    )
    
    results.show()
    
    summary = pipeline.get_risk_summary(results)
    print(f"Risk Summary: {summary}")
