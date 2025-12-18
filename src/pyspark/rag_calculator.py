"""
RAG (Red/Amber/Green) Calculator Module.

This module implements the business logic for calculating RAG status
based on month-over-month order comparisons and order limit checks.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


class RAGCalculator:
    """
    Calculator for determining RAG (Red/Amber/Green) status for customer orders.
    
    BUSINESS_RULE: RAG_THRESHOLDS
    The RAG system uses the following thresholds for month-over-month comparison:
    - RED: Current month total is 50% or more higher than previous month
    - AMBER: Current month total is 30-49% higher than previous month
    - GREEN: Current month total is less than 30% higher than previous month
    """
    
    RED_THRESHOLD = 0.50
    AMBER_THRESHOLD = 0.30
    GREEN_THRESHOLD = 0.10
    
    def __init__(self, spark_session):
        """Initialize the RAG calculator with a Spark session."""
        self.spark = spark_session
    
    def calculate_monthly_totals(self, orders_df: DataFrame) -> DataFrame:
        """
        Calculate monthly order totals per account.
        
        BUSINESS_RULE: MONTHLY_AGGREGATION
        Orders are aggregated by account_id and month to calculate:
        - Total order amount for the month
        - Total number of orders for the month
        - Total product count for the month
        
        Args:
            orders_df: DataFrame containing order data
            
        Returns:
            DataFrame with monthly totals per account
        """
        return orders_df.withColumn(
            "order_month", F.date_trunc("month", F.col("order_date"))
        ).groupBy(
            "account_id", "order_month"
        ).agg(
            F.sum("total_amount").alias("monthly_total"),
            F.count("order_id").alias("order_count"),
            F.sum("product_count").alias("total_products")
        )
    
    def calculate_month_over_month_change(self, monthly_totals_df: DataFrame) -> DataFrame:
        """
        Calculate the percentage change between current and previous month.
        
        BUSINESS_RULE: MOM_COMPARISON
        For each account, compare the current month's total against the previous month's total.
        The percentage change is calculated as: (current - previous) / previous * 100
        If previous month total is zero or null, the change is considered as null.
        
        Args:
            monthly_totals_df: DataFrame with monthly totals per account
            
        Returns:
            DataFrame with month-over-month percentage change
        """
        window_spec = Window.partitionBy("account_id").orderBy("order_month")
        
        return monthly_totals_df.withColumn(
            "previous_month_total",
            F.lag("monthly_total", 1).over(window_spec)
        ).withColumn(
            "percentage_change",
            F.when(
                F.col("previous_month_total").isNull() | (F.col("previous_month_total") == 0),
                F.lit(None)
            ).otherwise(
                ((F.col("monthly_total") - F.col("previous_month_total")) 
                 / F.col("previous_month_total")) * 100
            )
        )
    
    def determine_rag_status(self, df: DataFrame) -> DataFrame:
        """
        Determine RAG status based on percentage change thresholds.
        
        BUSINESS_RULE: RAG_STATUS_DETERMINATION
        RAG status is assigned based on the following rules:
        - RED: percentage_change >= 50% (significant increase requiring attention)
        - AMBER: percentage_change >= 30% and < 50% (moderate increase, monitor closely)
        - GREEN: percentage_change < 30% (acceptable change within normal bounds)
        - NULL/No previous data: Defaults to GREEN (new customers)
        
        Args:
            df: DataFrame with percentage_change column
            
        Returns:
            DataFrame with rag_status column added
        """
        return df.withColumn(
            "rag_status",
            F.when(
                F.col("percentage_change").isNull(),
                F.lit("GREEN")
            ).when(
                F.col("percentage_change") >= (self.RED_THRESHOLD * 100),
                F.lit("RED")
            ).when(
                F.col("percentage_change") >= (self.AMBER_THRESHOLD * 100),
                F.lit("AMBER")
            ).otherwise(
                F.lit("GREEN")
            )
        )
    
    def check_order_limit(self, df: DataFrame, accounts_df: DataFrame) -> DataFrame:
        """
        Check if customer has exceeded their monthly order limit.
        
        BUSINESS_RULE: ORDER_LIMIT_CHECK
        Each account has a defined order_limit (max orders per month).
        If the current month's order_count exceeds order_limit, flag as exceeded.
        This is an additional risk indicator independent of RAG status.
        
        Args:
            df: DataFrame with order counts
            accounts_df: DataFrame with account order limits
            
        Returns:
            DataFrame with limit_exceeded flag
        """
        df_with_limits = df.join(
            accounts_df.select("account_id", "order_limit", "customer_name"),
            on="account_id",
            how="left"
        )
        
        return df_with_limits.withColumn(
            "limit_exceeded",
            F.when(
                F.col("order_count") > F.col("order_limit"),
                F.lit("YES")
            ).otherwise(
                F.lit("NO")
            )
        )
    
    def calculate_rag(
        self, 
        orders_df: DataFrame, 
        accounts_df: DataFrame,
        target_month: str = None
    ) -> DataFrame:
        """
        Main method to calculate RAG status for all accounts.
        
        BUSINESS_RULE: RAG_CALCULATION_PIPELINE
        The complete RAG calculation follows these steps:
        1. Aggregate orders by month per account
        2. Calculate month-over-month percentage change
        3. Determine RAG status based on thresholds
        4. Check order limit violations
        5. Return final status for the target month (or latest month if not specified)
        
        Args:
            orders_df: DataFrame containing all orders
            accounts_df: DataFrame containing account information
            target_month: Optional target month in 'YYYY-MM-DD' format
            
        Returns:
            DataFrame with complete RAG analysis results
        """
        monthly_totals = self.calculate_monthly_totals(orders_df)
        
        with_mom_change = self.calculate_month_over_month_change(monthly_totals)
        
        with_rag_status = self.determine_rag_status(with_mom_change)
        
        final_result = self.check_order_limit(with_rag_status, accounts_df)
        
        if target_month:
            final_result = final_result.filter(
                F.col("order_month") == F.lit(target_month)
            )
        else:
            max_month = final_result.agg(F.max("order_month")).collect()[0][0]
            final_result = final_result.filter(F.col("order_month") == max_month)
        
        return final_result.select(
            "account_id",
            "customer_name",
            F.col("monthly_total").alias("current_month_total"),
            "previous_month_total",
            "percentage_change",
            "order_count",
            "order_limit",
            "rag_status",
            "limit_exceeded"
        )
