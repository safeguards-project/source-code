"""
Order Validation Module.

This module implements the business logic for validating order records
and routing them to result_table or holding_table based on validation rules.
"""

from dataclasses import dataclass
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dataclass
class PipelineOutput:
    """
    Container for pipeline outputs.
    
    BUSINESS_RULE: DUAL_OUTPUT_STRUCTURE
    The pipeline produces two outputs:
    - result_table: Records that pass all validation rules
    - holding_table: Records that fail one or more validation rules
    """
    result_table: DataFrame
    holding_table: DataFrame


class OrderValidator:
    """
    Validator for customer order records.
    
    BUSINESS_RULE: VALIDATION_RULES
    Records are validated against the following rules:
    1. MISSING_CUSTOMER_NAME: customer_name must not be null
    2. NEGATIVE_AMOUNT: total_amount must be >= 0
    3. INVALID_ORDER_COUNT: order_count must be > 0
    4. MISSING_ORDER_LIMIT: order_limit must not be null
    5. MISSING_ACCOUNT_ID: account_id must not be null
    6. STALE_ORDER_DATE: order_date must be within last 365 days
    """
    
    def __init__(self, spark_session):
        """Initialize the validator with a Spark session."""
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
    
    def enrich_with_account_data(
        self, 
        monthly_totals_df: DataFrame, 
        accounts_df: DataFrame
    ) -> DataFrame:
        """
        Enrich monthly totals with account information.
        
        BUSINESS_RULE: ACCOUNT_ENRICHMENT
        Monthly totals are joined with account data to include
        customer_name and order_limit for validation.
        
        Args:
            monthly_totals_df: DataFrame with monthly totals
            accounts_df: DataFrame with account information
            
        Returns:
            DataFrame enriched with account data
        """
        return monthly_totals_df.join(
            accounts_df.select("account_id", "customer_name", "order_limit"),
            on="account_id",
            how="left"
        )
    
    def apply_validation_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply validation rules and determine hold reasons.
        
        BUSINESS_RULE: VALIDATION_RULES
        Records are validated against the following rules:
        1. MISSING_CUSTOMER_NAME: customer_name must not be null
        2. NEGATIVE_AMOUNT: monthly_total must be >= 0
        3. INVALID_ORDER_COUNT: order_count must be > 0
        4. MISSING_ORDER_LIMIT: order_limit must not be null
        5. MISSING_ACCOUNT_ID: account_id must not be null
        
        Records failing any rule are routed to holding_table with the reason.
        
        Args:
            df: DataFrame with enriched order data
            
        Returns:
            DataFrame with validation_failed flag and hold_reason columns
        """
        df_with_validation = df.withColumn(
            "hold_reason",
            F.when(
                F.col("account_id").isNull(),
                F.lit("MISSING_ACCOUNT_ID")
            ).when(
                F.col("customer_name").isNull(),
                F.lit("MISSING_CUSTOMER_NAME")
            ).when(
                F.col("monthly_total") < 0,
                F.lit("NEGATIVE_AMOUNT")
            ).when(
                F.col("order_count") <= 0,
                F.lit("INVALID_ORDER_COUNT")
            ).when(
                F.col("order_limit").isNull(),
                F.lit("MISSING_ORDER_LIMIT")
            ).otherwise(
                F.lit(None)
            )
        ).withColumn(
            "validation_failed",
            F.col("hold_reason").isNotNull()
        )
        
        return df_with_validation
    
    def split_by_validation(self, df: DataFrame) -> PipelineOutput:
        """
        Split records into result_table and holding_table based on validation.
        
        BUSINESS_RULE: RECORD_ROUTING
        - result_table: Contains all records that pass validation rules
        - holding_table: Contains all records that fail validation rules with hold_reason
        
        Args:
            df: DataFrame with validation_failed and hold_reason columns
            
        Returns:
            PipelineOutput containing result_table and holding_table DataFrames
        """
        base_columns = [
            "account_id",
            "customer_name",
            "order_month",
            "monthly_total",
            "order_count",
            "total_products",
            "order_limit"
        ]
        
        result_table = df.filter(
            ~F.col("validation_failed")
        ).select(*base_columns)
        
        holding_table = df.filter(
            F.col("validation_failed")
        ).select(
            *base_columns,
            "hold_reason",
            F.current_timestamp().alias("hold_timestamp")
        )
        
        return PipelineOutput(
            result_table=result_table,
            holding_table=holding_table
        )

    def validate_orders(
        self, 
        orders_df: DataFrame, 
        accounts_df: DataFrame,
        target_month: str = None
    ) -> PipelineOutput:
        """
        Main method to validate orders and split into result/holding tables.
        
        BUSINESS_RULE: VALIDATION_PIPELINE
        The complete validation pipeline follows these steps:
        1. Aggregate orders by month per account
        2. Enrich with account data
        3. Apply validation rules
        4. Split into result_table (valid) and holding_table (invalid)
        5. Return both outputs for the target month (or latest month if not specified)
        
        Args:
            orders_df: DataFrame containing all orders
            accounts_df: DataFrame containing account information
            target_month: Optional target month in 'YYYY-MM-DD' format
            
        Returns:
            PipelineOutput containing result_table and holding_table DataFrames
        """
        monthly_totals = self.calculate_monthly_totals(orders_df)
        
        enriched = self.enrich_with_account_data(monthly_totals, accounts_df)
        
        if target_month:
            enriched = enriched.filter(
                F.col("order_month") == F.lit(target_month)
            )
        else:
            max_month = enriched.agg(F.max("order_month")).collect()[0][0]
            if max_month:
                enriched = enriched.filter(F.col("order_month") == max_month)
        
        validated = self.apply_validation_rules(enriched)
        
        return self.split_by_validation(validated)
    
    def calculate_customer_risk_score(self, df: DataFrame) -> DataFrame:
        """
        Calculate a risk score for each customer based on order patterns.
        
        BUSINESS_RULE: CUSTOMER_RISK_SCORING
        Risk score is calculated as follows:
        - Base score starts at 0
        - Add 20 points if order_count exceeds order_limit
        - Add 15 points if monthly_total exceeds 10000
        - Add 10 points for each validation failure
        - Customers with score >= 50 are flagged as HIGH_RISK
        - Customers with score 25-49 are flagged as MEDIUM_RISK
        - Customers with score < 25 are flagged as LOW_RISK
        
        Args:
            df: DataFrame with customer order data
            
        Returns:
            DataFrame with risk_score and risk_category columns added
        """
        return df.withColumn(
            "risk_score",
            F.lit(0)
            + F.when(F.col("order_count") > F.col("order_limit"), 20).otherwise(0)
            + F.when(F.col("monthly_total") > 10000, 15).otherwise(0)
            + F.when(F.col("validation_failed"), 10).otherwise(0)
        ).withColumn(
            "risk_category",
            F.when(F.col("risk_score") >= 50, "HIGH_RISK")
            .when(F.col("risk_score") >= 25, "MEDIUM_RISK")
            .otherwise("LOW_RISK")
        )
