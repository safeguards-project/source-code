"""
Data models and schemas for the Customer Orders Pipeline.

This module defines the PySpark schemas for accounts, orders, and transactions.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
    TimestampType,
)


class AccountSchema:
    """
    Schema definition for customer accounts.
    
    BUSINESS_RULE: ACCOUNT_STRUCTURE
    Each account has an order_limit that defines the maximum number of orders
    a customer can place per month. This limit is used in RAG calculations.
    """
    
    schema = StructType([
        StructField("account_id", StringType(), False),
        StructField("customer_name", StringType(), False),
        StructField("order_limit", IntegerType(), False),
        StructField("created_date", DateType(), True),
        StructField("status", StringType(), True),
    ])


class OrderSchema:
    """
    Schema definition for customer orders.
    
    BUSINESS_RULE: ORDER_STRUCTURE
    Orders are linked to accounts via account_id. Each order has a total_amount
    and product_count which are used in monthly aggregations for RAG calculations.
    """
    
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("account_id", StringType(), False),
        StructField("order_date", DateType(), False),
        StructField("total_amount", DoubleType(), False),
        StructField("product_count", IntegerType(), False),
        StructField("status", StringType(), True),
    ])


class TransactionSchema:
    """
    Schema definition for order transactions.
    
    BUSINESS_RULE: TRANSACTION_STRUCTURE
    Transactions are linked to orders via order_id. Each transaction represents
    a payment or refund associated with an order.
    """
    
    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("transaction_date", TimestampType(), False),
        StructField("status", StringType(), False),
    ])


class RAGResultSchema:
    """
    Schema definition for RAG calculation results.
    """
    
    schema = StructType([
        StructField("account_id", StringType(), False),
        StructField("customer_name", StringType(), False),
        StructField("current_month_total", DoubleType(), False),
        StructField("previous_month_total", DoubleType(), False),
        StructField("percentage_change", DoubleType(), True),
        StructField("order_count", IntegerType(), False),
        StructField("order_limit", IntegerType(), False),
        StructField("rag_status", StringType(), False),
        StructField("limit_exceeded", StringType(), False),
    ])
