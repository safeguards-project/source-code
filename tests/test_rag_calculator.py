"""
Unit tests for RAG Calculator module.

Tests the business logic for RAG status determination and order limit checks.
"""

import pytest
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for testing."""
    return SparkSession.builder \
        .appName("TestRAGCalculator") \
        .master("local[*]") \
        .getOrCreate()


@pytest.fixture
def sample_accounts(spark):
    """Create sample accounts DataFrame."""
    data = [
        Row(account_id="ACC001", customer_name="Test Corp", order_limit=10),
        Row(account_id="ACC002", customer_name="Sample Inc", order_limit=5),
        Row(account_id="ACC003", customer_name="Demo LLC", order_limit=8),
    ]
    return spark.createDataFrame(data)


@pytest.fixture
def sample_orders_same_month(spark):
    """Create orders all in the same month for testing."""
    data = [
        Row(order_id="ORD001", account_id="ACC001", order_date=date(2024, 12, 1), total_amount=1000.0, product_count=5, status="COMPLETED"),
        Row(order_id="ORD002", account_id="ACC001", order_date=date(2024, 12, 10), total_amount=1500.0, product_count=7, status="COMPLETED"),
        Row(order_id="ORD003", account_id="ACC002", order_date=date(2024, 12, 5), total_amount=800.0, product_count=3, status="COMPLETED"),
    ]
    return spark.createDataFrame(data)


@pytest.fixture
def sample_orders_two_months(spark):
    """Create orders spanning two months for MoM comparison."""
    data = [
        Row(order_id="ORD001", account_id="ACC001", order_date=date(2024, 11, 5), total_amount=1000.0, product_count=5, status="COMPLETED"),
        Row(order_id="ORD002", account_id="ACC001", order_date=date(2024, 11, 15), total_amount=1000.0, product_count=5, status="COMPLETED"),
        Row(order_id="ORD003", account_id="ACC001", order_date=date(2024, 12, 5), total_amount=3200.0, product_count=12, status="COMPLETED"),
        Row(order_id="ORD004", account_id="ACC002", order_date=date(2024, 11, 10), total_amount=1000.0, product_count=4, status="COMPLETED"),
        Row(order_id="ORD005", account_id="ACC002", order_date=date(2024, 12, 10), total_amount=1350.0, product_count=5, status="COMPLETED"),
        Row(order_id="ORD006", account_id="ACC003", order_date=date(2024, 11, 20), total_amount=1000.0, product_count=4, status="COMPLETED"),
        Row(order_id="ORD007", account_id="ACC003", order_date=date(2024, 12, 20), total_amount=1100.0, product_count=4, status="COMPLETED"),
    ]
    return spark.createDataFrame(data)


class TestRAGThresholds:
    """
    Test RAG threshold logic.
    
    BUSINESS_RULE: RAG_THRESHOLDS
    - RED: >= 50% increase
    - AMBER: >= 30% and < 50% increase
    - GREEN: < 30% increase
    """
    
    def test_red_status_50_percent_increase(self, spark, sample_accounts):
        """Test that 50%+ increase results in RED status."""
        from src.pyspark.rag_calculator import RAGCalculator
        
        orders_data = [
            Row(order_id="ORD001", account_id="ACC001", order_date=date(2024, 11, 5), total_amount=1000.0, product_count=5, status="COMPLETED"),
            Row(order_id="ORD002", account_id="ACC001", order_date=date(2024, 12, 5), total_amount=1500.0, product_count=7, status="COMPLETED"),
        ]
        orders_df = spark.createDataFrame(orders_data)
        
        calculator = RAGCalculator(spark)
        result = calculator.calculate_rag(orders_df, sample_accounts, "2024-12-01")
        
        result_row = result.collect()[0]
        assert result_row["rag_status"] == "RED"
        assert result_row["percentage_change"] == 50.0
    
    def test_amber_status_35_percent_increase(self, spark, sample_accounts):
        """Test that 30-49% increase results in AMBER status."""
        from src.pyspark.rag_calculator import RAGCalculator
        
        orders_data = [
            Row(order_id="ORD001", account_id="ACC001", order_date=date(2024, 11, 5), total_amount=1000.0, product_count=5, status="COMPLETED"),
            Row(order_id="ORD002", account_id="ACC001", order_date=date(2024, 12, 5), total_amount=1350.0, product_count=6, status="COMPLETED"),
        ]
        orders_df = spark.createDataFrame(orders_data)
        
        calculator = RAGCalculator(spark)
        result = calculator.calculate_rag(orders_df, sample_accounts, "2024-12-01")
        
        result_row = result.collect()[0]
        assert result_row["rag_status"] == "AMBER"
        assert result_row["percentage_change"] == 35.0
    
    def test_green_status_20_percent_increase(self, spark, sample_accounts):
        """Test that <30% increase results in GREEN status."""
        from src.pyspark.rag_calculator import RAGCalculator
        
        orders_data = [
            Row(order_id="ORD001", account_id="ACC001", order_date=date(2024, 11, 5), total_amount=1000.0, product_count=5, status="COMPLETED"),
            Row(order_id="ORD002", account_id="ACC001", order_date=date(2024, 12, 5), total_amount=1200.0, product_count=5, status="COMPLETED"),
        ]
        orders_df = spark.createDataFrame(orders_data)
        
        calculator = RAGCalculator(spark)
        result = calculator.calculate_rag(orders_df, sample_accounts, "2024-12-01")
        
        result_row = result.collect()[0]
        assert result_row["rag_status"] == "GREEN"
        assert result_row["percentage_change"] == 20.0
    
    def test_green_status_new_customer_no_previous_month(self, spark, sample_accounts):
        """Test that new customers with no previous month data get GREEN status."""
        from src.pyspark.rag_calculator import RAGCalculator
        
        orders_data = [
            Row(order_id="ORD001", account_id="ACC001", order_date=date(2024, 12, 5), total_amount=5000.0, product_count=20, status="COMPLETED"),
        ]
        orders_df = spark.createDataFrame(orders_data)
        
        calculator = RAGCalculator(spark)
        result = calculator.calculate_rag(orders_df, sample_accounts, "2024-12-01")
        
        result_row = result.collect()[0]
        assert result_row["rag_status"] == "GREEN"
        assert result_row["percentage_change"] is None


class TestOrderLimitCheck:
    """
    Test order limit checking logic.
    
    BUSINESS_RULE: ORDER_LIMIT_CHECK
    Flag customers who exceed their monthly order limit.
    """
    
    def test_order_limit_exceeded(self, spark, sample_accounts):
        """Test that exceeding order limit is flagged."""
        from src.pyspark.rag_calculator import RAGCalculator
        
        orders_data = [
            Row(order_id=f"ORD{i:03d}", account_id="ACC002", order_date=date(2024, 12, i+1), total_amount=100.0, product_count=1, status="COMPLETED")
            for i in range(6)
        ]
        orders_df = spark.createDataFrame(orders_data)
        
        calculator = RAGCalculator(spark)
        result = calculator.calculate_rag(orders_df, sample_accounts, "2024-12-01")
        
        result_row = result.collect()[0]
        assert result_row["limit_exceeded"] == "YES"
        assert result_row["order_count"] == 6
        assert result_row["order_limit"] == 5
    
    def test_order_limit_not_exceeded(self, spark, sample_accounts):
        """Test that staying within order limit is not flagged."""
        from src.pyspark.rag_calculator import RAGCalculator
        
        orders_data = [
            Row(order_id=f"ORD{i:03d}", account_id="ACC002", order_date=date(2024, 12, i+1), total_amount=100.0, product_count=1, status="COMPLETED")
            for i in range(4)
        ]
        orders_df = spark.createDataFrame(orders_data)
        
        calculator = RAGCalculator(spark)
        result = calculator.calculate_rag(orders_df, sample_accounts, "2024-12-01")
        
        result_row = result.collect()[0]
        assert result_row["limit_exceeded"] == "NO"
        assert result_row["order_count"] == 4


class TestMonthlyAggregation:
    """
    Test monthly aggregation logic.
    
    BUSINESS_RULE: MONTHLY_AGGREGATION
    Orders should be correctly aggregated by account and month.
    """
    
    def test_monthly_totals_calculation(self, spark):
        """Test that monthly totals are calculated correctly."""
        from src.pyspark.rag_calculator import RAGCalculator
        
        orders_data = [
            Row(order_id="ORD001", account_id="ACC001", order_date=date(2024, 12, 1), total_amount=100.0, product_count=1, status="COMPLETED"),
            Row(order_id="ORD002", account_id="ACC001", order_date=date(2024, 12, 15), total_amount=200.0, product_count=2, status="COMPLETED"),
            Row(order_id="ORD003", account_id="ACC001", order_date=date(2024, 12, 20), total_amount=300.0, product_count=3, status="COMPLETED"),
        ]
        orders_df = spark.createDataFrame(orders_data)
        
        calculator = RAGCalculator(spark)
        result = calculator.calculate_monthly_totals(orders_df)
        
        result_row = result.collect()[0]
        assert result_row["monthly_total"] == 600.0
        assert result_row["order_count"] == 3
        assert result_row["total_products"] == 6


class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def test_exactly_30_percent_is_amber(self, spark, sample_accounts):
        """Test that exactly 30% increase results in AMBER (boundary case)."""
        from src.pyspark.rag_calculator import RAGCalculator
        
        orders_data = [
            Row(order_id="ORD001", account_id="ACC001", order_date=date(2024, 11, 5), total_amount=1000.0, product_count=5, status="COMPLETED"),
            Row(order_id="ORD002", account_id="ACC001", order_date=date(2024, 12, 5), total_amount=1300.0, product_count=6, status="COMPLETED"),
        ]
        orders_df = spark.createDataFrame(orders_data)
        
        calculator = RAGCalculator(spark)
        result = calculator.calculate_rag(orders_df, sample_accounts, "2024-12-01")
        
        result_row = result.collect()[0]
        assert result_row["rag_status"] == "AMBER"
    
    def test_exactly_50_percent_is_red(self, spark, sample_accounts):
        """Test that exactly 50% increase results in RED (boundary case)."""
        from src.pyspark.rag_calculator import RAGCalculator
        
        orders_data = [
            Row(order_id="ORD001", account_id="ACC001", order_date=date(2024, 11, 5), total_amount=1000.0, product_count=5, status="COMPLETED"),
            Row(order_id="ORD002", account_id="ACC001", order_date=date(2024, 12, 5), total_amount=1500.0, product_count=7, status="COMPLETED"),
        ]
        orders_df = spark.createDataFrame(orders_data)
        
        calculator = RAGCalculator(spark)
        result = calculator.calculate_rag(orders_df, sample_accounts, "2024-12-01")
        
        result_row = result.collect()[0]
        assert result_row["rag_status"] == "RED"
    
    def test_decrease_in_orders_is_green(self, spark, sample_accounts):
        """Test that a decrease in orders results in GREEN status."""
        from src.pyspark.rag_calculator import RAGCalculator
        
        orders_data = [
            Row(order_id="ORD001", account_id="ACC001", order_date=date(2024, 11, 5), total_amount=2000.0, product_count=10, status="COMPLETED"),
            Row(order_id="ORD002", account_id="ACC001", order_date=date(2024, 12, 5), total_amount=1000.0, product_count=5, status="COMPLETED"),
        ]
        orders_df = spark.createDataFrame(orders_data)
        
        calculator = RAGCalculator(spark)
        result = calculator.calculate_rag(orders_df, sample_accounts, "2024-12-01")
        
        result_row = result.collect()[0]
        assert result_row["rag_status"] == "GREEN"
        assert result_row["percentage_change"] == -50.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
