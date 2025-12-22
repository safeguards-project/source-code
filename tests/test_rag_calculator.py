"""
Unit tests for Order Validator module.

Tests the business logic for validation rules and routing to result/holding tables.
"""

import pytest
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import Row


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for testing."""
    return SparkSession.builder \
        .appName("TestOrderValidator") \
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
def sample_orders(spark):
    """Create sample orders DataFrame."""
    data = [
        Row(order_id="ORD001", account_id="ACC001", order_date=date(2024, 12, 1), total_amount=1000.0, product_count=5, status="COMPLETED"),
        Row(order_id="ORD002", account_id="ACC001", order_date=date(2024, 12, 10), total_amount=1500.0, product_count=7, status="COMPLETED"),
        Row(order_id="ORD003", account_id="ACC002", order_date=date(2024, 12, 5), total_amount=800.0, product_count=3, status="COMPLETED"),
    ]
    return spark.createDataFrame(data)


class TestMonthlyAggregation:
    """
    Test monthly aggregation logic.
    
    BUSINESS_RULE: MONTHLY_AGGREGATION
    Orders should be correctly aggregated by account and month.
    """
    
    def test_monthly_totals_calculation(self, spark):
        """Test that monthly totals are calculated correctly."""
        from src.pyspark.rag_calculator import OrderValidator
        
        orders_data = [
            Row(order_id="ORD001", account_id="ACC001", order_date=date(2024, 12, 1), total_amount=100.0, product_count=1, status="COMPLETED"),
            Row(order_id="ORD002", account_id="ACC001", order_date=date(2024, 12, 15), total_amount=200.0, product_count=2, status="COMPLETED"),
            Row(order_id="ORD003", account_id="ACC001", order_date=date(2024, 12, 20), total_amount=300.0, product_count=3, status="COMPLETED"),
        ]
        orders_df = spark.createDataFrame(orders_data)
        
        validator = OrderValidator(spark)
        result = validator.calculate_monthly_totals(orders_df)
        
        result_row = result.collect()[0]
        assert result_row["monthly_total"] == 600.0
        assert result_row["order_count"] == 3
        assert result_row["total_products"] == 6


class TestValidationRules:
    """
    Test validation rules logic.
    
    BUSINESS_RULE: VALIDATION_RULES
    Records are validated and routed to holding_table if they fail:
    - MISSING_ACCOUNT_ID: account_id is null
    - MISSING_CUSTOMER_NAME: customer_name is null
    - NEGATIVE_AMOUNT: monthly_total < 0
    - INVALID_ORDER_COUNT: order_count <= 0
    - MISSING_ORDER_LIMIT: order_limit is null
    """
    
    def test_valid_records_go_to_result_table(self, spark, sample_accounts, sample_orders):
        """Test that valid records go to result_table, not holding_table."""
        from src.pyspark.rag_calculator import OrderValidator
        
        validator = OrderValidator(spark)
        output = validator.validate_orders(sample_orders, sample_accounts, "2024-12-01")
        
        assert output.result_table.count() > 0
        assert output.holding_table.count() == 0
    
    def test_missing_customer_name_routed_to_holding(self, spark):
        """Test that records with missing customer_name go to holding_table."""
        from src.pyspark.rag_calculator import OrderValidator
        
        accounts_data = [
            Row(account_id="ACC001", customer_name=None, order_limit=10),
        ]
        accounts_df = spark.createDataFrame(accounts_data)
        
        orders_data = [
            Row(order_id="ORD001", account_id="ACC001", order_date=date(2024, 12, 5), total_amount=1000.0, product_count=5, status="COMPLETED"),
        ]
        orders_df = spark.createDataFrame(orders_data)
        
        validator = OrderValidator(spark)
        output = validator.validate_orders(orders_df, accounts_df, "2024-12-01")
        
        assert output.result_table.count() == 0
        assert output.holding_table.count() == 1
        holding_row = output.holding_table.collect()[0]
        assert holding_row["hold_reason"] == "MISSING_CUSTOMER_NAME"
    
    def test_missing_order_limit_routed_to_holding(self, spark):
        """Test that records with missing order_limit go to holding_table."""
        from src.pyspark.rag_calculator import OrderValidator
        
        accounts_data = [
            Row(account_id="ACC001", customer_name="Test Corp", order_limit=None),
        ]
        accounts_df = spark.createDataFrame(accounts_data)
        
        orders_data = [
            Row(order_id="ORD001", account_id="ACC001", order_date=date(2024, 12, 5), total_amount=1000.0, product_count=5, status="COMPLETED"),
        ]
        orders_df = spark.createDataFrame(orders_data)
        
        validator = OrderValidator(spark)
        output = validator.validate_orders(orders_df, accounts_df, "2024-12-01")
        
        assert output.result_table.count() == 0
        assert output.holding_table.count() == 1
        holding_row = output.holding_table.collect()[0]
        assert holding_row["hold_reason"] == "MISSING_ORDER_LIMIT"
    
    def test_holding_table_has_hold_timestamp(self, spark):
        """Test that holding_table records include hold_timestamp."""
        from src.pyspark.rag_calculator import OrderValidator
        
        accounts_data = [
            Row(account_id="ACC001", customer_name=None, order_limit=10),
        ]
        accounts_df = spark.createDataFrame(accounts_data)
        
        orders_data = [
            Row(order_id="ORD001", account_id="ACC001", order_date=date(2024, 12, 5), total_amount=1000.0, product_count=5, status="COMPLETED"),
        ]
        orders_df = spark.createDataFrame(orders_data)
        
        validator = OrderValidator(spark)
        output = validator.validate_orders(orders_df, accounts_df, "2024-12-01")
        
        holding_row = output.holding_table.collect()[0]
        assert holding_row["hold_timestamp"] is not None


class TestResultTableOutput:
    """
    Test result_table output structure.
    
    BUSINESS_RULE: RESULT_TABLE_OUTPUT
    result_table should contain valid records with correct columns.
    """
    
    def test_result_table_has_expected_columns(self, spark, sample_accounts, sample_orders):
        """Test that result_table has all expected columns."""
        from src.pyspark.rag_calculator import OrderValidator
        
        validator = OrderValidator(spark)
        output = validator.validate_orders(sample_orders, sample_accounts, "2024-12-01")
        
        expected_columns = [
            "account_id",
            "customer_name",
            "order_month",
            "monthly_total",
            "order_count",
            "total_products",
            "order_limit"
        ]
        
        for col in expected_columns:
            assert col in output.result_table.columns


class TestHoldingTableOutput:
    """
    Test holding_table output structure.
    
    BUSINESS_RULE: HOLDING_TABLE_OUTPUT
    holding_table should contain invalid records with hold_reason and timestamp.
    """
    
    def test_holding_table_has_expected_columns(self, spark):
        """Test that holding_table has all expected columns."""
        from src.pyspark.rag_calculator import OrderValidator
        
        accounts_data = [
            Row(account_id="ACC001", customer_name=None, order_limit=10),
        ]
        accounts_df = spark.createDataFrame(accounts_data)
        
        orders_data = [
            Row(order_id="ORD001", account_id="ACC001", order_date=date(2024, 12, 5), total_amount=1000.0, product_count=5, status="COMPLETED"),
        ]
        orders_df = spark.createDataFrame(orders_data)
        
        validator = OrderValidator(spark)
        output = validator.validate_orders(orders_df, accounts_df, "2024-12-01")
        
        expected_columns = [
            "account_id",
            "customer_name",
            "order_month",
            "monthly_total",
            "order_count",
            "total_products",
            "order_limit",
            "hold_reason",
            "hold_timestamp"
        ]
        
        for col in expected_columns:
            assert col in output.holding_table.columns


class TestMultipleAccounts:
    """Test handling of multiple accounts with mixed validation results."""
    
    def test_mixed_valid_and_invalid_records(self, spark):
        """Test that valid and invalid records are correctly split."""
        from src.pyspark.rag_calculator import OrderValidator
        
        accounts_data = [
            Row(account_id="ACC001", customer_name="Valid Corp", order_limit=10),
            Row(account_id="ACC002", customer_name=None, order_limit=5),
        ]
        accounts_df = spark.createDataFrame(accounts_data)
        
        orders_data = [
            Row(order_id="ORD001", account_id="ACC001", order_date=date(2024, 12, 5), total_amount=1000.0, product_count=5, status="COMPLETED"),
            Row(order_id="ORD002", account_id="ACC002", order_date=date(2024, 12, 5), total_amount=500.0, product_count=3, status="COMPLETED"),
        ]
        orders_df = spark.createDataFrame(orders_data)
        
        validator = OrderValidator(spark)
        output = validator.validate_orders(orders_df, accounts_df, "2024-12-01")
        
        assert output.result_table.count() == 1
        assert output.holding_table.count() == 1
        
        result_row = output.result_table.collect()[0]
        assert result_row["account_id"] == "ACC001"
        
        holding_row = output.holding_table.collect()[0]
        assert holding_row["account_id"] == "ACC002"
        assert holding_row["hold_reason"] == "MISSING_CUSTOMER_NAME"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
