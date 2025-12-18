# Customer Orders Pipeline

A PySpark and SQL-based pipeline for processing customer orders and calculating RAG (Red/Amber/Green) status indicators.

## Overview

This pipeline processes customer orders, transactions, and accounts to:
- Calculate monthly order aggregations per customer
- Compare month-over-month changes
- Assign RAG status based on defined thresholds
- Identify customers exceeding order limits

## Business Rules

### RAG Thresholds

The RAG system compares current month order totals against previous month:

| Status | Threshold | Description |
|--------|-----------|-------------|
| RED | ≥ 50% increase | Significant increase requiring attention |
| AMBER | 30-49% increase | Moderate increase, monitor closely |
| GREEN | < 30% increase | Acceptable change within normal bounds |

### Order Limit Check

Each account has a defined `order_limit` (maximum orders per month). Customers exceeding this limit are flagged independently of their RAG status.

## Project Structure

```
source-code/
├── .github/workflows/ci.yml    # CI pipeline configuration
├── src/
│   ├── pyspark/                # PySpark implementation
│   │   ├── customer_orders_pipeline.py
│   │   ├── rag_calculator.py
│   │   └── models.py
│   └── sql/                    # SQL implementation
│       ├── customer_orders_pipeline.sql
│       └── rag_calculation.sql
├── data/sample/                # Sample test data
├── tests/                      # Unit tests
└── requirements.txt
```

## Installation

```bash
pip install -r requirements.txt
```

## Usage

### PySpark Pipeline

```python
from src.pyspark import CustomerOrdersPipeline

pipeline = CustomerOrdersPipeline()
results = pipeline.run_rag_analysis(
    accounts_path="data/sample/accounts.json",
    orders_path="data/sample/orders.json",
    transactions_path="data/sample/transactions.json"
)
results.show()
```

### Running Tests

```bash
pytest tests/ -v
```

## Data Schemas

### Accounts
- `account_id`: Unique account identifier
- `customer_name`: Customer name
- `order_limit`: Maximum orders allowed per month

### Orders
- `order_id`: Unique order identifier
- `account_id`: Reference to account
- `order_date`: Date of order
- `total_amount`: Order total value
- `product_count`: Number of products
- `status`: COMPLETED, PENDING, or CANCELLED

### Transactions
- `transaction_id`: Unique transaction identifier
- `order_id`: Reference to order
- `amount`: Transaction amount
- `transaction_date`: Timestamp of transaction
- `status`: SUCCESS, PENDING, or FAILED

## CI/CD

The CI pipeline validates:
- PySpark business logic against documentation
- SQL business logic against documentation
- Unit test execution with coverage

## License

Internal use only.
