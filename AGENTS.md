# Agent Instructions for Customer Orders Pipeline

## Project Overview

This is a PySpark/SQL data pipeline that calculates RAG (Red/Amber/Green) status for customer orders.

## Commands

### Testing
```bash
pytest tests/ -v
```

### Type Checking
```bash
# No static type checking configured - PySpark uses dynamic typing
```

### Linting
```bash
# If ruff/flake8 is installed:
ruff check src/
```

## Code Conventions

### Business Rule Documentation

All business rules must be documented using the `BUSINESS_RULE:` marker pattern:

```python
"""
BUSINESS_RULE: RULE_NAME
Description of the business rule...
"""
```

This pattern is used by the CI pipeline to extract and validate business rules against documentation.

### PySpark Style

- Use DataFrame API, avoid RDDs
- Use `F.col()` for column references
- Define schemas explicitly using StructType
- Include docstrings with BUSINESS_RULE markers

### SQL Style

- Use CTEs for clarity
- Include comments with BUSINESS_RULE markers
- Use uppercase for SQL keywords
- Use snake_case for column aliases

## Key Business Rules

1. **RAG_THRESHOLDS**: RED ≥50%, AMBER 30-49%, GREEN <30%
2. **ORDER_LIMIT_CHECK**: Flag customers exceeding monthly order limit
3. **VALID_ORDERS**: Only COMPLETED and PENDING orders are included
4. **SUCCESSFUL_TRANSACTIONS**: Only SUCCESS transactions count

## File Structure

- `src/pyspark/` - PySpark implementation
- `src/sql/` - SQL implementation  
- `data/sample/` - Test data (JSON format)
- `tests/` - Pytest unit tests
