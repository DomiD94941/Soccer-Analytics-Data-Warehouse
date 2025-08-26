# Custom Airflow SQL Task Decorator

A custom Apache Airflow decorator that simplifies SQL task creation by allowing you to write SQL queries directly in Python functions using the `@task.sql` decorator.

## Overview

This package provides a custom decorator `@task.sql` that extends Airflow's TaskFlow API to make SQL execution more intuitive. Instead of manually configuring SQL operators, you can simply return SQL strings from Python functions and let the decorator handle the execution.

## Features

- **Simple SQL Integration**: Write SQL queries as Python strings and execute them with ease
- **TaskFlow Compatible**: Works seamlessly with Airflow's TaskFlow API
- **Template Rendering**: Supports Airflow's templating system for dynamic SQL generation
- **Type Safety**: Built with proper type hints for better development experience
- **Error Handling**: Automatic validation of SQL output with helpful error messages

## Installation

```bash
# The @task.sql decorator is packaged directly into the Docker image via the provided Dockerfile
docker compose up --build -d
```

## Requirements

- Python >= 3.10
- Apache Airflow >= 2.7.0
- typing-extensions >= 4.0.0

## Usage

```python
from airflow.sdk import dag, task

@dag 
def sql_dag():

    @task.sql(
        conn_id="postgres"
    )
    def get_nb_xcoms():
        return "SELECT COUNT(*) FROM xcom"

```


## API Reference

**Parameters:**
- `python_callable`: The function that returns the SQL query (optional when used as a decorator)
- `**kwargs`: Additional arguments passed to the underlying SQL operator

**Returns:**
- A callable that can be used as an Airflow task

**Function Requirements:**
- Must return a non-empty string containing valid SQL
- Can accept any arguments (positional or keyword)
- Supports Airflow's templating system

## Error Handling

The decorator includes built-in error handling:

- **Empty SQL**: Raises `TypeError` if the function returns an empty string
- **Invalid Return Type**: Raises `TypeError` if the function doesn't return a string
- **Multiple Outputs**: Warns if `multiple_outputs=True` is specified (not supported)

## Best Practices

1. **Keep SQL readable**: Use multi-line strings for complex queries
2. **Use parameters**: Pass dynamic values through function arguments
3. **Leverage templating**: Use Airflow's context variables when appropriate
4. **Handle errors**: Implement proper error handling in your SQL functions
5. **Test locally**: Verify your SQL syntax before deploying

