# Soccer-Analytics-Data-Warehouse
Built data pipelines to collect football statistics from API-Football and load them into Oracle SQL.

# Airflow Oracle Connection Setup

This project uses **Apache Airflow** with an **Oracle Database** as the main data warehouse.  
Follow the steps below to configure the connection inside Airflow.

---

## Connection Details

- **Connection ID**: `oracle_default`  
- **Connection Type**: `oracle`  
- **Description (Optional)**: `Soccer-Analytics-Data-Warehouse Oracle Database.`  

---

## Standard Fields

| Field        | Value        |
|--------------|--------------|
| **Host**     | `oracle`     |
| **Login**    | `airflow`    |
| **Password** | `airflow`    |
| **Port**     | `1521`       |
| **Schema**   | `airflow`    |

---

## Extra Fields (JSON)

In the **Extra** section of the connection, add:

```json
{
  "service_name": "XEPDB1"
}
```

## Import variables file

```bash 
docker exec -it soccer-analytics-data-warehouse-airflow-scheduler-1 /bin/bash

airflow variables import /opt/airflow/secrets/var.json
```