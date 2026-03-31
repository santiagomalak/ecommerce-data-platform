# E-Commerce Data Platform — Olist Brazil

Pipeline de datos end-to-end sobre el dataset real de **Olist** (100k+ órdenes de e-commerce brasileño). Demuestra el stack completo de Analytics Engineering: ingesta → PostgreSQL → dbt → análisis SQL avanzado.

---

## Stack

| Capa | Tecnología |
|---|---|
| Base de datos | PostgreSQL 15 (Docker) |
| Ingesta | Python + SQLAlchemy + Pandas |
| Transformación | **dbt** (staging → intermediate → marts) |
| Análisis | SQL avanzado (RFM, cohorts, revenue) |
| CI/CD | GitHub Actions (dbt compile en cada push) |
| Orquestación local | Make |

---

## Arquitectura

```
data/ (CSV de Kaggle — no incluido)
    │
    ▼
ingestion/load.py         ← carga CSVs a PostgreSQL raw schema
    │
    ▼
dbt_project/
  models/
    staging/              ← limpieza y tipado (VIEWs)
      stg_orders
      stg_order_items
      stg_customers
      stg_reviews
      stg_payments
    intermediate/         ← lógica de negocio (VIEWs)
      int_orders_enriched
      int_customer_orders
    marts/                ← tablas analíticas finales (TABLEs)
      mart_rfm_segments
      mart_monthly_revenue
      mart_cohort_retention
      mart_seller_performance
    │
    ▼
analysis/                 ← queries SQL listos para BI
  01_revenue_overview.sql
  02_rfm_analysis.sql
  03_cohort_retention.sql
  04_seller_performance.sql
```

---

## Análisis implementados

### RFM Segmentation
Segmentación de clientes en 8 categorías (Champions, Loyal, At Risk, Lost, etc.) basada en Recency, Frequency y Monetary value con quintiles calculados en SQL.

### Cohort Retention
Retención mensual por cohorte de adquisición. Matriz completa de retención con mes 0 a 12. Identifica los mejores y peores cohorts.

### Monthly Revenue
Serie temporal de revenue, órdenes y clientes únicos con crecimiento MoM calculado con LAG window function.

### Seller Performance
Ranking de vendedores con tier (Platinum/Gold/Silver/Bronze), métricas de calidad de entrega, review score y detección de sellers de alto riesgo.

---

## Setup rápido

### 1. Clonar y configurar
```bash
git clone https://github.com/santiagomalak/ecommerce-data-platform.git
cd ecommerce-data-platform
python -m venv .venv && source .venv/bin/activate  # o .venv\Scripts\activate en Windows
pip install -r requirements.txt
```

### 2. Levantar PostgreSQL
```bash
docker compose up -d
```

### 3. Descargar el dataset
Descargar de [Kaggle — Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) y colocar los CSV en `data/`.

### 4. Cargar datos
```bash
python ingestion/load.py --data-dir ./data
```

### 5. Ejecutar dbt
```bash
cd dbt_project
dbt deps
dbt run
dbt test
dbt docs generate && dbt docs serve
```

---

## Dataset

- **Fuente:** [Kaggle — Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- **Órdenes:** ~100.000
- **Período:** 2016–2018
- **Tablas:** 9 (orders, items, payments, reviews, customers, sellers, products, geolocation)

---

## CI/CD

GitHub Actions corre `dbt compile` automáticamente en cada push a `main` que modifique modelos dbt. Incluye PostgreSQL como service container para validar el proyecto sin infraestructura externa.

---

**Autor:** Santiago Aragon — [Portfolio](https://santiagomalak.is-a.dev) · [LinkedIn](https://linkedin.com/in/aragonmalak)
