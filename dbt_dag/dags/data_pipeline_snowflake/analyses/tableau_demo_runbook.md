# Tableau Demo Runbook (Source -> dbt -> Airflow -> Tableau)

## Workbook Name
`ETL Lineage & Impact Demo`

## Dashboard 1: Executive KPIs
Use model: `rpt_tableau_orders_daily`, `rpt_tableau_status_daily`, `rpt_tableau_top_customers`

Sheets:
- KPI - Net Sales (`SUM(net_sales_amount)`)
- KPI - Orders (`SUM(order_count)`)
- KPI - Avg Order Value (`AVG(avg_order_value_amount)`)
- Trend - Net Sales by Day (`order_date`, `net_sales_amount`)
- Status Mix - Orders by Status (`status_code`, `order_count`)
- Top Customers (`customer_key`, `net_sales_amount`, `net_sales_rank`)

## Dashboard 2: Pipeline Health
Use model: `rpt_tableau_quality_signals_daily`

Sheets:
- Unexpected Status Rows by Day (`unexpected_status_rows`)
- Non-positive Price Rows by Day (`non_positive_total_price_rows`)
- Positive Discount Rows by Day (`positive_discount_rows`)
- Null Customer Rows by Day (`null_customer_rows`)
- Total Checked Rows by Day (`total_rows`)

## Dashboard 3: Lineage Explorer
Use model: `rpt_tableau_lineage_edges`

Sheets:
- Lineage Edges Table (`from_node`, `to_node`, `edge_type`)
- Layer Transition Heatmap (`from_layer`, `to_layer`, `COUNT(*)`)
- Source-to-Dashboard Paths (filter `from_layer='source'`)

## Dashboard 4: Impact Analysis
Use model: `rpt_tableau_impact_paths`, `rpt_tableau_quality_signals_daily`

Sheets:
- Changed Node Selector (`changed_node`)
- Impacted Nodes (`impacted_node`, `impacted_node_type`, `depth`)
- Impact Paths (`impact_path`)
- Risk Signals on Selected Date (`unexpected_status_rows`, `positive_discount_rows`)

## Demo Storyline
1. Open Executive KPIs and show stable metrics.
2. Open Lineage Explorer to show source -> dbt models -> Tableau lineage.
3. In Impact Analysis, select `tpch.orders` and show impacted models + dashboards.
4. Explain that source-level schema/value issues surface in Pipeline Health first, then KPI dashboards.
