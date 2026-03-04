select
    from_node,
    from_layer,
    to_node,
    to_layer,
    edge_type
from (
    select 'tpch.orders' as from_node, 'source' as from_layer, 'stg_tpch_orders' as to_node, 'staging' as to_layer, 'transformation' as edge_type
    union all
    select 'tpch.lineitem', 'source', 'stg_tpch_line_items', 'staging', 'transformation'
    union all
    select 'stg_tpch_orders', 'staging', 'int_order_items', 'intermediate', 'transformation'
    union all
    select 'stg_tpch_line_items', 'staging', 'int_order_items', 'intermediate', 'transformation'
    union all
    select 'int_order_items', 'intermediate', 'int_order_items_summary', 'mart', 'transformation'
    union all
    select 'stg_tpch_orders', 'staging', 'fact_orders', 'mart', 'transformation'
    union all
    select 'int_order_items_summary', 'mart', 'fact_orders', 'mart', 'transformation'
    union all
    select 'fact_orders', 'mart', 'rpt_tableau_orders_daily', 'reporting', 'serves'
    union all
    select 'fact_orders', 'mart', 'rpt_tableau_status_daily', 'reporting', 'serves'
    union all
    select 'fact_orders', 'mart', 'rpt_tableau_top_customers', 'reporting', 'serves'
    union all
    select 'fact_orders', 'mart', 'rpt_tableau_quality_signals_daily', 'reporting', 'serves'
    union all
    select 'rpt_tableau_orders_daily', 'reporting', 'tableau.executive_kpis', 'dashboard', 'consumed_by'
    union all
    select 'rpt_tableau_status_daily', 'reporting', 'tableau.executive_kpis', 'dashboard', 'consumed_by'
    union all
    select 'rpt_tableau_top_customers', 'reporting', 'tableau.executive_kpis', 'dashboard', 'consumed_by'
    union all
    select 'rpt_tableau_quality_signals_daily', 'reporting', 'tableau.impact_analysis', 'dashboard', 'consumed_by'
    union all
    select 'rpt_tableau_lineage_edges', 'reporting', 'tableau.lineage_explorer', 'dashboard', 'consumed_by'
    union all
    select 'rpt_tableau_impact_paths', 'reporting', 'tableau.impact_analysis', 'dashboard', 'consumed_by'
)
