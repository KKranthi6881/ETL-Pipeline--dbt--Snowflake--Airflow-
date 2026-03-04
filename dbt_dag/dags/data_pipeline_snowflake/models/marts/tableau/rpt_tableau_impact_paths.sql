with recursive lineage_paths as (
    select
        from_node as changed_node,
        to_node as impacted_node,
        1 as depth,
        from_node || ' -> ' || to_node as impact_path
    from {{ ref('rpt_tableau_lineage_edges') }}

    union all

    select
        p.changed_node,
        e.to_node as impacted_node,
        p.depth + 1 as depth,
        p.impact_path || ' -> ' || e.to_node as impact_path
    from lineage_paths p
    join {{ ref('rpt_tableau_lineage_edges') }} e
        on p.impacted_node = e.from_node
    where p.depth < 10
)
select distinct
    changed_node,
    impacted_node,
    depth,
    impact_path,
    case
        when impacted_node like 'tableau.%' then 'dashboard'
        when impacted_node like 'rpt_tableau_%' then 'reporting_model'
        when impacted_node like 'stg_%' then 'staging_model'
        when impacted_node like 'int_%' or impacted_node like 'fact_%' then 'mart_model'
        when impacted_node like 'tpch.%' then 'source_table'
        else 'other'
    end as impacted_node_type
from lineage_paths
order by changed_node, depth, impacted_node
