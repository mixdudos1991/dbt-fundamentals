{% set old_etl_relation=ref('p2_customer_orders') %} 

{% set dbt_relation=ref('p2_fct_customer_orders') %}  {{ 

audit_helper.compare_relations(
        a_relation=old_etl_relation,
        b_relation=dbt_relation,
        primary_key="order_id"
    ) }}