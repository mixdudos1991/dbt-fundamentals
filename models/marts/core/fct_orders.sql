with orders as (

    select * from {{ref('stg_orders')}}

),

customers as (

    select * from {{ref('stg_customers')}}

),

payments as (

    select order_id, sum(case when status = 'sucess' then amount end) amount
    from {{ref('stg_payments')}}
    group by order_id

)

select orders.order_id, customers.customer_id, coalesce(payments.amount, 0) as amount from orders
    left join customers using (customer_id)
    left join payments using (order_id)