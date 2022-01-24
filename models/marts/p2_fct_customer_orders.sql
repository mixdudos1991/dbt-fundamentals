with

--- Import CTEs

base_orders as (

    select * from {{source('jaffle_shop', 'orders')}}

),

base_customers as (

    select * from {{source('jaffle_shop', 'customers')}}

),

base_payment as (

    select * from {{source('stripe', 'payment')}}

),

--- Logical CTE
 
completed_payments
as (select orderid             as order_id,
        Max(created)        as payment_finalized_date,
        Sum(amount) / 100.0 as total_amount_paid
from   base_payment
where  status <> 'fail'
group  by 1),

paid_orders
as (select base_orders.id         as order_id,
        base_orders.user_id    as customer_id,
        base_orders.order_date as order_placed_at,
        base_orders.status     as order_status,
        p.total_amount_paid,
        p.payment_finalized_date,
        C.first_name           as customer_first_name,
        C.last_name            as customer_last_name
from   base_orders
        left join completed_payments p
            on base_orders.id = p.order_id
        left join base_customers C
            on base_orders.user_id = C.id),

customer_orders
as (select C.id             as customer_id,
        Min(order_date)  as first_order_date,
        Max(order_date)  as most_recent_order_date,
        Count(orders.id) as number_of_orders
from   base_customers C
        left join base_orders as Orders
            on orders.user_id = C.id
group  by 1),

--- Final CTE
final
as (select p.*,
        Row_number()
        over (
            order by p.order_id) as transaction_seq,
        Row_number()
        over (
            partition by customer_id
            order by p.order_id) as customer_sales_seq,
        case
        when c.first_order_date = p.order_placed_at then 'new'
        else 'return'
        end                      as nvsr,
        sum(p.total_amount_paid) over (
            partition by p.customer_id
            order by p.order_placed_at
        ) as customer_lifetime_value,
        c.first_order_date       as fdos        
from   paid_orders p
        left join customer_orders as c using (customer_id)
order  by order_id)

--- Simple Select Statement
select *
from   final 



