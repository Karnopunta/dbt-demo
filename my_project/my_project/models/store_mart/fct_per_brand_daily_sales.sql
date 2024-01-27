select
    brand_name
    , order_date
    , sum(quantity) as total_quantity
    , sum(price) as total_revenue
from {{ ref ('stg_order_details') }}