select
    sum(extendedprice*discount) as revenue
from
    lineitem
where
    shipdate >= DATE('1994-01-01')
    and shipdate < date ('1995-01-01')
    and discount >0.05 and discount<0.07 and quantity < 24;