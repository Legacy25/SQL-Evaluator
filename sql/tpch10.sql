

SELECT  
        customer.custkey, 
        customer.name, 
        customer.acctbal,
        nation.name,
        customer.address,
        customer.phone,
        customer.comment,
        SUM(lineitem.extendedprice * (1 - lineitem.discount)) AS revenue
FROM    
        customer, 
        orders,
        lineitem,
        nation
WHERE   
        customer.custkey = orders.custkey
        AND   lineitem.orderkey = orders.orderkey
        AND   orders.orderdate >= DATE('1993-10-01')
        AND   orders.orderdate < DATE('1994-01-01')
        AND   lineitem.returnflag = 'R'
        AND   customer.nationkey = nation.nationkey
GROUP BY 
        customer.custkey, customer.name, customer.acctbal, customer.phone, nation.name, customer.address, customer.comment;