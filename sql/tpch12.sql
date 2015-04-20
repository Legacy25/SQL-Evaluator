CREATE TABLE LINEITEM (
        orderkey       INT,
        partkey        INT,
        suppkey        INT,
        linenumber     INT,
        quantity       DECIMAL,
        extendedprice  DECIMAL,
        discount       DECIMAL,
        tax            DECIMAL,
        returnflag     CHAR(1),
        linestatus     CHAR(1),
        shipdate       DATE,
        commitdate     DATE,
        receiptdate    DATE,
        shipinstruct   VARCHAR(25),
        shipmode       VARCHAR(10),
        comment        VARCHAR(44)
    );
CREATE TABLE ORDERS (
        orderkey       INT,
        custkey        INT,
        orderstatus    CHAR(1),
        totalprice     DECIMAL,
        orderdate      DATE,
        orderpriority  VARCHAR(15),
        clerk          VARCHAR(15),
        shippriority   INT,
        comment        VARCHAR(79)
    );
select  lineitem.shipmode, 
        sum(case when orders.orderpriority = '1-URGENT'
                     or orders.orderpriority = '2-HIGH'
                   then 1
                   else 0 end) as high_line_count,
        sum(case when orders.orderpriority <> '1-URGENT'
                     and orders.orderpriority <> '2-HIGH'
                   then 1
                   else 0 end) as low_line_count
from orders, lineitem
where orders.orderkey = lineitem.orderkey
and (lineitem.shipmode='AIR' or lineitem.shipmode='MAIL' or lineitem.shipmode='TRUCK' or lineitem.shipmode='SHIP')
and lineitem.commitdate < lineitem.receiptdate
and lineitem.shipdate < lineitem.commitdate
and lineitem.receiptdate >= date('1995-03-05')
and lineitem.receiptdate < date('1996-03-05')
group by lineitem.shipmode
order by lineitem.shipmode;
