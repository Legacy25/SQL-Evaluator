-- [DELTA] 
--    Range: 60-120
--    Default: 90
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
        shipinstruct   CHAR(25),
        shipmode       CHAR(10),
        comment        VARCHAR(44)
    );
SELECT
  returnflag,
  linestatus,
  sum(quantity) as sum_qty,
  sum(extendedprice) as sum_base_price, sum(extendedprice*(1-discount)) as sum_disc_price, 
  sum(extendedprice*(1-discount)*(1+tax)) as sum_charge, avg(quantity) as avg_qty,
  avg(extendedprice) as avg_price,
  avg(discount) as avg_disc,
  count(*) as count_order
FROM
  lineitem
WHERE
  shipdate <= DATE('1998-09-01') -- (- interval '[DELTA]' day (3))
GROUP BY 
  returnflag, linestatus 
ORDER BY
  returnflag, linestatus;
