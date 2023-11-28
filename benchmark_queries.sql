SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity) AS sum_qty,
    SUM(l_extendedprice) AS sum_base_price,
    SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    AVG(l_quantity) AS avg_qty,
    AVG(l_extendedprice) AS avg_price,
    AVG(l_discount) AS avg_disc,
    COUNT(*) AS count_order
FROM
    lineitem
WHERE
    l_shipdate <= DATE_SUB(TO_DATE('1998-12-01'), 90)
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;

SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    customer
    JOIN orders ON c_custkey = o_custkey
    JOIN lineitem ON l_orderkey = o_orderkey
WHERE
    c_mktsegment = 'BUILDING'
    AND o_orderdate < TO_DATE('1995-03-15')
    AND l_shipdate > TO_DATE('1995-03-15')
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT
    10;

SELECT
    o_orderpriority,
    COUNT(*) AS order_count
FROM
    orders
WHERE
    o_orderdate >= TO_DATE('1993-07-01')
    AND o_orderdate < ADD_MONTHS(TO_DATE('1993-07-01'), 3)
    AND EXISTS (
        SELECT
            *
        FROM
            lineitem
        WHERE
            l_orderkey = o_orderkey
            AND l_commitdate < l_receiptdate
    )
GROUP BY
    o_orderpriority
ORDER BY
    o_orderpriority;

SELECT
    n_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    customer
    JOIN orders ON c_custkey = o_custkey
    JOIN lineitem ON l_orderkey = o_orderkey
    JOIN supplier ON l_suppkey = s_suppkey
    JOIN nation ON c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    JOIN region ON n_regionkey = r_regionkey
WHERE
    r_name = 'ASIA'
    AND o_orderdate >= TO_DATE('1994-01-01')
    AND o_orderdate < ADD_MONTHS(TO_DATE('1994-01-01'), 12)
GROUP BY
    n_name
ORDER BY
    revenue DESC;

SELECT
    SUM(l_extendedprice * l_discount) AS revenue
FROM
    lineitem
WHERE
    l_shipdate >= TO_DATE('1994-01-01')
    AND l_shipdate < ADD_MONTHS(TO_DATE('1994-01-01'), 12)
    AND l_discount BETWEEN 0.05
    AND 0.070001
    AND l_quantity < 24;

SELECT
    supp_nation,
    cust_nation,
    l_year,
    SUM(volume) AS revenue
FROM
    (
        SELECT
            n1.n_name AS supp_nation,
            n2.n_name AS cust_nation,
            YEAR(l_shipdate) AS l_year,
            l_extendedprice * (1 - l_discount) AS volume
        FROM
            supplier
            JOIN lineitem ON s_suppkey = l_suppkey
            JOIN orders ON o_orderkey = l_orderkey
            JOIN customer ON c_custkey = o_custkey
            JOIN nation n1 ON s_nationkey = n1.n_nationkey
            JOIN nation n2 ON c_nationkey = n2.n_nationkey
        WHERE
            (
                (
                    n1.n_name = 'FRANCE'
                    AND n2.n_name = 'GERMANY'
                )
                OR (
                    n1.n_name = 'GERMANY'
                    AND n2.n_name = 'FRANCE'
                )
            )
            AND l_shipdate BETWEEN TO_DATE('1995-01-01')
            AND TO_DATE('1996-12-31')
    ) AS shipping
GROUP BY
    supp_nation,
    cust_nation,
    l_year
ORDER BY
    supp_nation,
    cust_nation,
    l_year;

SELECT
    o_year,
    SUM(
        CASE
            WHEN nation = 'BRAZIL' THEN volume
            ELSE 0
        END
    ) / SUM(volume) AS mkt_share
FROM
    (
        SELECT
            YEAR(o_orderdate) AS o_year,
            l_extendedprice * (1 - l_discount) AS volume,
            n2.n_name AS nation
        FROM
            part
            JOIN lineitem ON p_partkey = l_partkey
            JOIN supplier ON s_suppkey = l_suppkey
            JOIN orders ON l_orderkey = o_orderkey
            JOIN customer ON o_custkey = c_custkey
            JOIN nation n1 ON c_nationkey = n1.n_nationkey
            JOIN region ON n1.n_regionkey = region.r_regionkey
            JOIN nation n2 ON s_nationkey = n2.n_nationkey
        WHERE
            region.r_name = 'AMERICA'
            AND o_orderdate BETWEEN TO_DATE('1995-01-01')
            AND TO_DATE('1996-12-31')
            AND p_type = 'ECONOMY ANODIZED STEEL'
    ) AS all_nations
GROUP BY
    o_year
ORDER BY
    o_year;

SELECT
    c_custkey,
    c_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM
    customer
    JOIN orders ON c_custkey = o_custkey
    JOIN lineitem ON l_orderkey = o_orderkey
    JOIN nation ON c_nationkey = n_nationkey
WHERE
    o_orderdate >= TO_DATE('1993-10-01')
    AND o_orderdate < ADD_MONTHS(TO_DATE('1993-10-01'), 3)
    AND l_returnflag = 'R'
GROUP BY
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
ORDER BY
    revenue DESC
LIMIT
    20;

SELECT
    ps_partkey,
    SUM(ps_supplycost * ps_availqty) AS value
FROM
    partsupp
    JOIN supplier ON ps_suppkey = s_suppkey
    JOIN nation ON s_nationkey = n_nationkey
WHERE
    n_name = 'GERMANY'
GROUP BY
    ps_partkey
HAVING
    SUM(ps_supplycost * ps_availqty) > (
        SELECT
            SUM(ps_supplycost * ps_availqty) * 0.0001
        FROM
            partsupp
            JOIN supplier ON ps_suppkey = s_suppkey
            JOIN nation ON s_nationkey = n_nationkey
        WHERE
            n_name = 'GERMANY'
    )
ORDER BY
    value DESC
LIMIT
    10;

SELECT
    l_shipmode,
    SUM(
        CASE
            WHEN o_orderpriority = '1-URGENT'
            OR o_orderpriority = '2-HIGH' THEN 1
            ELSE 0
        END
    ) AS high_line_count,
    SUM(
        CASE
            WHEN o_orderpriority <> '1-URGENT'
            AND o_orderpriority <> '2-HIGH' THEN 1
            ELSE 0
        END
    ) AS low_line_count
FROM
    orders
    JOIN lineitem ON o_orderkey = l_orderkey
WHERE
    l_shipmode IN ('MAIL', 'SHIP')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= TO_DATE('1994-01-01')
    AND l_receiptdate < ADD_MONTHS(TO_DATE('1994-01-01'), 12)
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode;

SELECT
    c_count,
    COUNT(*) AS custdist
FROM
    (
        SELECT
            c_custkey,
            COUNT(o_orderkey) AS c_count
        FROM
            (
                SELECT
                    *
                FROM
                    customer
                    LEFT OUTER JOIN orders ON c_custkey = o_custkey
                    AND o_comment NOT LIKE '%special%requests%'
            ) AS c_customer
        GROUP BY
            c_custkey
    ) AS c_orders
GROUP BY
    c_count
ORDER BY
    custdist DESC,
    c_count DESC;

SELECT
    100.00 * SUM(
        CASE
            WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount)
            ELSE 0
        END
    ) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM
    lineitem
    JOIN part ON l_partkey = p_partkey
WHERE
    l_shipdate >= TO_DATE('1995-09-01')
    AND l_shipdate < ADD_MONTHS(TO_DATE('1995-09-01'), 1);

SELECT
    p_brand,
    p_type,
    p_size,
    COUNT(DISTINCT ps_suppkey) AS supplier_cnt
FROM
    partsupp
    JOIN part ON p_partkey = ps_partkey
WHERE
    p_brand <> 'Brand#45'
    AND p_type NOT LIKE 'MEDIUM POLISHED%'
    AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND ps_suppkey NOT IN (
        SELECT
            s_suppkey
        FROM
            supplier
        WHERE
            s_comment LIKE '%Customer%Complaints%'
    )
GROUP BY
    p_brand,
    p_type,
    p_size
ORDER BY
    supplier_cnt DESC,
    p_brand,
    p_type,
    p_size;

SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    SUM(l_quantity) AS total_quantity
FROM
    customer
    JOIN orders ON c_custkey = o_custkey
    JOIN lineitem ON o_orderkey = l_orderkey
WHERE
    o_orderkey IN (
        SELECT
            l_orderkey
        FROM
            lineitem
        GROUP BY
            l_orderkey
        HAVING
            SUM(l_quantity) > 300
    )
GROUP BY
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
ORDER BY
    o_totalprice DESC,
    o_orderdate
LIMIT
    100;

SELECT
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    lineitem,
    part
WHERE
    (
        p_partkey = l_partkey
        AND p_brand = 'Brand#12'
        AND p_container IN (
            'SM CASE',
            'SM BOX',
            'SM PACK',
            'SM PKG'
        )
        AND l_quantity >= 1
        AND l_quantity <= 1 + 10
        AND p_size BETWEEN 1
        AND 5
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR (
        p_partkey = l_partkey
        AND p_brand = 'Brand#23'
        AND p_container IN (
            'MED BAG',
            'MED BOX',
            'MED PKG',
            'MED PACK'
        )
        AND l_quantity >= 10
        AND l_quantity <= 10 + 10
        AND p_size BETWEEN 1
        AND 10
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR (
        p_partkey = l_partkey
        AND p_brand = 'Brand#34'
        AND p_container IN (
            'LG CASE',
            'LG BOX',
            'LG PACK',
            'LG PKG'
        )
        AND l_quantity >= 20
        AND l_quantity <= 20 + 10
        AND p_size BETWEEN 1
        AND 15
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    );

SELECT
    s_name,
    s_address
FROM
    supplier
    JOIN nation ON s_nationkey = n_nationkey
WHERE
    s_suppkey IN (
        SELECT
            ps_suppkey
        FROM
            partsupp
        WHERE
            ps_partkey IN (
                SELECT
                    p_partkey
                FROM
                    part
                WHERE
                    p_name LIKE 'forest%'
            )
            AND ps_availqty > (
                SELECT
                    0.5 * SUM(l_quantity)
                FROM
                    lineitem
                WHERE
                    l_partkey = partsupp.ps_partkey
                    AND l_suppkey = partsupp.ps_suppkey
                    AND l_shipdate >= TO_DATE('1994-01-01')
                    AND l_shipdate < ADD_MONTHS(TO_DATE('1994-01-01'), 12)
            )
    )
    AND n_name = 'CANADA'
ORDER BY
    s_name;
