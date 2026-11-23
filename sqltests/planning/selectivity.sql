-- setup:
CREATE TABLE test(a int, b int, c int, PRIMARY KEY (a, b));

CREATE INDEX test_c ON test(c);

INSERT INTO
    test (a, b, c)
VALUES
    (1, 1, 1),
    (1, 2, 2),
    (1, 3, 3),
    (1, 4, 4),
    (1, 5, 500);

-- test: exact match on indexed column preferred over unbounded pk range
EXPLAIN SELECT * FROM test WHERE a = 1 AND b > 0 AND c = 500;
/* result:
{
    "plan": 'index.Scan("test_c", [{"min": (500), "exact": true}]) | rows.Filter(a = 1) | rows.Filter(b > 0)'
}
*/

-- test: rows returned are the same regardless of the selected scan
SELECT * FROM test WHERE a = 1 AND b > 0 AND c = 500;
/* result:
{
    a: 1,
    b: 5,
    c: 500
}
*/

-- test: exact match on the pk preferred over exact match on indexed column
EXPLAIN SELECT * FROM test WHERE a = 1 AND b = 2 AND c = 500;
/* result:
{
    "plan": 'table.Scan("test", [{"min": (1, 2), "exact": true}]) | rows.Filter(c = 500)'
}
*/

-- test: pk range preferred when it is the most selective condition available
EXPLAIN SELECT * FROM test WHERE a = 1 AND b > 0;
/* result:
{
    "plan": 'table.Scan("test", [{"min": (1, 0), "exclusive": true}])'
}
*/
