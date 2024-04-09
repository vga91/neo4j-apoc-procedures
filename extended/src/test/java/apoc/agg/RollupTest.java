package apoc.agg;

import apoc.map.Maps;
import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.agg.RollupTestUtil.ANOTHER_ID;
import static apoc.agg.RollupTestUtil.CATEGORY_ID;
import static apoc.agg.RollupTestUtil.SUPPLIER_ID;
import static apoc.agg.RollupTestUtil.getCubeTripleGroupTwo;
import static apoc.agg.RollupTestUtil.getRollupTripleGroup;
import static apoc.agg.RollupTestUtil.getRollupTripleGroupTwo;
import static apoc.util.ExtendedTestUtil.assertMapEquals;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static apoc.util.Util.map;
import static org.junit.Assert.assertEquals;


public class RollupTest {
    /**
     * TODO: METTERE ESEMPIO CON ROLLUP USANDO MYSQL
     * 
     * 
     * +------------+------------+-------+----------------------+
     * | SupplierID | CategoryID | Price | Unit                 |
     * +------------+------------+-------+----------------------+
     * |          1 |          1 |    18 | 10 boxes x 20 bags   |
     * |          1 |          1 |    19 | 24 - 12 oz bottles   |
     * |          1 |          2 |    10 | 12 - 550 ml bottles  |
     * |          2 |          2 |    22 | 48 - 6 oz jars       |
     * |          2 |          2 |    21 | 36 boxes             |
     * |          3 |          2 |    25 | 12 - 8 oz jars       |
     * |          3 |          7 |    30 | 12 - 1 lb pkgs.      |
     * |          3 |          2 |    40 | 12 - 12 oz jars      |
     * |          4 |          6 |    97 | 18 - 500 g pkgs.     |
     * |          4 |          8 |    31 | 12 - 200 ml jars     |
     * |          5 |          4 |    21 | 1 kg pkg.            |
     * |          5 |          4 |    38 | 10 - 500 g pkgs.     |
     * |          6 |          8 |     6 | 2 kg box             |
     * |          6 |          7 |    23 | 40 - 100 g pkgs.     |
     * |          6 |          2 |    16 | 24 - 250 ml bottles  |
     * |          7 |          3 |    17 | 32 - 500 g boxes     |
     * |          7 |          6 |    39 | 20 - 1 kg tins       |
     * |          7 |          8 |    63 | 16 kg pkg.           |
     * |          8 |          3 |     9 | 10 boxes x 12 pieces |
     * |          8 |          3 |    81 | 30 gift boxes        |
     * |          8 |          3 |    10 | 24 pkgs. x 4 pieces  |
     * |          9 |          5 |    21 | 24 - 500 g pkgs.     |
     * |          9 |          5 |     9 | 12 - 250 g pkgs.     |
     * |         10 |          1 |     5 | 12 - 355 ml cans     |
     * |         11 |          3 |    14 | 20 - 450 g glasses   |
     * |         11 |          3 |    31 | 100 - 250 g bags     |
     * |         11 |          3 |    44 | 100 - 100 g pieces   |
     * |         12 |          7 |    46 | 25 - 825 g cans      |
     * |         12 |          6 |   124 | 50 bags x 30 sausgs. |
     * |         13 |          8 |    26 | 10 - 200 g glasses   |
     * |         14 |          4 |    13 | 12 - 100 g pkgs      |
     * |         14 |          4 |    32 | 24 - 200 g pkgs.     |
     * |         15 |          4 |     3 | 500 g                |
     * |         16 |          1 |    14 | 24 - 12 oz bottles   |
     * |         16 |          1 |    18 | 24 - 12 oz bottles   |
     * |         17 |          8 |    19 | 24 - 250 g jars      |
     * |         17 |          8 |    26 | 12 - 500 g pkgs.     |
     * |         18 |          1 |   264 | 12 - 75 cl bottles   |
     * |         18 |          1 |    18 | 750 cc per bottle    |
     * |         19 |          8 |    18 | 24 - 4 oz tins       |
     * |         19 |          8 |    10 | 12 - 12 oz cans      |
     * |         20 |          5 |    14 | 32 - 1 kg pkgs.      |
     * |         20 |          1 |    46 | 16 - 500 g tins      |
     * |         20 |          2 |    19 | 20 - 2 kg bags       |
     * |         21 |          8 |    10 | 1k pkg.              |
     * |         21 |          8 |    12 | 4 - 450 g glasses    |
     * |         22 |          3 |    10 | 10 - 4 oz boxes      |
     * |         22 |          3 |    13 | 10 pkgs.             |
     * |         23 |          3 |    20 | 24 - 50 g pkgs.      |
     * |         23 |          3 |    16 | 12 - 100 g bars      |
     * |         24 |          7 |    53 | 50 - 300 g pkgs.     |
     * |         24 |          5 |     7 | 16 - 2 kg boxes      |
     * |         24 |          6 |    33 | 48 pieces            |
     * |         25 |          6 |     7 | 16 pies              |
     * |         25 |          6 |    24 | 24 boxes x 2 pies    |
     * |         26 |          5 |    38 | 24 - 250 g pkgs.     |
     * |         26 |          5 |    20 | 24 - 250 g pkgs.     |
     * |         27 |          8 |    13 | 24 pieces            |
     * |         28 |          4 |    55 | 5 kg pkg.            |
     * |         28 |          4 |    34 | 15 - 300 g rounds    |
     * |         29 |          2 |    29 | 24 - 500 ml bottles  |
     * |         29 |          3 |    49 | 48 pies              |
     * |          7 |          2 |    44 | 15 - 625 g jars      |
     * |         12 |          5 |    33 | 20 bags x 4 pieces   |
     * |          2 |          2 |    21 | 32 - 8 oz bottles    |
     * |          2 |          2 |    17 | 24 - 8 oz jars       |
     * |         16 |          1 |    14 | 24 - 12 oz bottles   |
     * |          8 |          3 |    13 | 10 boxes x 8 pieces  |
     * |         15 |          4 |    36 | 10 kg pkg.           |
     * |          7 |          1 |    15 | 24 - 355 ml bottles  |
     * |         15 |          4 |    22 | 10 - 500 g pkgs.     |
     * |         14 |          4 |    35 | 24 - 200 g pkgs.     |
     * |         17 |          8 |    15 | 24 - 150 g jars      |
     * |          4 |          7 |    10 | 5 kg pkg.            |
     * |         12 |          1 |     8 | 24 - 0.5 l bottles   |
     * |         23 |          1 |    18 | 500 ml               |
     * |         12 |          2 |    13 | 12 boxes             |
     * |          1 |       NULL |    18 | 10 boxes x 20 bags   |
     * |       NULL |       NULL |    18 | 10 boxes x 20 bags   |
     * +------------+------------+-------+----------------------+
     * 
     * 
     * 
     */

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();


    /*
     TODO - LASCIARE QUESTO SUL COMMIT COME ESEMPIO:
      
CREATE TABLE ProductsAltro1(
    SupplierID anotherID,
    CategoryID anotherID,
    Price NUMERIC,
    floatNum FLOAT,
    anotherID anotherID
);

INSERT INTO ProductsAltro1 VALUES(1,  1,  1,  18,  0.3);
INSERT INTO ProductsAltro1 VALUES(1,  1,  0,  19,  0.5);
INSERT INTO ProductsAltro1 VALUES(1,  2,  1,  10,  0.6);
INSERT INTO ProductsAltro1 VALUES(4,  8,  0,  31,  0.6);
INSERT INTO ProductsAltro1 VALUES(5,  4,  1,  21,  0.2);
INSERT INTO ProductsAltro1 VALUES(6,  8,  1,  6,  0.5);
INSERT INTO ProductsAltro1 VALUES(6,  7,  1,  23,  0.6);
INSERT INTO ProductsAltro1 VALUES(7,  3,  1,  17,  0.7);
INSERT INTO ProductsAltro1 VALUES(7,  6,  1,  39,  0.8);
INSERT INTO ProductsAltro1 VALUES(7,  8,  1,  63,  0.9);
INSERT INTO ProductsAltro1 VALUES(8,  3,  0,  9,  0.2);
INSERT INTO ProductsAltro1 VALUES(8,  3,  1,  81,  0.5);
INSERT INTO ProductsAltro1 VALUES(9,  5,  0,  9,  0.9);
INSERT INTO ProductsAltro1 VALUES(10,  1,  1,  5,  0.2);
INSERT INTO ProductsAltro1 VALUES(11,  3,  1,  14,  0.1);
INSERT INTO ProductsAltro1 VALUES(11,  3,  0,  31,  0.2);
INSERT INTO ProductsAltro1 VALUES(11,  4,  0,  44,  0.7);
INSERT INTO ProductsAltro1 VALUES(1,  NULL,  1,  18,  0.7);
INSERT INTO ProductsAltro1 VALUES(NULL,  NULL,  0,  18,  0.6);
INSERT INTO ProductsAltro1 VALUES(NULL,  2,  0,  199,  0.8); 
     */

    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, Maps.class, Rollup.class);

        db.executeTransactionally("""
                CREATE (:Product {SupplierID: 1, CategoryID: 1, anotherID: 1, Price: 18, floatNum: 0.3}),
                       (:Product {SupplierID: 1, CategoryID: 1, anotherID: 0, Price: 19, floatNum: 0.5}),
                       (:Product {SupplierID: 1, CategoryID: 2, anotherID: 1, Price: 10, floatNum: 0.6}),
                       (:Product {SupplierID: 4, CategoryID: 8, anotherID: 0, Price: 31, floatNum: 0.6}),
                       (:Product {SupplierID: 5, CategoryID: 4, anotherID: 1, Price: 21, floatNum: 0.2}),
                       (:Product {SupplierID: 6, CategoryID: 8, anotherID: 1, Price: 6, floatNum: 0.5}),
                       (:Product {SupplierID: 6, CategoryID: 7, anotherID: 1, Price: 23, floatNum: 0.6}),
                       (:Product {SupplierID: 7, CategoryID: 3, anotherID: 1, Price: 17, floatNum: 0.7}),
                       (:Product {SupplierID: 7, CategoryID: 6, anotherID: 1, Price: 39, floatNum: 0.8}),
                       (:Product {SupplierID: 7, CategoryID: 8, anotherID: 1, Price: 63, floatNum: 0.9}),
                       (:Product {SupplierID: 8, CategoryID: 3, anotherID: 0, Price: 9, floatNum: 0.2}),
                       (:Product {SupplierID: 8, CategoryID: 3, anotherID: 1, Price: 81, floatNum: 0.5}),
                       (:Product {SupplierID: 9, CategoryID: 5, anotherID: 0, Price: 9, floatNum: 0.9}),
                       (:Product {SupplierID: 10, CategoryID: 1, anotherID: 1, Price: 5, floatNum: 0.2}),
                       (:Product {SupplierID: 11, CategoryID: 3, anotherID: 1, Price: 14, floatNum: 0.1}),
                       (:Product {SupplierID: 11, CategoryID: 3, anotherID: 0, Price: 31, floatNum: 0.2}),
                       (:Product {SupplierID: 11, CategoryID: 4, anotherID: 0, Price: 44, floatNum: 0.7}),
                       (:Product {SupplierID: 1, CategoryID: NULL, anotherID: 1, Price: 18, floatNum: 0.7}),
                       (:Product {SupplierID: NULL, CategoryID: NULL, anotherID: 0, Price: 18, floatNum: 0.6}),
                       (:Product {SupplierID: NULL, CategoryID: 2, anotherID: 0, Price: 199, floatNum: 0.8})""");
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    // similar to https://docs.oracle.com/cd/F49540_01/DOC/server.815/a68003/rollup_c.htm and MySql `WITH ROLLUP` command
    @Test
    public void testRollup() {

            List<Map> expected = getRollupTripleGroup();
        testCall(db, """
                MATCH (p:Product)
                RETURN apoc.agg.rollup(p, $groupKeys, ["Price", "floatNum"]) as data
                """,
        map("groupKeys", List.of(SUPPLIER_ID, CATEGORY_ID, ANOTHER_ID)),
        r -> {
            extracted(expected, r);
//            List<Map> data = (List<Map>) r.get("data");
//            assertEquals(expected.size(), data.size());
//            for (int i = 0; i < expected.size(); i++) {
//                assertMapEquals("Maps at index %s are not equal. Actual: %s, Expected: %s".formatted(i, expected.get(i), data.get(i)),
//                        expected.get(i),
//                        data.get(i)
//                );
//            }
        });
    }

    @Test
    public void testRollup2() {
        
        // todo -- apoc.coll.sortMulti!!

                    List<Map> expected = getRollupTripleGroupTwo();
        testCall(db, """
                MATCH (p:Product)
                RETURN apoc.agg.rollup(p, $groupKeys, ["Price", "floatNum"]) as data
                """,
                map("groupKeys", List.of(CATEGORY_ID, SUPPLIER_ID, ANOTHER_ID)),
                r -> {
                    extracted(expected, r);
                });
    }

    @Test
    public void testCube() {

                    List<Map> expected = getCubeTripleGroupTwo();
        testCall(db, """
                MATCH (p:Product)
                RETURN apoc.agg.rollup(p, $groupKeys, ["Price", "floatNum"], {cube: true}) as data
                
                """,
                map("groupKeys", List.of(CATEGORY_ID, SUPPLIER_ID, ANOTHER_ID)),
                r -> {
                    extracted(expected, r);
                });
    }

    private void extracted(List<Map> expected, Map<String, Object> r) {
        List<Map> data = (List<Map>) r.get("data");

        System.out.println("data = \n\n" + data.stream().map(Object::toString).collect(Collectors.joining("\n")));

        for (int i = 0; i < expected.size(); i++) {
            assertMapEquals("Maps at index %s are not equal. Actual: %s, Expected: %s".formatted(i, expected.get(i), data.get(i)),
                    expected.get(i),
                    data.get(i)
            );
        }
    }
}
