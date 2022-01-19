package apoc.export.csv;

import apoc.ApocSettings;
import apoc.graph.Graphs;
import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static org.neo4j.graphdb.Label.label;


// Created to not affect ExportCsvTest results
public class ExportCsvUseTypeTest {
    protected static final long EXPECTED_NODES = 3L;
    protected static final long EXPECTED_RELS = 2L;
    protected static final long EXPECTED_PROPS = 18L;
    protected static final String ANOTHER_NODE = "AnotherNode";

    protected static final Map<String, Object> SUPER_NODE_PROPS = Map.of("one", ZonedDateTime.of(2018, 5, 10, 10, 30, 0,0, ZoneId.of("Europe/Berlin")),
            "two", OffsetTime.of(12, 2, 33, 0, ZoneOffset.of(GraphDatabaseSettings.db_temporal_timezone.defaultValue().getId())),
            "three", LocalTime.of(17, 58, 30),
            "four", LocalDateTime.of(2021, 6, 8, 0, 0, 0),
            "five", DateValue.parse("2020").asObjectCopy(),
            "six", DurationValue.parse("P14DT16H12M"),
            "seven", "2020");
    
    protected static final Map<String, Object> ANOTHER_NODE_PROPS = new HashMap<>(Map.of(
            "alpha", (short) 12,
            "beta", "qwerty".getBytes(),
            "gamma", 'A',
            "epsilon", 1,
            "zeta", 1.1F,
            "eta", 133L,
            "theta", 10.1D,
            "iota", "bar e \" bar",
            "kappa", new String[] {"un", "deux", "trois"}
    ));
    protected static final Map<String, Object> REL_PROPS = Map.of("rel", PointValue.parse("{x: 56.7, y: 12.78, crs: 'cartesian'}"));


    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, ExportCsvTest.directory.toPath().toAbsolutePath())
            .withSetting(ApocSettings.apoc_import_file_enabled, true)
            .withSetting(ApocSettings.apoc_export_file_enabled, true);


    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, ExportCSV.class, Graphs.class, ImportCsv.class);
        
        db.executeTransactionally("CREATE (n:SuperNode $superNodeProps)-[:REL_TYPE $relProps]->(m:AnotherNode), \n" +
                "(m)-[:ANOTHER_REL]->(:SuperNode:Foo:Bar {foo: 'bar'})", 
                map("superNodeProps", SUPER_NODE_PROPS, "anotherNodProps", ANOTHER_NODE_PROPS, "relProps", REL_PROPS));

        try(Transaction tx = db.beginTx()) {
            final Node node = tx.findNodes(label(ANOTHER_NODE)).next();
            // force property types
            ANOTHER_NODE_PROPS.forEach(node::setProperty);
            tx.commit();
        }
    }
}
