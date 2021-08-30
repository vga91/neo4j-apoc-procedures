import apoc.util.TestUtil;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.reflections.Reflections;
import org.reflections.scanners.MemberUsageScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.Utils;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static apoc.ApocConfig.APOC_UUID_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static org.junit.Assert.assertFalse;

/**
 * @author ab-larus
 * @since 05.09.18
 */
public class DocsTest {
    
    // todo - fare in modo che ..docs/asciidoc/modules/ROOT/pages/transaction/index.adoc venga fatto ogni volta

    public static final String GENERATED_DOCUMENTATION_DIR = "../docs/asciidoc/modules/ROOT/examples/generated-documentation";
    public static final String GENERATED_PARTIALS_DOCUMENTATION_DIR = "../docs/asciidoc/modules/ROOT/partials/generated-documentation";
    public static final String GENERATED_OVERVIEW_DIR = "../docs/asciidoc/modules/ROOT/pages/overview";
    public static final String TRANSACTION_FILE = "../docs/asciidoc/modules/ROOT/pages/transaction/index.adoc";
    
    private static Reflections reflections = null;
    
    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.auth_enabled, true)
            .withSetting(GraphDatabaseSettings.procedure_unrestricted, Collections.singletonList("apoc.*"));

    public static void testMethod() {
        System.out.println("testing");
    }
    
    private static final Set<String> procs = new TreeSet<>();
    
    private void searchProcedures(Member member, Reflections reflections) {
//        List<Method> methodList = new ArrayList<>();
        String name;
        try {
            if (member instanceof Constructor) {
                name = Utils.name((Constructor) member);
            } else if (member instanceof Method) {
                name = Utils.name((Method) member);
            } else {
                name = Utils.name((Field) member);
            }
        } catch (Exception e) {
            // todo - vedere se entra
            name = "";
            System.out.println("DocsTest.searchProcedures");
        }

        // currently we cannot use directly org.reflections.Reflections.getMethodUsage() because doesn't recognize lambda function
        reflections.getStore().get(MemberUsageScanner.class, name).forEach(methodUsed -> {

            methodUsed = methodUsed.replaceAll("\\$\\d+", "").replaceAll("lambda\\$", "");
            
            final int indexOf = methodUsed.indexOf("(");
            final String substring = methodUsed.substring(0, indexOf);
            final String[] split = substring.split("\\.");
            final String collect = Arrays.stream(Arrays.copyOf(split, split.length - 1)).collect(Collectors.joining("."));

            
            try {
                Arrays.stream(Class.forName(collect).getDeclaredMethods()).filter(item -> item.getName().equals(split[split.length - 1]) && !item.equals(member)).forEach(item -> {
                    final Procedure annotationProc = item.getAnnotation(Procedure.class);
                    final UserFunction annotationFun = item.getAnnotation(UserFunction.class);
                    if (annotationProc != null) {
                        getCompleteName(item, annotationProc.value(), annotationProc.name());
                    } else if(annotationFun != null) {
                        getCompleteName(item, annotationFun.value(), annotationFun.name());
                    } else {
                        // to match e.g. java.util.concurrent.Callable
                        if (Arrays.stream(item.getDeclaringClass().getInterfaces()).anyMatch(i -> i.getAnnotation(FunctionalInterface.class) != null)) {
                            final Constructor<?>[] constructors = item.getDeclaringClass().getConstructors();
                            for (Constructor cons : constructors) {
                                searchProcedures(cons, reflections);
                            }
                        } else {
                            searchProcedures(item, reflections);
                        }
                    }
                });
            } catch (ClassNotFoundException e) {  }
        });

//        methodList.forEach(method1 -> searchProcedures(method1, reflections));
    }

    private void getCompleteName(Method item, String value, String name) {
        if (!value.equals("")) {
            procs.add(value);
        } else if (!name.equals("")) {
            procs.add(name);
        } else {
            procs.add(item.getDeclaringClass().getPackageName() + "." + item.getName());
        }
    }

    @Before
    public void setUp() throws Exception {

        // todo - foreach searchProcedures. List.of
        Method method = Class.forName(GraphDatabaseService.class.getName()).getDeclaredMethod("beginTx");
        Method method1 = Class.forName(GraphDatabaseService.class.getName()).getDeclaredMethod("beginTx", long.class, TimeUnit.class);
        Method method2 = Class.forName(Session.class.getName()).getDeclaredMethod("beginTransaction");
        Method method3 = Class.forName(Session.class.getName()).getDeclaredMethod("beginTransaction", TransactionConfig.class);

        
//        Reflections reflections = new Reflections(new ConfigurationBuilder()
//                .forPackages("apoc")
//                .setScanners(new SubTypesScanner(true), new TypeAnnotationsScanner(), new MemberUsageScanner())
//                .filterInputsBy(input -> !input.contains("Test") && !input.endsWith("Test.class") && !input.endsWith("Result.class"))
////                .getUrls()
//        );
        reflections = new Reflections(new ConfigurationBuilder()
                .forPackages("apoc")
                .setScanners(new SubTypesScanner(false), new TypeAnnotationsScanner(), new MemberUsageScanner())
                .filterInputsBy(input -> !input.endsWith("Test.class") && !input.endsWith("Result.class") && !input.contains("$"))
        );
        
        // todo - foreach
        searchProcedures(method, reflections);
        searchProcedures(method1, reflections);
        searchProcedures(method2, reflections);
        searchProcedures(method3, reflections);

        try(FileWriter writer = new FileWriter(TRANSACTION_FILE)) {
            // header
            writer.write("[[transaction]]\n" +
                    "= List of procedures with its own transaction\n" +
                    ":description: This chapter describes the list of procedures that start their own transaction in the APOC library.\n\n" +
                    "The list of procedures that start their own transaction:\n\n");

            for (String proc : procs) {
                writer.write("* " + proc + "\n");
            }
            
            writer.write("\n\n");
//        procs.forEach(proc -> writer.write(proc));
        }
        
        
        apocConfig().setProperty(APOC_UUID_ENABLED, true);
        
//        Reflections reflections = new Reflections(new ConfigurationBuilder()
//                .forPackages("apoc")
//                .setScanners(new SubTypesScanner(false), new TypeAnnotationsScanner())
//                .filterInputsBy(input -> !input.endsWith("Test.class") && !input.endsWith("Result.class") && !input.contains("$"))
//        );
        Set<Class<?>> allClasses = allClasses(reflections);
        assertFalse(allClasses.isEmpty());

        for (Class<?> klass : allClasses) {
            if(!klass.getName().endsWith("Test")) {
                TestUtil.registerProcedure(db, klass);
            }
        }

        new File(GENERATED_DOCUMENTATION_DIR).mkdirs();
        new File(GENERATED_PARTIALS_DOCUMENTATION_DIR).mkdirs();
        new File(GENERATED_OVERVIEW_DIR).mkdirs();
    }

    @Test
    @Ignore
    public void findMissingUsageDocs() {
        Set<String> extended = readExtended();
        Map<String, String> docs = docsMapping();
        DocumentationGenerator documentationGenerator = new DocumentationGenerator(db, extended, docs);

        Set<String> deprecated = documentationGenerator.getDeprecated();

        List<DocumentationGenerator.Row> allRows = documentationGenerator.getRows();

        long done = allRows.stream().filter(row -> new File("../docs/asciidoc/modules/ROOT/partials/usage", row.getName() + ".adoc").exists()).count();

        List<DocumentationGenerator.Row> missingRows = allRows.stream()
                .sorted(Comparator.comparing(DocumentationGenerator.Row::getName))
                .filter(row -> !new File("../docs/asciidoc/modules/ROOT/partials/usage", row.getName() + ".adoc").exists())
                .collect(Collectors.toList());

        System.out.println("done = " + done);
        System.out.println("missing = " + missingRows.size()  + " (" + missingRows.stream().filter(row1 -> !deprecated.contains(row1.getName())).count() + ")");
        System.out.println("deprecated = " + deprecated.size());
        for (DocumentationGenerator.Row row : missingRows) {
            System.out.println("procedure/function = " + row.getName() + (deprecated.contains(row.getName()) ? " (deprecated)" : ""));
        }
    }

    @Test
    public void generateDocs() {
        // todo - mettere qua
        
        Set<String> extended = readExtended();
        Map<String, String> docs = docsMapping();
        DocumentationGenerator documentationGenerator = new DocumentationGenerator(db, extended, docs);

        documentationGenerator.writeAllToCsv(docs, extended);
        documentationGenerator.writeNamespaceCsvs();
        documentationGenerator.writeIndividualCsvs();

        documentationGenerator.writeNavAndOverview();

        documentationGenerator.writeProcedurePages();
        documentationGenerator.writeFunctionPages();
        
        
    }

    @NotNull
    private Set<String> readExtended() {
        Set<String> extended = new HashSet<>();
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream("extended.txt")) {
            if (stream != null) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
                String name;
                while ((name = reader.readLine()) != null) {
                    extended.add(name);
                }
            }
        } catch (IOException e) {
            // Failed to load extended file
        }
        return extended;
    }

    @NotNull
    private Map<String, String> docsMapping() {
        Map<String, String> docs = new HashMap<>();
        docs.put("apoc.path.expand", "graph-querying/expand-paths.adoc");
        docs.put("apoc.path.expandConfig", "graph-querying/expand-paths-config.adoc");
        docs.put("apoc.path.subgraphNodes", "graph-querying/expand-subgraph-nodes.adoc");
        docs.put("apoc.path.subgraphAll", "graph-querying/expand-subgraph.adoc");
        docs.put("apoc.export.cypher.*", "export/cypher.adoc");
        docs.put("apoc.export.json.*", "export/json.adoc");
        docs.put("apoc.export.csv.*", "export/csv.adoc");
        docs.put("apoc.export.graphml.*", "export/graphml.adoc");
        docs.put("apoc.graph.*", "virtual/virtual-graph.adoc");
        docs.put("apoc.gephi.*", "export/gephi.adoc");
        docs.put("apoc.load.json.*|apoc.import.json", "import/load-json.adoc");
        docs.put("apoc.load.csv", "import/load-csv.adoc");
        docs.put("apoc.create.v.*|apoc.create.virtual.*", "virtual/virtual-nodes-rels.adoc");
        docs.put("apoc.math.*|apoc.number.romanToArabic|apoc.number.arabicToRoman", "mathematical/math-functions.adoc");
        docs.put("apoc.nodes.*|apoc.node.*|apoc.any.properties|apoc.any.property|apoc.label.exists", "graph-querying/node-querying.adoc");
        docs.put("apoc.path.*", "graph-querying/path-querying.adoc");
        docs.put("apoc.util.md5|apoc.util.sha1", "misc/text-functions.adoc#text-functions-hashing");
        docs.put("apoc.mongodb.*", "database-integration/mongodb.adoc");
        docs.put("apoc.neighbors.*", "graph-querying/neighborhood.adoc");
        docs.put("apoc.monitor.*", "database-introspection/monitoring.adoc");
        docs.put("apoc.periodic.rock_n_roll", "graph-updates/periodic-execution.adoc#periodic-rock-n-roll");
        docs.put("apoc.refactor.cloneNodesWithRelationships", "graph-updates/graph-refactoring/clone-nodes.adoc");
        docs.put("apoc.refactor.merge.*", "graph-updates/graph-refactoring/merge-nodes.adoc");
        docs.put("apoc.refactor.to|apoc.refactor.from", "graph-updates/graph-refactoring/redirect-relationship.adoc");
        docs.put("apoc.refactor.invert", "graph-updates/graph-refactoring/invert-relationship.adoc");
        docs.put("apoc.refactor.setType", "graph-updates/graph-refactoring/set-relationship-type.adoc");
        docs.put("apoc.static.*", "misc/static-values.adoc");
        docs.put("apoc.spatial.*", "misc/spatial.adoc");
        docs.put("apoc.schema.*", "indexes/schema-index-operations.adoc");
        docs.put("apoc.search.node.*", "graph-querying/parallel-node-search.adoc");
        docs.put("apoc.trigger.*", "background-operations/triggers.adoc");
        docs.put("apoc.ttl.*", "graph-updates/ttl.adoc");
        docs.put("apoc.create.uuid", "graph-updates/uuid.adoc");
        docs.put("apoc.cypher.*", "cypher-execution/index.adoc");
        docs.put("apoc.hashing.*", "comparing-graphs/fingerprinting.adoc");
        docs.put("apoc.temporal.*", "temporal/temporal-conversions.adoc");
        docs.put("apoc.uuid.*", "graph-updates/uuid.adoc");
        docs.put("apoc.model.jdbc", "database-integration/database-modeling.adoc");
        docs.put("apoc.algo.*", "algorithms/path-finding-procedures.adoc");
        docs.put("apoc.atomic.*", "graph-updates/atomic-updates.adoc");
        docs.put("apoc.bolt.*", "database-integration/bolt-neo4j.adoc");
        docs.put("apoc.case|apoc.do.case|apoc.when|apoc.do.when", "cypher-execution/conditionals.adoc");
        docs.put("apoc.es.*", "database-integration/elasticsearch.adoc");
        docs.put("apoc.refactor.rename.*", "graph-updates/graph-refactoring/rename-label-type-property.adoc");
        docs.put("apoc.couchbase.*", "database-integration/couchbase.adoc");
        docs.put("apoc.create.node.*|apoc.create.setP.*|apoc.create.setRel.*|apoc.create.relationship|apoc.nodes.link|apoc.merge.*|apoc.create.remove.*", "graph-updates/data-creation.adoc");
        docs.put("apoc.custom.*", "cypher-execution/cypher-based-procedures-functions.adoc");
        docs.put("apoc.generate.*", "graph-updates/graph-generators.adoc");
        docs.put("apoc.config.*", "database-introspection/config.adoc");
        docs.put("apoc.load.jdbc.*", "database-integration/load-jdbc.adoc");
        docs.put("apoc.load.xml.*|apoc.xml.parse", "import/xml.adoc");
        docs.put("apoc.lock.*", "graph-updates/locking.adoc");
        return docs;
    }

    private Set<Class<?>> allClasses(Reflections reflections) {
//        Reflections reflections = new Reflections(new ConfigurationBuilder()
//                .forPackages("apoc")
//                .setScanners(new SubTypesScanner(false), new TypeAnnotationsScanner())
//                .filterInputsBy(input -> !input.endsWith("Test.class") && !input.endsWith("Result.class") && !input.contains("$"))
//        );

        return reflections.getSubTypesOf(Object.class);
    }

}
