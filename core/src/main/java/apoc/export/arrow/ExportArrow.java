package apoc.export.arrow;

import apoc.ApocConfig;
import apoc.Pools;
import apoc.convert.Convert;
import apoc.export.csv.CsvFormat;
import apoc.export.cypher.ExportFileManager;
import apoc.export.cypher.FileManagerFactory;
import apoc.export.graphml.XmlGraphMLReader;
import apoc.export.util.ExportConfig;
import apoc.export.util.ExportUtils;
import apoc.export.util.FormatUtils;
import apoc.export.util.NodesAndRelsSubGraph;
import apoc.export.util.ProgressReporter;
import apoc.meta.Meta;
import apoc.result.ProgressInfo;
import apoc.util.JsonUtil;
import apoc.util.Util;
import com.google.common.collect.ImmutableList;
import net.minidev.json.JSONUtil;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BufferLayout;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TypeLayout;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.ApocConfig.apocConfig;
import static apoc.export.arrow.ArrowUtils.END_FIELD;
import static apoc.export.arrow.ArrowUtils.ID_FIELD;
import static apoc.export.arrow.ArrowUtils.LABELS_FIELD;
import static apoc.export.arrow.ArrowUtils.START_FIELD;
import static apoc.export.arrow.ArrowUtils.TYPE_FIELD;
//import static apoc.export.csv.CsvFormat.writeResultHeader;
import static apoc.util.Util.labelString;
import static apoc.util.Util.labelStrings;
import static org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE;

public class ExportArrow {
    public static final String DICT_PREFIX = "dict___";

    @Context
    public Transaction tx;

    @Context
    public GraphDatabaseService db;

    @Context
    public ApocConfig apocConfig;

    @Context
    public Pools pools;

    @Context
    public TerminationGuard terminationGuard;

    @Procedure
    @Description("TODO")
    public Stream<ProgressInfo> all(@Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {
        String source = String.format("database: nodes(%d), rels(%d)", Util.nodeCount(tx), Util.relCount(tx));
        return exportArrow(fileName, source, new DatabaseSubGraph(tx), config);
    }

    @Procedure
    @Description("TODO")
    public Stream<ProgressInfo> data(@Name("nodes") List<Node> nodes, @Name("rels") List<Relationship> rels, @Name("file") String fileName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {

        String source = String.format("data: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportArrow(fileName, source, new NodesAndRelsSubGraph(tx, nodes, rels), config);
    }

    @Procedure
    @Description("TODO")
    public Stream<ProgressInfo> graph(@Name("graph") Map<String,Object> graph, @Name("file") String fileName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {

        Collection<Node> nodes = (Collection<Node>) graph.get("nodes");
        Collection<Relationship> rels = (Collection<Relationship>) graph.get("relationships");
        String source = String.format("graph: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportArrow(fileName, source, new NodesAndRelsSubGraph(tx, nodes, rels), config);
    }

    @Procedure
    @Description("TODO")
    public Stream<ProgressInfo> query(@Name("query") String query, @Name("file") String fileName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        Map<String,Object> params = config == null ? Collections.emptyMap() : (Map<String,Object>)config.getOrDefault("params", Collections.emptyMap());
        Result result = tx.execute(query,params);
        String source = String.format("statement: cols(%d)", result.columns().size());
        return exportArrow(fileName, source,result,config);
    }

    // TODO - CI SONO DUE org.apache.arrow.memory.DefaultAllocationManagerFactory


    private Stream<ProgressInfo> exportArrow(@Name("file") String fileName, String source, Object data, Map<String, Object> config) throws Exception {
        // todo - parquet?

        ExportConfig exportConfig = new ExportConfig(config);
        if (StringUtils.isNotBlank(fileName)) apocConfig.checkWriteAllowed(exportConfig);
        final String format = "arrow";
        ProgressInfo progressInfo = new ProgressInfo(fileName, source, format);
        progressInfo.batchSize = exportConfig.getBatchSize();
        ProgressReporter reporter = new ProgressReporter(null, null, progressInfo);
        ArrowFormat exporter = new ArrowFormat(db);

        ExportFileManager cypherFileManager = FileManagerFactory.createFileManager(fileName, false);

        try (RootAllocator allocator = new RootAllocator()) {

                // todo - che differenza c'è con BigIntVector ??
//                final UInt8Vector nodeId = (UInt8Vector) schemaRoot.getVector("nodeId");

                if (exportConfig.streamStatements()) {

                    // todo nb: dovrebbe batchare... vedere se c'è qualche test
                    Stream<ProgressInfo> stream = ExportUtils.getProgressInfoStream(db,
                            pools.getDefaultExecutorService(),
                            terminationGuard,
                            format,
                            exportConfig,
                            reporter,
                            cypherFileManager, progressReporter -> {
                            // todo - dump(.....)
                                try {
                                    // ...
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });
//                fileWriter.writeBatch();
                } else {
                    dump(data, exportConfig, reporter, cypherFileManager, exporter, allocator, fileName);
//                    fileWriter.writeBatch();
                    return reporter.stream(); // todo - fare in modo che vengano passate cose
                }
        }
        return Stream.empty();
    }


    private <T> void allocateAfterCheckExistence(Map<String, FieldVector> vectorMap, Map<String, FieldVector> dictVectorMap, int currentIndex, RootAllocator allocator, String key, Object value, Class<T> clazz) {
//        Constructor<?> constructor = null;//FieldVector.class);
        try {
            Constructor<?> constructor = clazz.getConstructor(String.class, BufferAllocator.class);
//        } catch (NoSuchMethodException e) {
//            e.printStackTrace();
//        }
            final boolean fieldNotExists = !vectorMap.containsKey(key);

            vectorMap.putIfAbsent(key, (FieldVector) constructor.newInstance(key, allocator));
            dictVectorMap.putIfAbsent(DICT_PREFIX + key, (FieldVector) constructor.newInstance(key, allocator));

            FieldVector labelsVector = vectorMap.get(key);
            FieldVector dictLabelsVector = dictVectorMap.get(DICT_PREFIX + key);

            if (fieldNotExists) {
                labelsVector.allocateNewSafe();
                dictLabelsVector.allocateNewSafe();
            }


            if (labelsVector instanceof VarCharVector) {
                final byte[] objBytes = JsonUtil.OBJECT_MAPPER.writeValueAsBytes(value);
                ((VarCharVector) labelsVector).setSafe(currentIndex, objBytes);
                ((VarCharVector) dictLabelsVector).setSafe(currentIndex, objBytes);
            } else {
                ((UInt8Vector) labelsVector).setSafe(currentIndex, (long) value);
                ((UInt8Vector) dictLabelsVector).setSafe(currentIndex, (long) value);
            }
//            final byte[] objBytes = JsonUtil.OBJECT_MAPPER.writeValueAsString(value).getBytes();

        } catch (Exception e) {
            e.printStackTrace(); // todo - Runtime
        }
    }

    // todo - test con streamsStatements:true

    // todo - utilizzare exportConfig
    private void dump(Object valueToExport, ExportConfig exportConfig, ProgressReporter reporter, ExportFileManager printWriter, ArrowFormat exporter, RootAllocator allocator, String fileName) throws Exception {
        DictionaryProvider.MapDictionaryProvider dictProvider = new DictionaryProvider.MapDictionaryProvider();

        String importDir = apocConfig().getString("dbms.directories.import", "import");
        File file_nodes = new File(importDir, "nodes_" + fileName);
        File file_rels = new File(importDir, "edges_" + fileName);

        int batchSize = exportConfig.getBatchSize();

        // populate nodes VectorSchemaRoot
//        final UInt8Vector nodeId = new UInt8Vector(ID_FIELD, allocator);
//        nodeId.allocateNewSafe();
//        final UInt8Vector nodeIdDict = new UInt8Vector(DICT_PREFIX + ID_FIELD, allocator);
//        nodeIdDict.allocateNewSafe();
        if (valueToExport instanceof SubGraph) {
            SubGraph subGraph = (SubGraph) valueToExport;

            Map<String, FieldVector> vectorMap = new TreeMap<>();
            Map<String, FieldVector> dictVectorMap = new TreeMap<>();
//            vectorMap.put(ID_FIELD, nodeId);
//            dictVectorMap.put(DICT_PREFIX + ID_FIELD, nodeId);

            AtomicInteger indexNode = new AtomicInteger();
            subGraph.getNodes().forEach(node -> {
//                try {
                    writeNode(reporter, allocator, vectorMap, dictVectorMap, indexNode, node);
//                } catch (InvocationTargetException e) {
//                    e.printStackTrace();
//                } catch (NoSuchMethodException e) {
//                    e.printStackTrace();
//                } catch (InstantiationException e) {
//                    e.printStackTrace();
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
            });

            vectorMap.values().forEach(item -> item.setValueCount(indexNode.get()));

            AtomicLong dictIndex = new AtomicLong();
            dictVectorMap.values().forEach(item -> {
                item.setValueCount(indexNode.get());
                dictProvider.put(new Dictionary(item, new DictionaryEncoding(dictIndex.incrementAndGet(), false, null)));
            });

            AtomicLong dictIndexTwo = new AtomicLong();
            List<FieldVector> encodedVectors = vectorMap.values().stream().map(vector ->
                    (FieldVector) DictionaryEncoder.encode(vector, dictProvider.lookup(dictIndexTwo.incrementAndGet()))
            ).collect(Collectors.toList());


//            final List<FieldVector> vectors = new ArrayList<>(vectorMap.values());
            final List<Field> encodedFields = encodedVectors.stream().map(ValueVector::getField).collect(Collectors.toList());


            try (VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(encodedFields, encodedVectors);
                 FileOutputStream fd = new FileOutputStream(file_nodes);
                 ArrowFileWriter nodeFileWriter = new ArrowFileWriter(vectorSchemaRoot, dictProvider, fd.getChannel())) {
                nodeFileWriter.start();

                // -- todo: parte comune
                int currIdx = 0;
                while (currIdx < indexNode.get()) {
                    vectorSchemaRoot.setRowCount(batchSize);
                    nodeFileWriter.writeBatch();
                    currIdx += batchSize;
                }
                nodeFileWriter.end();
            }

            vectorMap.values().forEach(ValueVector::close);
            dictVectorMap.values().forEach(ValueVector::close);
//            dictProvider.close()

            // todo - check
            if(!subGraph.getRelationships().iterator().hasNext()) {
                return;
            }

            // TODO - DIZIONARIO IN RELAZIONI
            DictionaryProvider.MapDictionaryProvider dictProviderRel = new DictionaryProvider.MapDictionaryProvider();

            // -- relationships
            AtomicInteger indexRel = new AtomicInteger();


            Map<String, FieldVector> vectorRelMap = new TreeMap<>();
            Map<String, FieldVector> dictVectorRelMap = new TreeMap<>();

            subGraph.getRelationships().forEach(rel -> {
//                try {
                    writeRelationship(reporter, allocator, vectorRelMap, dictVectorRelMap, indexRel, rel);
//                } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
//                    e.printStackTrace();
//                }
            });

            vectorRelMap.values().forEach(item -> item.setValueCount(indexRel.get()));

            AtomicLong dictIndexRel = new AtomicLong();
            dictVectorRelMap.values().forEach(item -> {
                item.setValueCount(indexRel.get());
                dictProviderRel.put(new Dictionary(item, new DictionaryEncoding(dictIndexRel.incrementAndGet(), false, null)));
            });
            AtomicLong dictIndexTwoRel = new AtomicLong();
            List<FieldVector> encodedVectorsRel = vectorRelMap.values().stream().map(vector ->
                    (FieldVector) DictionaryEncoder.encode(vector, dictProviderRel.lookup(dictIndexTwoRel.incrementAndGet()))).collect(Collectors.toList()
            );
            final List<Field> fieldsRel = encodedVectorsRel.stream().map(ValueVector::getField).collect(Collectors.toList());
//            VectorSchemaRoot vectorRelSchemaRoot = new VectorSchemaRoot(fieldsRel, encodedVectorsRel);


//            DictionaryProvider.MapDictionaryProvider dictProviderRel = new DictionaryProvider.MapDictionaryProvider();
            try (VectorSchemaRoot vectorRelSchemaRoot = new VectorSchemaRoot(fieldsRel, encodedVectorsRel);
                    FileOutputStream fd = new FileOutputStream(file_rels);
                 ArrowFileWriter relFileWriter = new ArrowFileWriter(vectorRelSchemaRoot, dictProviderRel, fd.getChannel())) {
                relFileWriter.start();
                // todo - batch
                relFileWriter.writeBatch();
                relFileWriter.end();
            }

            vectorRelMap.values().forEach(ValueVector::close);
            dictVectorRelMap.values().forEach(ValueVector::close);
        }

        if (valueToExport instanceof Result) {
            File file = new File(importDir, "result_" + fileName);

            DictionaryProvider.MapDictionaryProvider dictProviderRel = new DictionaryProvider.MapDictionaryProvider();
            AtomicInteger indexRel = new AtomicInteger();

            Map<String, FieldVector> vectorRelMap = new TreeMap<>();
            Map<String, FieldVector> dictVectorRelMap = new TreeMap<>();

            // todo - da jsonFormat
            String[] header = ((Result) valueToExport).columns().toArray(new String[((Result) valueToExport).columns().size()]);
            ((Result) valueToExport).accept((row) -> {
                for (String keyName : header) {
                    Object value = row.get(keyName);
                    writeArrowResult(reporter, allocator, vectorRelMap, dictVectorRelMap, indexRel, value);
                }
                reporter.nextRow();
                return true;
            });




            vectorRelMap.values().forEach(item -> item.setValueCount(indexRel.get()));
            AtomicLong dictIndexRel = new AtomicLong();
            dictVectorRelMap.values().forEach(item -> {
                item.setValueCount(indexRel.get());
                dictProviderRel.put(new Dictionary(item, new DictionaryEncoding(dictIndexRel.incrementAndGet(), false, null)));
            });
            AtomicLong dictIndexTwoRel = new AtomicLong();
            List<FieldVector> encodedVectorsRel = vectorRelMap.values().stream().map(vector ->
                    (FieldVector) DictionaryEncoder.encode(vector, dictProviderRel.lookup(dictIndexTwoRel.incrementAndGet()))).collect(Collectors.toList()
            );
            final List<Field> fieldsRel = encodedVectorsRel.stream().map(ValueVector::getField).collect(Collectors.toList());


            try (VectorSchemaRoot vectorRelSchemaRoot = new VectorSchemaRoot(fieldsRel, encodedVectorsRel);
                 FileOutputStream fd = new FileOutputStream(file);
                 ArrowFileWriter relFileWriter = new ArrowFileWriter(vectorRelSchemaRoot, dictProviderRel, fd.getChannel())) {
                relFileWriter.start();
                // todo - batch
                relFileWriter.writeBatch();
                relFileWriter.end();
            }

            vectorRelMap.values().forEach(ValueVector::close);
            dictVectorRelMap.values().forEach(ValueVector::close);

        }
    }

    private void writeArrowResult(ProgressReporter reporter, RootAllocator allocator, Map<String, FieldVector> vectorRelMap, Map<String, FieldVector> dictVectorRelMap, AtomicInteger indexRel, Object value) {
        Meta.Types type = Meta.Types.of(value);
        switch (type) {
            case NODE:
                writeNode(reporter, allocator, vectorRelMap, dictVectorRelMap, indexRel, (Node) value);
                break;
            case RELATIONSHIP:
                writeRelationship(reporter, allocator, vectorRelMap, dictVectorRelMap, indexRel, (Relationship) value);
                break;
            case PATH:
                ((Path)value).nodes().forEach(node -> writeNode(reporter, allocator, vectorRelMap, dictVectorRelMap, indexRel, node));
                ((Path)value).relationships().forEach(relationship -> writeRelationship(reporter, allocator, vectorRelMap, dictVectorRelMap, indexRel, relationship));
//                        .collect(Collectors.toList()));
                break;
            case LIST:
                ((List) value).forEach(value1 -> {
                    try {
                        writeArrowResult(reporter, allocator, vectorRelMap, dictVectorRelMap, indexRel, value1);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                break;

//                Convert.convertToList(value).stream().map(this::writeArrowResult).collect(Collectors.toList());
            case MAP:
                ((Map<String, Object>) value).forEach((key, value1) -> {
                    try {
                        writeArrowResult(reporter, allocator, vectorRelMap, dictVectorRelMap, indexRel, value1);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                break;

            default:
                allocateAfterCheckExistence(vectorRelMap, dictVectorRelMap, indexRel.get(), allocator, ID_FIELD, value, VarCharVector.class);
        }
    }

    private void writeRelationship(ProgressReporter reporter, RootAllocator allocator, Map<String, FieldVector> vectorRelMap, Map<String, FieldVector> dictVectorRelMap, AtomicInteger indexRel, Relationship rel) {
//        try {
        final int currentIndex = indexRel.getAndIncrement();

        allocateAfterCheckExistence(vectorRelMap, dictVectorRelMap, currentIndex, allocator, START_FIELD, rel.getStartNodeId(), UInt8Vector.class);
        allocateAfterCheckExistence(vectorRelMap, dictVectorRelMap, currentIndex, allocator, END_FIELD, rel.getEndNodeId(), UInt8Vector.class);
        allocateAfterCheckExistence(vectorRelMap, dictVectorRelMap, currentIndex, allocator, TYPE_FIELD, rel.getType().name(), VarCharVector.class);

        // todo - se type è vuoto?

        final Map<String, Object> allProperties = rel.getAllProperties();
        allProperties.forEach((key, value) -> {
//                try {
            allocateAfterCheckExistence(vectorRelMap, dictVectorRelMap, currentIndex, allocator, key, value, VarCharVector.class);
//                } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
//                    e.printStackTrace();
//                }
        });

        reporter.update(0, 1, allProperties.size());
//        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
//            e.printStackTrace();
//        }
    }

    private void writeNode(ProgressReporter reporter, RootAllocator allocator, Map<String, FieldVector> vectorMap, Map<String, FieldVector> dictVectorMap, AtomicInteger indexNode, Node node) {
//        try {

            final int currentIndex = indexNode.getAndIncrement();

            // id field
            allocateAfterCheckExistence(vectorMap, dictVectorMap, currentIndex, allocator, ID_FIELD, node.getId(), UInt8Vector.class);

            Map<String, Object> allProperties = node.getAllProperties();
            allProperties.forEach((key, value) -> {
//                try {
                    allocateAfterCheckExistence(vectorMap, dictVectorMap, currentIndex, allocator, key, value, VarCharVector.class);
//                } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
//                    e.printStackTrace();
//                }
            });

            if (node.getLabels().iterator().hasNext()) {
                allocateAfterCheckExistence(vectorMap, dictVectorMap, currentIndex, allocator, LABELS_FIELD, labelString(node), VarCharVector.class);
            }

            reporter.update(1, 0, allProperties.size());
//        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
//            e.printStackTrace();
//        }
    }
}

// todo -reporter al Result