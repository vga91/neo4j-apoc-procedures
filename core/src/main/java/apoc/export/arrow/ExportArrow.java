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
import apoc.result.ByteArrayResult;
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
import org.apache.arrow.vector.ipc.ArrowWriter;
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
import static apoc.export.arrow.ArrowUtils.DICT_PREFIX;
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

                if (exportConfig.streamStatements()) {

                    // todo nb: dovrebbe batchare... vedere se c'è qualche test
                    Stream<ProgressInfo> stream = ExportUtils.getProgressInfoStream(db,
                            pools.getDefaultExecutorService(),
                            terminationGuard,
                            format,
                            exportConfig,
                            reporter,
                            cypherFileManager,
                            progressReporter -> {
                                try {
                                    dump(data, exportConfig, reporter, cypherFileManager, exporter, allocator, fileName);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                    );
//                fileWriter.writeBatch();
                } else {
                    dump(data, exportConfig, reporter, cypherFileManager, exporter, allocator, fileName);
//                    fileWriter.writeBatch();
                    return reporter.stream(); // todo - fare in modo che vengano passate cose
                }
        }
        return Stream.empty();
    }


    protected static <T> void allocateAfterCheckExistence(Map<String, FieldVector> vectorMap, int currentIndex, RootAllocator allocator, String key, Object value, Class<T> clazz) {
        try {
            Constructor<?> constructor = clazz.getConstructor(String.class, BufferAllocator.class);

            final boolean fieldNotExists = !vectorMap.containsKey(key);

            vectorMap.putIfAbsent(key, (FieldVector) constructor.newInstance(key, allocator));
//            dictVectorMap.putIfAbsent(DICT_PREFIX + key, (FieldVector) constructor.newInstance(key, allocator));

            FieldVector labelsVector = vectorMap.get(key);
//            FieldVector dictLabelsVector = dictVectorMap.get(DICT_PREFIX + key);

            if (fieldNotExists) {
                labelsVector.allocateNewSafe();
//                dictLabelsVector.allocateNewSafe();
            }

            if (labelsVector instanceof VarCharVector) {
                final byte[] objBytes = JsonUtil.OBJECT_MAPPER.writeValueAsBytes(value);
                ((VarCharVector) labelsVector).setSafe(currentIndex, objBytes);
//                ((VarCharVector) dictLabelsVector).setSafe(currentIndex, objBytes);
            } else {
                ((UInt8Vector) labelsVector).setSafe(currentIndex, (long) value);
//                ((UInt8Vector) dictLabelsVector).setSafe(currentIndex, (long) value);
            }

        } catch (Exception e) {
            throw new RuntimeException("Error during vector allocation", e);
        }
    }

    // todo - test con streamsStatements:true

    // todo - utilizzare exportConfig
    private void dump(Object valueToExport, ExportConfig exportConfig, ProgressReporter reporter, ExportFileManager printWriter, ArrowFormat exporter, RootAllocator allocator, String fileName) throws Exception {

        String importDir = apocConfig().getString("dbms.directories.import", "import");
        File file_nodes = new File(importDir, "nodes_" + fileName);
        File file_rels = new File(importDir, "edges_" + fileName);

        int batchSize = exportConfig.getBatchSize();

        if (valueToExport instanceof SubGraph) {
            SubGraph subGraph = (SubGraph) valueToExport;

            try (FileOutputStream fd = new FileOutputStream(file_nodes)) {
                DictionaryProvider.MapDictionaryProvider dictProvider = new DictionaryProvider.MapDictionaryProvider();
                AtomicInteger indexNode = new AtomicInteger();

                Map<String, FieldVector> vectorMap = new TreeMap<>();
                subGraph.getNodes().forEach(node -> {
                    writeNode(reporter, allocator, vectorMap, indexNode, node);
                    checkBatchStatusAndWriteEventually(dictProvider, fd, indexNode, vectorMap, indexNode.get() % batchSize == 0);
                });

                checkBatchStatusAndWriteEventually(dictProvider, fd, indexNode, vectorMap,indexNode.get() % batchSize != 0);
            }

            if(!subGraph.getRelationships().iterator().hasNext()) {
                return;
            }

            try (FileOutputStream fd = new FileOutputStream(file_rels)) {
                DictionaryProvider.MapDictionaryProvider dictProvider = new DictionaryProvider.MapDictionaryProvider();
                AtomicInteger index = new AtomicInteger();

                Map<String, FieldVector> vectorMap = new TreeMap<>();
                subGraph.getRelationships().forEach(relationship -> {
                    writeRelationship(reporter, allocator, vectorMap, index, relationship);
                    checkBatchStatusAndWriteEventually(dictProvider, fd, index, vectorMap, index.get() % batchSize == 0);
                });

                checkBatchStatusAndWriteEventually(dictProvider, fd, index, vectorMap,index.get() % batchSize != 0);
            }

//            DictionaryProvider.MapDictionaryProvider dictProviderRel = new DictionaryProvider.MapDictionaryProvider();
//
//            // -- relationships
//            AtomicInteger indexRel = new AtomicInteger();
//
//            Map<String, FieldVector> vectorRelMap = new TreeMap<>();
//
//            subGraph.getRelationships().forEach(rel -> writeRelationship(reporter, allocator, vectorRelMap, indexRel, rel));
//
//            final List<FieldVector> encodedVectorsRel = returnEncodedVector(dictProviderRel, vectorRelMap, indexRel);
//
////            VectorSchemaRoot.create()
//
//            try (VectorSchemaRoot schemaRoot = new VectorSchemaRoot(getFields(encodedVectorsRel), encodedVectorsRel);
//                 FileOutputStream fd = new FileOutputStream(file_rels);
//                 ArrowFileWriter writer = new ArrowFileWriter(schemaRoot, dictProviderRel, fd.getChannel())) {
//
//                writeWithBatch(batchSize, indexRel, schemaRoot, writer);
//            }
//
//            closeFieldVectors(vectorRelMap, encodedVectorsRel);
        }

        if (valueToExport instanceof Result) {
//            DictionaryProvider.MapDictionaryProvider dictProvider = new DictionaryProvider.MapDictionaryProvider();
            File file = new File(importDir, "result_" + fileName);
            try (FileOutputStream fd = new FileOutputStream(file)) {

                DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
                AtomicInteger index = new AtomicInteger();
                Map<String, FieldVector> vectorMap = new TreeMap<>();
//            Map<String, FieldVector> dictVectorRelMap = new TreeMap<>();

                // todo - da jsonFormat - riciclare in qualche modo
                String[] header = ((Result) valueToExport).columns().toArray(new String[((Result) valueToExport).columns().size()]);
                ((Result) valueToExport).accept((row) -> {
                    for (String keyName : header) {
                        Object value = row.get(keyName);
                        writeArrowResult(reporter, allocator, vectorMap, index, value, provider, batchSize, fd);
                    }
                    reporter.nextRow();
                    return true;
                });
                checkBatchStatusAndWriteEventually(provider, fd, index, vectorMap, index.get() % batchSize != 0);
//                List<FieldVector> encodedVectors = returnEncodedVector(dictProvider, vectorMap, index);

//                try (VectorSchemaRoot schemaRoot = new VectorSchemaRoot(getFields(encodedVectors), encodedVectors);
//                     ArrowFileWriter writer = new ArrowFileWriter(schemaRoot, dictProviderRel, fd.getChannel())) {
//
//                    writeWithBatch(batchSize, indexResult, schemaRoot, writer);
//                }
//                closeFieldVectors(vectorMap, encodedVectors);
            }

        }
    }

    private void checkBatchStatusAndWriteEventually(DictionaryProvider.MapDictionaryProvider dictProvider, FileOutputStream fd, AtomicInteger indexNode, Map<String, FieldVector> vectorMap, boolean b) {
        if (b) {
            List<FieldVector> encodedVectors = returnEncodedVector(dictProvider, vectorMap, indexNode);
            try {
                getCurrentBatch(dictProvider, fd, encodedVectors);
            } catch (IOException e) {
                e.printStackTrace();
            }
            closeFieldVectors(vectorMap, encodedVectors);
            vectorMap.clear();
        }
    }

    private void getCurrentBatch(DictionaryProvider.MapDictionaryProvider dictProvider, FileOutputStream fd, List<FieldVector> encodedVectors) throws IOException {
        try (VectorSchemaRoot schemaRoot = new VectorSchemaRoot(getFields(encodedVectors), encodedVectors);
             ArrowFileWriter writer = new ArrowFileWriter(schemaRoot, dictProvider, fd.getChannel())) {

            writer.start();
            writer.writeBatch();
            writer.end();
        }
    }

    protected static void closeFieldVectors(Map<String, FieldVector> vectorMap, List<FieldVector> encodedVectors) {
        vectorMap.values().forEach(ValueVector::close);
        encodedVectors.forEach(ValueVector::close);
    }

    protected static List<Field> getFields(List<FieldVector> encodedVectorsRel) {
        return encodedVectorsRel.stream().map(ValueVector::getField).collect(Collectors.toList());
    }

    protected static List<FieldVector> returnEncodedVector(DictionaryProvider.MapDictionaryProvider dictProvider, Map<String, FieldVector> vectorMap, AtomicInteger indexNode) {

        vectorMap.values().forEach(item -> item.setValueCount(indexNode.get()));

        Map<String, FieldVector> dictVectorMap = vectorMap.entrySet().stream()
                .collect(Collectors.toMap(key -> DICT_PREFIX + key, Map.Entry::getValue, (a, b) -> a, TreeMap::new));

        AtomicLong dictIndex = new AtomicLong();
        // todo - valori uguali ... ma allora ciclo direttamente vector...
        dictVectorMap.values().forEach(item -> {
            dictProvider.put(new Dictionary(item, new DictionaryEncoding(dictIndex.incrementAndGet(), false, null)));
        });

        AtomicLong dictIndexTwo = new AtomicLong();
        return vectorMap.values().stream().map(vector ->
                (FieldVector) DictionaryEncoder.encode(vector, dictProvider.lookup(dictIndexTwo.incrementAndGet()))
        ).collect(Collectors.toList());
    }

    protected static void writeWithBatch(int batchSize, AtomicInteger indexNode, VectorSchemaRoot vectorSchemaRoot, ArrowWriter writer) throws IOException {
//        batchSize = 1;


        writer.start();
//        int currIdx = 0;
//        while (currIdx < indexNode.get()) {
//            vectorSchemaRoot.allocateNew();

// todo            int toProcessItems = Math.min(this.batchSize, this.entries - i);
//            vectorSchemaRoot.setRowCount(1);
//            vectorSchemaRoot.setRowCount(currIdx + batchSize);
//            vectorSchemaRoot.setRowCount(currIdx + batchSize);

//            vectorSchemaRoot.slice(currIdx, batchSize);

            writer.writeBatch();
//            currIdx += batchSize;
//            currIdx += batchSize;

//            vectorSchemaRoot.clear();
//            vectorSchemaRoot.close();
//        }
        writer.end();
    }

    private void writeArrowResult(ProgressReporter reporter, RootAllocator allocator, Map<String, FieldVector> vectorMap, AtomicInteger index, Object value, DictionaryProvider.MapDictionaryProvider provider, int batchSize, FileOutputStream fd) {
        Meta.Types type = Meta.Types.of(value);
        switch (type) {
            case NODE:
                writeNode(reporter, allocator, vectorMap, index, (Node) value);
                checkBatchStatusAndWriteEventually(provider, fd, index, vectorMap, index.get() % batchSize == 0);
                break;
            case RELATIONSHIP:
                writeRelationship(reporter, allocator, vectorMap, index, (Relationship) value);
                checkBatchStatusAndWriteEventually(provider, fd, index, vectorMap, index.get() % batchSize == 0);
                break;
            case PATH:
                ((Path)value).nodes().forEach(node -> {
                    writeNode(reporter, allocator, vectorMap, index, node);
                    checkBatchStatusAndWriteEventually(provider, fd, index, vectorMap, index.get() % batchSize == 0);
                });
                ((Path)value).relationships().forEach(relationship -> {
                    writeRelationship(reporter, allocator, vectorMap, index, relationship);
                    checkBatchStatusAndWriteEventually(provider, fd, index, vectorMap, index.get() % batchSize == 0);
                });
//                        .collect(Collectors.toList()));
                break;
            case LIST:
                ((List) value).forEach(listItem -> {
                    try {
                        writeArrowResult(reporter, allocator, vectorMap, index, listItem, provider, batchSize, fd);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                break;
            case MAP:
                ((Map<String, Object>) value).forEach((keyItem, valueItem) -> {
                    try {
                        writeArrowResult(reporter, allocator, vectorMap, index, valueItem, provider, batchSize, fd);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                break;

            default:
                index.getAndIncrement();
                allocateAfterCheckExistence(vectorMap, index.get(), allocator, ID_FIELD, value, VarCharVector.class);
                checkBatchStatusAndWriteEventually(provider, fd, index, vectorMap, index.get() % batchSize == 0);
        }
    }

    protected static void writeRelationship(ProgressReporter reporter, RootAllocator allocator, Map<String, FieldVector> vectorRelMap, AtomicInteger indexRel, Relationship rel) {
//        try {
        final int currentIndex = indexRel.getAndIncrement();

        allocateAfterCheckExistence(vectorRelMap, currentIndex, allocator, START_FIELD, rel.getStartNodeId(), UInt8Vector.class);
        allocateAfterCheckExistence(vectorRelMap, currentIndex, allocator, END_FIELD, rel.getEndNodeId(), UInt8Vector.class);
        allocateAfterCheckExistence(vectorRelMap, currentIndex, allocator, TYPE_FIELD, rel.getType().name(), VarCharVector.class);

        final Map<String, Object> allProperties = rel.getAllProperties();
        allProperties.forEach((key, value) -> {
//                try {
            allocateAfterCheckExistence(vectorRelMap, currentIndex, allocator, key, value, VarCharVector.class);
//                } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
//                    e.printStackTrace();
//                }
        });

        reporter.update(0, 1, allProperties.size());
//        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
//            e.printStackTrace();
//        }
    }

    protected static void writeNode(ProgressReporter reporter, RootAllocator allocator, Map<String, FieldVector> vectorMap, AtomicInteger indexNode, Node node) {
//        try {

            final int currentIndex = indexNode.getAndIncrement();

            // id field
            allocateAfterCheckExistence(vectorMap, currentIndex, allocator, ID_FIELD, node.getId(), UInt8Vector.class);

            Map<String, Object> allProperties = node.getAllProperties();
            allProperties.forEach((key, value) -> {
//                try {
                    allocateAfterCheckExistence(vectorMap, currentIndex, allocator, key, value, VarCharVector.class);
//                } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
//                    e.printStackTrace();
//                }
            });

            if (node.getLabels().iterator().hasNext()) {
                allocateAfterCheckExistence(vectorMap, currentIndex, allocator, LABELS_FIELD, labelString(node), VarCharVector.class);
            }

            if (reporter != null) {
                reporter.update(1, 0, allProperties.size());
            }
//        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
//            e.printStackTrace();
//        }
    }
}

// todo -reporter al Result