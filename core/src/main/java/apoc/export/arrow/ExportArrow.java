package apoc.export.arrow;

import apoc.ApocConfig;
import apoc.Pools;
import apoc.export.cypher.ExportFileManager;
import apoc.export.cypher.FileManagerFactory;
import apoc.export.util.ExportConfig;
import apoc.export.util.ExportUtils;
import apoc.export.util.NodesAndRelsSubGraph;
import apoc.export.util.ProgressReporter;
import apoc.meta.Meta;
import apoc.result.ProgressInfo;
import apoc.util.JsonUtil;
import apoc.util.Util;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.ArrowWriter;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.cypher.export.SubGraph;
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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.nio.channels.Channels;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.ApocConfig.apocConfig;
import static apoc.export.arrow.ArrowUtils.DICT_PREFIX;
import static apoc.export.arrow.ArrowUtils.EDGE_FILE_PREFIX;
import static apoc.export.arrow.ArrowUtils.END_FIELD;
import static apoc.export.arrow.ArrowUtils.ID_FIELD;
import static apoc.export.arrow.ArrowUtils.LABELS_FIELD;
import static apoc.export.arrow.ArrowUtils.NODE_FILE_PREFIX;
import static apoc.export.arrow.ArrowUtils.RESULT_FILE_PREFIX;
import static apoc.export.arrow.ArrowUtils.START_FIELD;
import static apoc.export.arrow.ArrowUtils.STREAM_EDGE_PREFIX;
import static apoc.export.arrow.ArrowUtils.STREAM_NODE_PREFIX;
import static apoc.export.arrow.ArrowUtils.TYPE_FIELD;
import static apoc.util.Util.labelString;

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
    @Description("apoc.export.arrow.all(file, config) - exports whole database as arrow to the provided file")
    public Stream<ProgressInfo> all(@Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {
        String source = String.format("database: nodes(%d), rels(%d)", Util.nodeCount(tx), Util.relCount(tx));
        return exportArrow(fileName, source, new DatabaseSubGraph(tx), config);
    }

    @Procedure
    @Description("apoc.export.arrow.data(nodes,rels,file,config) - exports given nodes and relationships as csv to the provided file")
    public Stream<ProgressInfo> data(@Name("nodes") List<Node> nodes, @Name("rels") List<Relationship> rels, @Name("file") String fileName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {

        String source = String.format("data: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportArrow(fileName, source, new NodesAndRelsSubGraph(tx, nodes, rels), config);
    }

    @Procedure
    @Description("apoc.export.arrow.graph(graph,file,config) - exports given graph object as arrow to the provided file")
    public Stream<ProgressInfo> graph(@Name("graph") Map<String,Object> graph, @Name("file") String fileName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {

        Collection<Node> nodes = (Collection<Node>) graph.get("nodes");
        Collection<Relationship> rels = (Collection<Relationship>) graph.get("relationships");
        String source = String.format("graph: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportArrow(fileName, source, new NodesAndRelsSubGraph(tx, nodes, rels), config);
    }

    @Procedure
    @Description("apoc.export.arrow.query(query,file,{config,...,params:{params}}) - exports results from the cypher statement as arrow to the provided file")
    public Stream<ProgressInfo> query(@Name("query") String query, @Name("file") String fileName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        Map<String,Object> params = config == null ? Collections.emptyMap() : (Map<String,Object>)config.getOrDefault("params", Collections.emptyMap());
        Result result = tx.execute(query,params);
        String source = String.format("statement: cols(%d)", result.columns().size());
        return exportArrow(fileName, source,result,config);
    }


    private Stream<ProgressInfo> exportArrow(@Name("file") String fileName, String source, Object data, Map<String, Object> config) throws Exception {

        ExportConfig exportConfig = new ExportConfig(config);
        if (StringUtils.isNotBlank(fileName)) apocConfig.checkWriteAllowed(exportConfig);
        final String format = "arrow";
        ProgressInfo progressInfo = new ProgressInfo(fileName, source, format);
        progressInfo.batchSize = exportConfig.getBatchSize();
        ProgressReporter reporter = new ProgressReporter(null, null, progressInfo);

        ExportFileManager cypherFileManager = FileManagerFactory.createFileManager(fileName, false);

            if (exportConfig.streamStatements()) {
                return ExportUtils.getProgressInfoStream(db,
                        pools.getDefaultExecutorService(),
                        terminationGuard,
                        format,
                        exportConfig,
                        reporter,
                        cypherFileManager,
                        progressReporter -> {
                            try {
                                dump(data, exportConfig, reporter, fileName);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                );
            } else {
                dump(data, exportConfig, reporter, fileName);
                return reporter.stream();
            }
    }


    protected static <T> void allocateAfterCheckExistence(Map<String, FieldVector> vectorMap, int currentIndex, RootAllocator allocator, String key, Object value, Class<T> clazz) {
        try {
            Constructor<?> constructor = clazz.getConstructor(String.class, BufferAllocator.class);

            final boolean fieldNotExists = !vectorMap.containsKey(key);

            vectorMap.putIfAbsent(key, (FieldVector) constructor.newInstance(key, allocator));

            FieldVector labelsVector = vectorMap.get(key);

            if (fieldNotExists) {
                labelsVector.allocateNewSafe();
            }

            if (labelsVector instanceof VarCharVector) {
                final byte[] objBytes = JsonUtil.OBJECT_MAPPER.writeValueAsBytes(value);
                ((VarCharVector) labelsVector).setSafe(currentIndex, objBytes);
            } else {
                ((UInt8Vector) labelsVector).setSafe(currentIndex, (long) value);
            }

        } catch (Exception e) {
            throw new RuntimeException("Error during vector allocation", e);
        }
    }

    private void dump(Object valueToExport, ExportConfig exportConfig, ProgressReporter reporter, String fileName) throws Exception {

        try (RootAllocator allocator = new RootAllocator()) {

            String importDir = apocConfig().getString("dbms.directories.import", "import");

            int batchSize = exportConfig.getBatchSize();

            if (valueToExport instanceof SubGraph) {
                SubGraph subGraph = (SubGraph) valueToExport;

                File fileNodes = new File(importDir, NODE_FILE_PREFIX + fileName);
                processArrowStream(reporter, allocator, batchSize, subGraph, fileNodes, ArrowUtils.FunctionType.NODES);

                if (!subGraph.getRelationships().iterator().hasNext()) {
                    return;
                }

                File fileEdges = new File(importDir, EDGE_FILE_PREFIX + fileName);
                processArrowStream(reporter, allocator, batchSize, subGraph, fileEdges, ArrowUtils.FunctionType.EDGES);
            }

            if (valueToExport instanceof Result) {

                File file = new File(importDir, RESULT_FILE_PREFIX + fileName);
                processArrowStream(reporter, allocator, batchSize, valueToExport, file, ArrowUtils.FunctionType.RESULT);
            }

            reporter.done();
        }
    }

    public static void processArrowStream(ProgressReporter reporter, RootAllocator allocator, int batchSize, Object valueToProcess, File file, ArrowUtils.FunctionType function) throws IOException {
        try (FileOutputStream fd = new FileOutputStream(file)) {

            implementExportCommon(allocator, batchSize, valueToProcess, function, reporter, fd, true);

        }
    }

    public static void implementExportCommon(RootAllocator allocator, int batchSize, Object valueToProcess, ArrowUtils.FunctionType function, ProgressReporter reporter, OutputStream fd, boolean isFile) {
        DictionaryProvider.MapDictionaryProvider dictProvider = new DictionaryProvider.MapDictionaryProvider();
        AtomicInteger index = new AtomicInteger();
        Map<String, FieldVector> vectorMap = new TreeMap<>();

        switch (function) {
            case NODES:
                ((SubGraph) valueToProcess).getNodes().forEach(node -> writeNode(
                        reporter, allocator, vectorMap, index, node, dictProvider, batchSize, fd, isFile));
                break;
            case EDGES:
                ((SubGraph) valueToProcess).getRelationships().forEach(relationship -> writeRelationship(
                        reporter, allocator, vectorMap, index, relationship, dictProvider, batchSize, fd, isFile));
                break;
            case RESULT:
                String[] header = ((Result) valueToProcess).columns().toArray(new String[((Result) valueToProcess).columns().size()]);
                ((Result) valueToProcess).accept((row) -> {
                    for (String keyName : header) {
                        Object value = row.get(keyName);
                        writeArrowResult(reporter, allocator, vectorMap, index, value, dictProvider, batchSize, fd, isFile);
                    }
                    reporter.nextRow();
                    return true;
                });
                break;
            case STREAM:
                ((SubGraph) valueToProcess).getNodes().forEach(node -> writeNode(
                        reporter, allocator, vectorMap, index, node, dictProvider, batchSize, fd, isFile));
                ((SubGraph) valueToProcess).getRelationships().forEach(relationship -> writeRelationship(
                        reporter, allocator, vectorMap, index, relationship, dictProvider, batchSize, fd, isFile));
                break;
            default:
                throw new RuntimeException("No function type");
        }
        checkBatchStatusAndWriteEventually(dictProvider, fd, index, vectorMap, index.get() % batchSize != 0, isFile);
    }

    private static void checkBatchStatusAndWriteEventually(DictionaryProvider.MapDictionaryProvider dictProvider, OutputStream fd, AtomicInteger indexNode, Map<String, FieldVector> vectorMap, boolean isBatchReadyForWrite) {
        checkBatchStatusAndWriteEventually(dictProvider, fd, indexNode, vectorMap,isBatchReadyForWrite, true);
    }

    protected static void checkBatchStatusAndWriteEventually(DictionaryProvider.MapDictionaryProvider dictProvider, OutputStream fd, AtomicInteger index, Map<String, FieldVector> vectorMap, boolean isBatchReadyForWrite, boolean isFileStream) {
        if (isBatchReadyForWrite) {
            List<FieldVector> encodedVectors = returnEncodedVector(dictProvider, vectorMap, index);
            try {
                if (isFileStream) {
                    getCurrentBatch(dictProvider, (FileOutputStream) fd, encodedVectors, index.get());
                } else {
                    getCurrentBatchStream(dictProvider, (ByteArrayOutputStream) fd, encodedVectors, index.get());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            closeFieldVectors(vectorMap, encodedVectors);
            vectorMap.clear();
        }
    }

    private static void getCurrentBatch(DictionaryProvider.MapDictionaryProvider dictProvider, FileOutputStream fd, List<FieldVector> encodedVectors, int index) throws IOException {
        try (VectorSchemaRoot schemaRoot = new VectorSchemaRoot(getFields(encodedVectors), encodedVectors);
             ArrowFileWriter writer = new ArrowFileWriter(schemaRoot, dictProvider, fd.getChannel())) {

            writeBatch(writer);
            schemaRoot.setRowCount(index);
        }
    }


    protected static void getCurrentBatchStream(DictionaryProvider.MapDictionaryProvider dictProvider, ByteArrayOutputStream out, List<FieldVector> encodedVectors, int index) throws IOException {
        try (VectorSchemaRoot schemaRoot = new VectorSchemaRoot(getFields(encodedVectors), encodedVectors);
             ArrowStreamWriter writer = new ArrowStreamWriter(schemaRoot, dictProvider, Channels.newChannel(out))) {

            writeBatch(writer);
            schemaRoot.setRowCount(index);
        }
    }

    protected static void writeBatch(ArrowWriter writer) throws IOException {
        writer.start();
        writer.writeBatch();
        writer.end();
    }

    protected static void closeFieldVectors(Map<String, FieldVector> vectorMap, List<FieldVector> encodedVectors) {
        vectorMap.values().forEach(ValueVector::close);
        encodedVectors.forEach(ValueVector::close);
    }

    protected static List<Field> getFields(List<FieldVector> encodedVectorsRel) {
        return encodedVectorsRel.stream().map(ValueVector::getField).collect(Collectors.toList());
    }

    protected static List<FieldVector> returnEncodedVector(DictionaryProvider.MapDictionaryProvider dictProvider, Map<String, FieldVector> vectorMap, AtomicInteger indexNode) {

        final Collection<FieldVector> valuesVector = vectorMap.values();
        valuesVector.forEach(item -> item.setValueCount(indexNode.get()));

        AtomicLong dictIndex = new AtomicLong();

        valuesVector.forEach(item -> dictProvider.put(
                new Dictionary(item, new DictionaryEncoding(dictIndex.incrementAndGet(), false, null))));

        AtomicLong dictIndexEncoded = new AtomicLong();
        return valuesVector.stream().map(vector ->
                (FieldVector) DictionaryEncoder.encode(vector, dictProvider.lookup(dictIndexEncoded.incrementAndGet()))
        ).collect(Collectors.toList());
    }


    private static void writeArrowResult(ProgressReporter reporter, RootAllocator allocator, Map<String, FieldVector> vectorMap, AtomicInteger index, Object value, DictionaryProvider.MapDictionaryProvider provider, int batchSize, OutputStream fd, boolean isFile) {
        Meta.Types type = Meta.Types.of(value);
        switch (type) {
            case NODE:
                writeNode(reporter, allocator, vectorMap, index, (Node) value, provider, batchSize, fd, isFile);
                break;

            case RELATIONSHIP:
                writeRelationship(reporter, allocator, vectorMap, index, (Relationship) value, provider, batchSize, fd, isFile);
                break;

            case PATH:
                ((Path) value).nodes().forEach(node -> writeNode(
                        reporter, allocator, vectorMap, index, node, provider, batchSize, fd, true));
                ((Path) value).relationships().forEach(relationship ->
                    writeRelationship(reporter, allocator, vectorMap, index, relationship, provider, batchSize, fd, isFile));
                break;

            case LIST:
                ((List) value).forEach(listItem -> {
                    try {
                        writeArrowResult(reporter, allocator, vectorMap, index, listItem, provider, batchSize, fd, isFile);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                break;

            case MAP:
                ((Map<String, Object>) value).forEach((keyItem, valueItem) -> {
                    try {
                        writeArrowResult(reporter, allocator, vectorMap, index, valueItem, provider, batchSize, fd, isFile);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                break;

            default:
                index.getAndIncrement();
                allocateAfterCheckExistence(vectorMap, index.get(), allocator, ID_FIELD, value, VarCharVector.class);
                checkBatchStatusAndWriteEventually(provider, fd, index, vectorMap, index.get() % batchSize == 0, isFile);
        }
    }

    protected static void writeRelationship(ProgressReporter reporter, RootAllocator allocator, Map<String, FieldVector> vectorMap, AtomicInteger index, Relationship rel, DictionaryProvider.MapDictionaryProvider provider, int batchSize, OutputStream fd, boolean isFile) {
        final int currentIndex = index.getAndIncrement();

        allocateAfterCheckExistence(vectorMap, currentIndex, allocator, START_FIELD, rel.getStartNodeId(), UInt8Vector.class);
        allocateAfterCheckExistence(vectorMap, currentIndex, allocator, END_FIELD, rel.getEndNodeId(), UInt8Vector.class);
        allocateAfterCheckExistence(vectorMap, currentIndex, allocator, TYPE_FIELD, rel.getType().name(), VarCharVector.class);

        final Map<String, Object> allProperties = rel.getAllProperties();
        allProperties.forEach((key, value) -> allocateAfterCheckExistence(
                vectorMap, currentIndex, allocator, isFile ? key : (STREAM_EDGE_PREFIX + key), value, VarCharVector.class));

        if (reporter != null) {
            reporter.update(0, 1, allProperties.size());
        }
        checkBatchStatusAndWriteEventually(provider, fd, index, vectorMap, index.get() % batchSize == 0, isFile);
    }

    protected static void writeNode(ProgressReporter reporter, RootAllocator allocator, Map<String, FieldVector> vectorMap, AtomicInteger index, Node node, DictionaryProvider.MapDictionaryProvider provider, int batchSize, OutputStream fd, boolean isFile) {
        final int currentIndex = index.getAndIncrement();

        // id field
        allocateAfterCheckExistence(vectorMap, currentIndex, allocator, ID_FIELD, node.getId(), UInt8Vector.class);

        Map<String, Object> allProperties = node.getAllProperties();
        allProperties.forEach((key, value) -> allocateAfterCheckExistence(
                vectorMap, currentIndex, allocator, isFile ? key : (STREAM_NODE_PREFIX + key) , value, VarCharVector.class));

        if (node.getLabels().iterator().hasNext()) {
            allocateAfterCheckExistence(vectorMap, currentIndex, allocator, LABELS_FIELD, labelString(node), VarCharVector.class);
        }

        if (reporter != null) {
            reporter.update(1, 0, allProperties.size());
        }

        checkBatchStatusAndWriteEventually(provider, fd, index, vectorMap, index.get() % batchSize == 0, isFile);
    }
}

// todo -reporter al Result