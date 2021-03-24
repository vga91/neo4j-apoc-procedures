package apoc.export.arrow;

import apoc.ApocConfig;
import apoc.Pools;
import apoc.export.csv.CsvFormat;
import apoc.export.cypher.ExportFileManager;
import apoc.export.cypher.FileManagerFactory;
import apoc.export.graphml.XmlGraphMLReader;
import apoc.export.util.ExportConfig;
import apoc.export.util.ExportUtils;
import apoc.export.util.FormatUtils;
import apoc.export.util.NodesAndRelsSubGraph;
import apoc.export.util.ProgressReporter;
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
import org.apache.arrow.vector.VarBinaryVector;
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
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
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
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
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

import static apoc.ApocConfig.apocConfig;
import static apoc.export.arrow.ArrowUtils.END_FIELD;
import static apoc.export.arrow.ArrowUtils.ID_FIELD;
import static apoc.export.arrow.ArrowUtils.LABELS_FIELD;
import static apoc.export.arrow.ArrowUtils.START_FIELD;
import static apoc.export.arrow.ArrowUtils.TYPE_FIELD;
import static apoc.util.Util.labelString;
import static apoc.util.Util.labelStrings;
import static org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE;

public class ExportArrow {
    private static ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);

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
//        DictionaryProvider.MapDictionaryProvider dictProvider = new DictionaryProvider.MapDictionaryProvider();
//
//        dictProvider.

        String importDir = apocConfig().getString("dbms.directories.import", "import");
        File file_nodes = new File(importDir, "nodes_" + fileName);
        File file_rels = new File(importDir, fileName + "_edges");
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


    private void allocateAfterCheckExistence(Map<String, FieldVector> vectorMap, Map<String, FieldVector> dictVectorMap, int currentIndex, RootAllocator allocator, String key, Object value) {
        final boolean fieldNotExists = !vectorMap.containsKey(key);

        vectorMap.putIfAbsent(key, new VarBinaryVector(key, allocator));
        dictVectorMap.putIfAbsent("dict___" + key, new VarBinaryVector(key, allocator));
//        if (dictionaryProvider.lookup())

        VarBinaryVector labelsVector = (VarBinaryVector) vectorMap.get(key);
        VarBinaryVector dictLabelsVector = (VarBinaryVector) dictVectorMap.get("dict___" + key);

        if (fieldNotExists) {
            labelsVector.allocateNewSafe();
            dictLabelsVector.allocateNewSafe();
        }

        try {
            final byte[] objBytes = JsonUtil.OBJECT_MAPPER.writeValueAsString(value).getBytes();
            labelsVector.setSafe(currentIndex, objBytes);
            dictLabelsVector.setSafe(currentIndex, objBytes);
        } catch (IOException e) {
            e.printStackTrace(); // todo - Runtime
        }
    }

    private void dump(Object data, ExportConfig exportConfig, ProgressReporter reporter, ExportFileManager printWriter, ArrowFormat exporter, RootAllocator allocator, String fileName) throws Exception {
        DictionaryProvider.MapDictionaryProvider dictProvider = new DictionaryProvider.MapDictionaryProvider();

        // TODO - dictionary
//        DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
//// create dictionary and provider
//        final VarCharVector dictVectorId = new VarCharVector("dict", allocator);

//        Dictionary dictionaryId =
//                new Dictionary(dictVectorId, new DictionaryEncoding(1L, false, /*indexType=*/null));
//        dictProvider.put(dictionaryId);
//        dictProvider.put(dictionaryLabels);
//        dictProvider.put(dictionary);
//        dictProvider.put(dictionary);


//        dictVector.allocateNewSafe();
//        dictVector.setSafe(0, "aa".getBytes());
//        dictVector.setSafe(1, "bb".getBytes());
//        dictVector.setSafe(2, "cc".getBytes());
//        dictVector.setValueCount(3);
        // create vector and encode it
//        final VarCharVector vector = new VarCharVector("vector", allocator);
//        vector.allocateNewSafe();
//        vector.setSafe(0, "bb".getBytes());
//        vector.setSafe(1, "bb".getBytes());
//        vector.setSafe(2, "cc".getBytes());
//        vector.setSafe(3, "aa".getBytes());
//        vector.setValueCount(4);
//// get the encoded vector
        // IntVector encodedVector = (IntVector) DictionaryEncoder.encode(vector, dictionary);


        String importDir = apocConfig().getString("dbms.directories.import", "import");
        File file_nodes = new File(importDir, "nodes_" + fileName);
        File file_rels = new File(importDir, "edges_" + fileName);

        // populate nodes VectorSchemaRoot
        final UInt8Vector nodeId = new UInt8Vector(ID_FIELD, allocator);
        nodeId.allocateNewSafe();
        final UInt8Vector nodeIdDict = new UInt8Vector("dict___" + ID_FIELD, allocator);
        nodeIdDict.allocateNewSafe();

        if (data instanceof SubGraph) {
            SubGraph subGraph = (SubGraph) data;

            Map<String, FieldVector> vectorMap = new TreeMap<>();
            Map<String, FieldVector> dictVectorMap = new TreeMap<>();
            vectorMap.put(ID_FIELD, nodeId);
            dictVectorMap.put("dict___" + ID_FIELD, nodeId);

            AtomicInteger indexNode = new AtomicInteger();
            subGraph.getNodes().forEach(node -> {

                final int currentIndex = indexNode.getAndIncrement();

                nodeId.setSafe(currentIndex, node.getId());
                nodeIdDict.setSafe(currentIndex, node.getId());

                Map<String, Object> allProperties = node.getAllProperties();
                allProperties.forEach((key, value) -> allocateAfterCheckExistence(vectorMap, dictVectorMap, currentIndex, allocator, key, value));

                if (node.getLabels().iterator().hasNext()) {
                    allocateAfterCheckExistence(vectorMap, dictVectorMap, currentIndex, allocator, LABELS_FIELD, labelString(node));
                }

                // TODO - check
                reporter.update(1, 0, allProperties.size());
            });

            vectorMap.values().forEach(item -> item.setValueCount(indexNode.get()));

            AtomicLong dictIndex = new AtomicLong();
            dictVectorMap.values().forEach(item -> {
                item.setValueCount(indexNode.get());
                dictProvider.put(new Dictionary(item, new DictionaryEncoding(dictIndex.incrementAndGet(), false, null)));
            });

            AtomicLong dictIndexTwo = new AtomicLong();
            List<FieldVector> encodedVectors = vectorMap.values().stream().map(vector -> {
                return (FieldVector) DictionaryEncoder.encode(vector, dictProvider.lookup(dictIndexTwo.incrementAndGet()));
            }).collect(Collectors.toList());


//            final List<FieldVector> vectors = new ArrayList<>(vectorMap.values());
            final List<Field> encodedFields = encodedVectors.stream().map(ValueVector::getField).collect(Collectors.toList());
            VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(encodedFields, encodedVectors);

            try (//VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(makeSchema(exportConfig), allocator);
                 FileOutputStream fd = new FileOutputStream(file_nodes);
                 ArrowFileWriter nodeFileWriter = new ArrowFileWriter(vectorSchemaRoot, dictProvider, fd.getChannel())) {
                nodeFileWriter.start();
                // todo - batch

                nodeFileWriter.writeBatch();
                nodeFileWriter.end();
            }

            vectorSchemaRoot.close();

            nodeId.close();
            nodeIdDict.close();
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
            final UInt8Vector startId = new UInt8Vector(START_FIELD, allocator);
            startId.allocateNewSafe();
            final UInt8Vector startIdDict = new UInt8Vector("dict___" + START_FIELD, allocator);
            startIdDict.allocateNewSafe();
            final UInt8Vector endId = new UInt8Vector(END_FIELD, allocator);
            endId.allocateNewSafe();
            final UInt8Vector endIdDict = new UInt8Vector("dict___" + END_FIELD, allocator);
            endIdDict.allocateNewSafe();
            final VarBinaryVector type = new VarBinaryVector(TYPE_FIELD, allocator);
            type.allocateNewSafe();
            final VarBinaryVector typeDict = new VarBinaryVector("dict___" + TYPE_FIELD, allocator);
            typeDict.allocateNewSafe();

            Map<String, FieldVector> vectorRelMap = new TreeMap<>();
            Map<String, FieldVector> dictVectorRelMap = new TreeMap<>();
            vectorRelMap.put(START_FIELD, startId);
            vectorRelMap.put(END_FIELD, endId);
            vectorRelMap.put(TYPE_FIELD, type);
            dictVectorRelMap.put("dict___" + START_FIELD, startId);
            dictVectorRelMap.put("dict___" + END_FIELD, endId);
            dictVectorRelMap.put("dict___" + TYPE_FIELD, type);

            subGraph.getRelationships().forEach(rel -> {
                final int currentIndex = indexRel.getAndIncrement();

                startId.setSafe(currentIndex, rel.getStartNodeId());
                endId.setSafe(currentIndex, rel.getEndNodeId());
                startIdDict.setSafe(currentIndex, rel.getStartNodeId());
                endIdDict.setSafe(currentIndex, rel.getEndNodeId());

                // todo - se type è vuoto?
                type.setSafe(currentIndex, rel.getType().name().getBytes());
                typeDict.setSafe(currentIndex, rel.getType().name().getBytes());

                final Map<String, Object> allProperties = rel.getAllProperties();
                allProperties.forEach((key, value) -> allocateAfterCheckExistence(vectorRelMap, dictVectorRelMap, currentIndex, allocator, key, value));

                reporter.update(0, 1, allProperties.size());
            });

            vectorRelMap.values().forEach(item -> item.setValueCount(indexRel.get()));

            AtomicLong dictIndexRel = new AtomicLong();
            dictVectorRelMap.values().forEach(item -> {
                item.setValueCount(indexRel.get());
                dictProviderRel.put(new Dictionary(item, new DictionaryEncoding(dictIndexRel.incrementAndGet(), false, null)));
            });

            AtomicLong dictIndexTwoRel = new AtomicLong();
            List<FieldVector> encodedVectorsRel = vectorRelMap.values().stream().map(vector -> {
                return (FieldVector) DictionaryEncoder.encode(vector, dictProviderRel.lookup(dictIndexTwoRel.incrementAndGet()));
            }).collect(Collectors.toList());

            // todo - vedere come ottimizzare
//            dictVectorId.setValueCount(indexRel.get());

            // todo

//            final List<FieldVector> vectorsRel = new ArrayList<>(vectorRelMap.values());
            final List<Field> fieldsRel = encodedVectorsRel.stream().map(ValueVector::getField).collect(Collectors.toList());
            VectorSchemaRoot vectorRelSchemaRoot = new VectorSchemaRoot(fieldsRel, encodedVectorsRel);


//            DictionaryProvider.MapDictionaryProvider dictProviderRel = new DictionaryProvider.MapDictionaryProvider();
            try (FileOutputStream fd = new FileOutputStream(file_rels);
                 ArrowFileWriter relFileWriter = new ArrowFileWriter(vectorRelSchemaRoot, dictProviderRel, fd.getChannel())) {
                relFileWriter.start();
                // todo - batch
                relFileWriter.writeBatch();
                relFileWriter.end();
            }

            vectorRelSchemaRoot.close();

            startId.close();
            startIdDict.close();
            endId.close();
            endIdDict.close();
            type.close();
            typeDict.close();
            vectorRelMap.values().forEach(ValueVector::close);
            dictVectorRelMap.values().forEach(ValueVector::close);
        }

        if (data instanceof Result) {
            // todo
        }
    }
}
