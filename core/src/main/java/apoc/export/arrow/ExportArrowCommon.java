package apoc.export.arrow;

import apoc.export.util.ProgressReporter;
import apoc.meta.Meta;
import apoc.util.JsonUtil;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.ArrowWriter;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.nio.channels.Channels;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static apoc.export.arrow.ArrowConstants.END_FIELD;
import static apoc.export.arrow.ArrowConstants.ID_FIELD;
import static apoc.export.arrow.ArrowConstants.LABELS_FIELD;
import static apoc.export.arrow.ArrowConstants.START_FIELD;
import static apoc.export.arrow.ArrowConstants.STREAM_EDGE_PREFIX;
import static apoc.export.arrow.ArrowConstants.STREAM_NODE_PREFIX;
import static apoc.export.arrow.ArrowConstants.TYPE_FIELD;
import static apoc.util.Util.labelString;

public class ExportArrowCommon {

    protected static void implementExportCommon(RootAllocator allocator, int batchSize, Object valueToProcess, ArrowConstants.FunctionType function, ProgressReporter reporter, OutputStream fd, boolean isFile) {
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

    private static void checkBatchStatusAndWriteEventually(DictionaryProvider.MapDictionaryProvider dictProvider, OutputStream fd, AtomicInteger index, Map<String, FieldVector> vectorMap, boolean isBatchReadyForWrite, boolean isFileStream) {
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


    private static void getCurrentBatchStream(DictionaryProvider.MapDictionaryProvider dictProvider, ByteArrayOutputStream out, List<FieldVector> encodedVectors, int index) throws IOException {
        try (VectorSchemaRoot schemaRoot = new VectorSchemaRoot(getFields(encodedVectors), encodedVectors);
             ArrowStreamWriter writer = new ArrowStreamWriter(schemaRoot, dictProvider, Channels.newChannel(out))) {

            writeBatch(writer);
            schemaRoot.setRowCount(index);
        }
    }

    private static void writeBatch(ArrowWriter writer) throws IOException {
        writer.start();
        writer.writeBatch();
        writer.end();
    }

    protected static void closeFieldVectors(Map<String, FieldVector> vectorMap, List<FieldVector> encodedVectors) {
        vectorMap.values().forEach(ValueVector::close);
        encodedVectors.forEach(ValueVector::close);
    }

    private static List<Field> getFields(List<FieldVector> encodedVectorsRel) {
        return encodedVectorsRel.stream().map(ValueVector::getField).collect(Collectors.toList());
    }

    private static List<FieldVector> returnEncodedVector(DictionaryProvider.MapDictionaryProvider dictProvider, Map<String, FieldVector> vectorMap, AtomicInteger indexNode) {

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

    private static void writeRelationship(ProgressReporter reporter, RootAllocator allocator, Map<String, FieldVector> vectorMap, AtomicInteger index, Relationship rel, DictionaryProvider.MapDictionaryProvider provider, int batchSize, OutputStream fd, boolean isFile) {
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

    private static void writeNode(ProgressReporter reporter, RootAllocator allocator, Map<String, FieldVector> vectorMap, AtomicInteger index, Node node, DictionaryProvider.MapDictionaryProvider provider, int batchSize, OutputStream fd, boolean isFile) {
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

    private static <T> void allocateAfterCheckExistence(Map<String, FieldVector> vectorMap, int currentIndex, RootAllocator allocator, String key, Object value, Class<T> clazz) {
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
}
