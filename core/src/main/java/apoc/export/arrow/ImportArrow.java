package apoc.export.arrow;

import apoc.Pools;
import apoc.export.csv.CsvEntityLoader;
import apoc.export.csv.CsvLoaderConfig;
import apoc.export.util.BatchTransaction;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
import apoc.util.JsonUtil;
import apoc.util.Util;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.collect.Iterables;
import net.minidev.json.JSONUtil;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.impl.VarBinaryHolderReaderImpl;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.holders.NullableVarBinaryHolder;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.bouncycastle.util.Arrays;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static apoc.export.arrow.ArrowUtils.END_FIELD;
import static apoc.export.arrow.ArrowUtils.ID_FIELD;
import static apoc.export.arrow.ArrowUtils.LABELS_FIELD;
import static apoc.export.arrow.ArrowUtils.START_FIELD;
import static apoc.export.arrow.ArrowUtils.TYPE_FIELD;
import static apoc.export.json.JsonImporter.flatMap;
import static apoc.util.JsonUtil.OBJECT_MAPPER;
import static java.util.Arrays.asList;

public class ImportArrow {
    @Context
    public GraphDatabaseService db;

    @Context
    public Pools pools;

    @Context
    public Log log;

//    public ImportArrow(GraphDatabaseService db) {
//        this.db = db;
//    }
//
    public ImportArrow() {}


    // TODO TODO TODO TODO - MA VA ESPORTATO PURE LO SCHEMA ????!!?!?!?!!!?
    @Procedure(name = "apoc.import.arrow", mode = Mode.SCHEMA)
    @Description("TODO")
    public Stream<ProgressInfo> importArrow(
            @Name("fileNodes") String fileNodes,
            @Name(value = "fileEdges", defaultValue = "") String fileEdges,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config // TODO - IMPORT CONFIG HA SENSO?
    ) throws Exception {
        ImportArrowConfig importConfig = new ImportArrowConfig(config);
        ProgressInfo result =
                Util.inThread(pools, () -> {

                    // TODO - BATCH HANDLING

                    try (RootAllocator allocator = new RootAllocator()) {
                        final ProgressReporter reporter = new ProgressReporter(null, null, new ProgressInfo("progress.arrow", "file", "arrow"));

                        final int batchSize = importConfig.getBatchSize();
                        try (FileInputStream fd = new FileInputStream(fileNodes);
                             ArrowFileReader nodeFileReader = new ArrowFileReader(new SeekableReadChannel(fd.getChannel()), allocator)) {
                            VectorSchemaRoot schemaRoot = nodeFileReader.getVectorSchemaRoot();

                            // TODO - COME METTERE IL BATCH SIZE ALL'IMPORT
//                        while (nodeFileReader.loadNextBatch()) {


                            // todo - mettere max size quando esporto e vedere che succede.....
                            // ciclo i nodi? - fare batch

                            try (BatchTransaction tx = new BatchTransaction(db, batchSize, reporter)) {
                                while (nodeFileReader.loadNextBatch()) {

                                    // todo - metodo comune con rel
                                    // get dictionaries and decode the vector
                                    Map<Long, Dictionary> dictionaryMap = nodeFileReader.getDictionaryVectors();

                                    Map<String, ValueVector> decodedVectorsMap = schemaRoot.getFieldVectors().stream().collect(Collectors.toMap(ValueVector::getName, vector -> {
                                        long idDictionary = vector.getField().getDictionary().getId();
                                        return DictionaryEncoder.decode(vector, dictionaryMap.get(idDictionary));
                                    }));

                                    int sizeId = decodedVectorsMap.get(ID_FIELD).getValueCount();

                                    IntStream.range(0, sizeId).forEach(index -> {
                                        Node node = tx.getTransaction().createNode();

                                        VarCharVector labelVector = (VarCharVector) decodedVectorsMap.get(LABELS_FIELD);

                                        asList(new String(labelVector.get(index)).split(":")).forEach(label -> {
                                            try {
                                                node.addLabel(Label.label(OBJECT_MAPPER.readValue(label, String.class)));
                                            } catch (JsonProcessingException e) {
                                                e.printStackTrace(); // todo - funzione comune
                                            }
                                        });

                                        // properties
                                        decodedVectorsMap.entrySet().stream()
                                                .filter(i -> !List.of(LABELS_FIELD, ID_FIELD).contains(i.getKey())).forEach(propVector -> {
                                            try {
//                                                    VarCharVector encodedVector = (VarCharVector) schemaRoot.getVector(index);
//                                                    long dictionaryVector = encodedVector.getField().getDictionary().getId();
                                                VarCharVector vector = (VarCharVector) propVector.getValue();

                                                byte[] value = vector.get(index);
                                                if (value != null) {
//                                                        String stringValue = new String(value);
//                                                    node.setProperty(vector.getName(), UtilreadMap( new String(value)) );
//                                                        if ()
                                                    Object valueRead = OBJECT_MAPPER.readValue(value, Object.class);
                                                    if (valueRead instanceof Map) {
                                                        Stream<Map.Entry<String, Object>> entryStream = flatMap((Map<String, Object>) valueRead, vector.getName());
                                                        entryStream
                                                                .filter(e -> e.getValue() != null)
                                                                .forEach(entry -> node.setProperty(entry.getKey(), entry.getValue()));
                                                    } else {
                                                        if (valueRead instanceof Collection) {
                                                            // TODO - PUO ESSERE ANCHE LISTA DI MAPPE O COSE COSì

//                                                                ((List) valueRead).to

                                                            final Collection valueReadAsList = (Collection) valueRead;
                                                            valueRead = Iterables.toArray(valueReadAsList, valueReadAsList.iterator().next().getClass());

//                                                                valueRead = ArrayUtils.toArray(valueRead);
//                                                                valueRead = ((List<String>) valueRead).toArray(new String[3]);//  ((List<String>) valueRead).toArray();
                                                        }
                                                        node.setProperty(vector.getName(), valueRead);
                                                    }
                                                }
                                            } catch (IllegalAccessError ignored) {
                                            } catch (IOException e) {
                                                e.printStackTrace(); // todo - funzione comune e runtime
                                            }
//                                    NullableVarBinaryHolder holder = new NullableVarBinaryHolder();
//                                    ((VarCharVector) vector).get(index, holder);
//                                    if (holder.isSet != 0) {
//                                        holder.buffer.getBytes(holder.start, holder.);
//                                    }

                                        });

                                    });
                                }
                            } catch (IOException e) {
                                e.printStackTrace(); // todo - runtime
                            }
                        }

                        try (FileInputStream fd = new FileInputStream(fileEdges);
                             ArrowFileReader relFileReader = new ArrowFileReader(new SeekableReadChannel(fd.getChannel()), allocator)) {
                            relFileReader.initialize();
                            VectorSchemaRoot schemaRoot = relFileReader.getVectorSchemaRoot();

                            try (BatchTransaction tx = new BatchTransaction(db, batchSize, reporter)) {
                                while (relFileReader.loadNextBatch()) {

                                    final UInt8Vector start = (UInt8Vector) schemaRoot.getVector(START_FIELD);
                                    final UInt8Vector end = (UInt8Vector) schemaRoot.getVector(END_FIELD);
                                    final VarCharVector type = (VarCharVector) schemaRoot.getVector(TYPE_FIELD);

                                    IntStream.range(0, start.getValueCapacity()).forEach(index -> {
                                        Node from = tx.getTransaction().getNodeById(start.get(index));
                                        Node to = tx.getTransaction().getNodeById(end.get(index));
                                        // todo - check empty type
//                                RelationshipType relationshipType = type == null ? getRelationshipType(reader) : RelationshipType.withName(label);
                                        RelationshipType relationshipType = RelationshipType.withName(new String(type.get(index)));
                                        Relationship relationship = from.createRelationshipTo(to, relationshipType);

                                        schemaRoot.getFieldVectors().stream()
                                                .filter(i -> !List.of(START_FIELD, END_FIELD, TYPE_FIELD).contains(i.getName())).forEach(vector -> {
                                            try {
                                                byte[] value = ((VarCharVector) vector).get(index);
                                                JsonParser parser = OBJECT_MAPPER.getFactory().createParser(new String(value));
                                                relationship.setProperty(vector.getName(), OBJECT_MAPPER.readValue(parser, Object.class));
                                            } catch (IllegalAccessError ignored) {
                                            } catch (IOException e) {
                                                e.printStackTrace(); // TODO - RUNTIME
                                            }
//                                    NullableVarBinaryHolder holder = new NullableVarBinaryHolder();
//                                    ((VarCharVector) vector).get(index, holder);
//                                    if (holder.isSet != 0) {
//                                        holder.buffer.getBytes(holder.start, holder.);
//                                    }
                                        });
                                    });
                                }
                            }
                        }

                        return reporter.getTotal();
                    }

                });
        return Stream.of(result);
    }
}
