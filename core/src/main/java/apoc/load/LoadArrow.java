package apoc.load;

import apoc.result.MapResult;
import apoc.result.ProgressInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static apoc.export.arrow.ArrowUtils.ID_FIELD;
import static apoc.export.arrow.ArrowUtils.LABELS_FIELD;
import static apoc.util.JsonUtil.OBJECT_MAPPER;
import static java.util.Arrays.asList;

public class LoadArrow {

    @Context
    public GraphDatabaseService db;

    @Procedure(name = "apoc.import.arrow.stream", mode = Mode.SCHEMA)
    @Description("TODO")
    public Stream<MapResult> importArrow(
            @Name("file") String file,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config // TODO - IMPORT CONFIG HA SENSO?
    ) throws Exception {

        try (RootAllocator allocator = new RootAllocator()) {
            try (FileInputStream fd = new FileInputStream(file);
                 ArrowFileReader fileReader = new ArrowFileReader(new SeekableReadChannel(fd.getChannel()), allocator)) {

                VectorSchemaRoot schemaRoot = fileReader.getVectorSchemaRoot();

                // get dictionaries and decode the vector
                Map<Long, Dictionary> dictionaryMap = fileReader.getDictionaryVectors();

                Map<String, ValueVector> decodedVectorsMap = schemaRoot.getFieldVectors().stream().collect(Collectors.toMap(ValueVector::getName, vector -> {
                    long idDictionary = vector.getField().getDictionary().getId();
                    return DictionaryEncoder.decode(vector, dictionaryMap.get(idDictionary));
                }));

                schemaRoot.getRowCount();

                int sizeId = decodedVectorsMap.get(ID_FIELD).getValueCount();
//                int sizeId = decodedVectorsMap.get(ID_FIELD).getValueCount();

                IntStream.range(0, sizeId).forEach(index -> {

                    VarCharVector labelVector = (VarCharVector) decodedVectorsMap.get(LABELS_FIELD);

//                    asList(new String(labelVector.get(index)).split(":")).forEach(label -> {
//                        try {
//                            node.addLabel(Label.label(OBJECT_MAPPER.readValue(label, String.class)));
//                        } catch (JsonProcessingException e) {
//                            e.printStackTrace(); // todo - funzione comune
//                        }
//                    });

                    // properties
                    decodedVectorsMap.entrySet().stream()
                            .filter(i -> !List.of(LABELS_FIELD, ID_FIELD).contains(i.getKey()))
                            .forEach(propVector -> setCurrentVector(index, node, propVector, ""));

                });

                closeVectors(schemaRoot, decodedVectorsMap);
            }
        }
    }

    private static class ArrowSpliterator extends Spliterators.AbstractSpliterator<CSVResult> {

    }
}
