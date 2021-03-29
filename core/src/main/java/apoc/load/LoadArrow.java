package apoc.load;

import apoc.load.util.LoadArrowConfig;
import apoc.load.util.Results;
import apoc.util.Util;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.neo4j.graphdb.GraphDatabaseService;

import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.FileInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static apoc.export.arrow.ImportArrow.closeVectors;
import static apoc.export.arrow.ImportArrow.getCurrentIndex;
import static apoc.util.FileUtils.changeFileUrlIfImportDirectoryConstrained;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class LoadArrow {

    @Context
    public GraphDatabaseService db;

    @Procedure(mode = Mode.WRITE)
    @Description("TODO")
    public Stream<ArrowResult> arrow(
            @Name("file") String file,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config // TODO - IMPORT CONFIG HA SENSO?
    ) throws Exception {

        LoadArrowConfig loadConfig = new LoadArrowConfig(config);

        try (RootAllocator allocator = new RootAllocator()) {

            try (FileInputStream fd = new FileInputStream(new URI(file).getPath());
                 ArrowFileReader fileReader = new ArrowFileReader(new SeekableReadChannel(fd.getChannel()), allocator)) {

                try (VectorSchemaRoot schemaRoot = fileReader.getVectorSchemaRoot()) {

                    List<ArrowResult> arrowResult = new ArrayList<>();
                    while (fileReader.loadNextBatch()) {

                        // get dictionaries and decode the vector
                        Map<Long, Dictionary> dictionaryMap = fileReader.getDictionaryVectors();

                        Map<String, ValueVector> decodedVectorsMap = schemaRoot.getFieldVectors().stream().collect(Collectors.toMap(ValueVector::getName, vector -> {
                            long idDictionary = vector.getField().getDictionary().getId();
                            return DictionaryEncoder.decode(vector, dictionaryMap.get(idDictionary));
                        }));

                        int sizeId = schemaRoot.getRowCount() + (int) loadConfig.getSkip();

                        final List<ArrowResult> currentBatch = IntStream.range(0, sizeId).mapToObj(id -> new ArrowResult(decodedVectorsMap,
                                id, loadConfig.getIgnore(), loadConfig.getResults()))
                                .collect(Collectors.toList());
                        arrowResult.addAll(currentBatch);

                        closeVectors(schemaRoot, decodedVectorsMap);
                    }

                    return arrowResult.stream();
                }
            }
        }
    }

    public static class ArrowResult {
        public long lineNo;
        public List<Object> list;
        public Map<String, Object> map;

        public ArrowResult(Map<String, ValueVector> header, long lineNo, List<String> ignore, EnumSet<Results> results) {
            this.lineNo = lineNo;
            this.map = results.contains(Results.map) ? createMap(header, lineNo, ignore) : emptyMap();
            this.list = results.contains(Results.list) ? createList(header, lineNo, ignore) : emptyList();
        }

        private List<Object> createList(Map<String, ValueVector> header, long lineNo, List<String> ignore) {
            removeIgnored(header, ignore);

            return header.values().stream()
                    .map(vector -> getVectorValue((int) lineNo, vector))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }

        private Map<String, Object> createMap(Map<String, ValueVector> header, long lineNo, List<String> ignore) {
            removeIgnored(header, ignore);

            return header.entrySet().stream()
                    .map(entry -> Pair.of(entry.getKey(), getVectorValue((int) lineNo, entry.getValue())))
                    .filter(i -> i.other() != null)
                    .collect(Collectors.toMap(Pair::first, Pair::other));
        }

        private Object getVectorValue(int lineNo, ValueVector vector) {
            try {
                if (vector instanceof UInt8Vector) {
                    return ((UInt8Vector) vector).get(lineNo);
                } else {
                    byte[] value = ((VarCharVector) vector).get(lineNo);
                    return getCurrentIndex(value);
                }
            } catch (IllegalStateException e) {
                return null;
            }
        }
    }

    private static void removeIgnored(Map<String, ValueVector> header, List<String> ignore) {
        header.entrySet().removeIf(i -> ignore.contains(i.getKey()));
    }
}
