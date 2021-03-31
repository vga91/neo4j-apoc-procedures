package apoc.load;

import apoc.load.util.ArrowResult;
import apoc.load.util.LoadArrowConfig;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.neo4j.graphdb.GraphDatabaseService;

import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static apoc.export.arrow.ImportArrowCommon.closeVectors;
import static apoc.export.arrow.ImportArrowCommon.getPathNormalized;

public class LoadArrow {

    @Context
    public GraphDatabaseService db;

    @Procedure
    @Description("apoc.load.arrow('file',$config) YIELD lineNo, list, map - load arrow from file as stream of values")
    public Stream<ArrowResult> arrow(
            @Name("file") String file,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config
    ) throws Exception {

        try (RootAllocator allocator = new RootAllocator();
             FileInputStream fd = new FileInputStream(getPathNormalized(file));
             ArrowFileReader reader = new ArrowFileReader(new SeekableReadChannel(fd.getChannel()), allocator)) {

            return getArrowResultFromInputStream(config, reader);
        }
    }


    @Procedure("apoc.load.arrow.stream")
    @Description("apoc.load.arrow.stream('source',$config) YIELD lineNo, list, map - load arrow from source byte[] as stream of values")
    public Stream<ArrowResult> arrow(
            @Name("source") byte[] source,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config
    ) throws Exception {

        try (RootAllocator allocator = new RootAllocator();
             ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(source), allocator)) {
            return getArrowResultFromInputStream(config, reader);
        }
    }

    public Stream<ArrowResult> getArrowResultFromInputStream(@Name(value = "config", defaultValue = "{}") Map<String, Object> config, ArrowReader reader) throws IOException {
        try (VectorSchemaRoot schemaRoot = reader.getVectorSchemaRoot()) {

            LoadArrowConfig loadConfig = new LoadArrowConfig(config);

            List<ArrowResult> arrowResult = new ArrayList<>();
            while (reader.loadNextBatch()) {

                // get dictionaries and decode the vector
                Map<Long, Dictionary> dictionaryMap = reader.getDictionaryVectors();

                Map<String, ValueVector> decodedVectorsMap = schemaRoot.getFieldVectors().stream().collect(Collectors.toMap(ValueVector::getName, vector -> {
                    long idDictionary = vector.getField().getDictionary().getId();
                    return DictionaryEncoder.decode(vector, dictionaryMap.get(idDictionary));
                }));

                final long skip = loadConfig.getSkip();
                final long limit = loadConfig.getLimit();
                final long limitPlusSkip = limit + skip < 0 ? Long.MAX_VALUE : limit + skip ;
                long sizeId = Math.min(schemaRoot.getRowCount(), limitPlusSkip);

                final List<ArrowResult> currentBatch = LongStream.range(skip, sizeId).mapToObj(id -> new ArrowResult(decodedVectorsMap,
                        id, loadConfig.getIgnore(), loadConfig.getResults()))
                        .collect(Collectors.toList());
                arrowResult.addAll(currentBatch);

                closeVectors(schemaRoot, decodedVectorsMap);
            }

            return arrowResult.stream();
        }
    }
}
