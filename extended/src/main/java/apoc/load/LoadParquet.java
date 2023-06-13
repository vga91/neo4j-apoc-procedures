package apoc.load;

import apoc.export.parquet.ParquetUtil;
import apoc.result.MapResult;
import apoc.util.Util;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.storable.Values;
import org.w3c.dom.Text;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.export.parquet.ExportParquetFileStrategy.genericData;
import static apoc.export.parquet.ExportParquetFileStrategy.genericDataLoad;
import static apoc.export.parquet.ParquetUtil.DurationType.DURATION_VALUE;
import static apoc.export.parquet.ParquetUtil.NodeType.NEO4J_NODE;
import static apoc.export.parquet.ParquetUtil.PointType.POINT_VALUE;
import static apoc.export.parquet.ParquetUtil.RelationshipType.NEO4J_REL;
import static apoc.export.parquet.ParquetUtil.TYPE_SEP;
import static org.neo4j.values.storable.NoValue.NO_VALUE;

public class LoadParquet {

    @Context public Log log;


    private static Object toValidValue(Object object) {
        if (object != null && object.getClass().isArray()) {
            // TODO...
            return null;//Arrays.stream(object)
        }
        if (object instanceof Collection) { // todo - if array...
            return ((Collection<?>) object).stream()
                    .map(LoadParquet::toValidValue)
                    .collect(Collectors.toList());
        }
        if (object instanceof Map) {
            return ((Map<String, Object>) object).entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> toValidValue(e.getValue())));
        }
        if (object instanceof Text) {
            // todo - maybe delete...
            System.out.println("object = " + object);
            return object.toString();
        }
        try {
            // we test if is a valid Neo4j type
            return Values.of(object);
        } catch (Exception e) {
            // otherwise we try to coerce it
            return object.toString();
        }
    }

    private static Map<String, Object> mapFromRecord(GenericRecord record) {
        return record.getSchema()
                .getFields()
                .stream()
                .collect(HashMap::new, // workaround for https://bugs.openjdk.java.net/browse/JDK-8148463
                        (mapAccumulator, field) -> {
                            String name = field.name();

                            Object value = toValidValue(record.get(name));
                            if (value != null && !NO_VALUE.equals(value)) {
                                mapAccumulator.put(name.split(TYPE_SEP)[0], value);
                            }
                        },
                        HashMap::putAll);
    }

    private static class ParquetSpliterator extends Spliterators.AbstractSpliterator<MapResult> {

        private final ParquetReader<GenericRecord> reader;
        private final AtomicInteger counter;

        public ParquetSpliterator(ParquetReader reader){
            super(Long.MAX_VALUE, Spliterator.ORDERED);
            this.reader = reader;
//            this.schemaRoot = schemaRoot;
            this.counter = new AtomicInteger();
//            this.reader.loadNextBatch();
        }

        @Override
        public synchronized boolean tryAdvance(Consumer<? super MapResult> action) {
            try {
                // todo - needed with batch??
//                if (counter.get() >= schemaRoot.getRowCount()) {
//                    if (reader.loadNextBatch()) {
//                        counter.set(0);
//                    } else {
//                        return false;
//                    }
//                }
                GenericRecord read = reader.read();
                if (read != null) {
//                final Map<String, Object> row = schemaRoot.getFieldVectors()
//                        .stream()
//                        .collect(HashMap::new, (map, fieldVector) -> map.put(fieldVector.getName(), read(fieldVector, counter.get())), HashMap::putAll); // please look at https://bugs.openjdk.java.net/browse/JDK-8148463
//                counter.incrementAndGet();
                    action.accept(new MapResult(mapFromRecord(read)));
                    return true;
                }

                return false;
            } catch (Exception e) {
                // todo -- log..
                return false;
            }

        }
    }

    // todo - if - else <-- if byte[] read from stream else read from file

//    @Procedure(name = "apoc.load.arrow.stream")
//    @Description("Imports nodes and relationships from the provided arrow byte array.")
//    public Stream<MapResult> stream(
//            @Name("source") byte[] source,
//            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws IOException {
//        RootAllocator allocator = new RootAllocator();
//        ByteArrayInputStream inputStream = new ByteArrayInputStream(source);
//        ArrowStreamReader streamReader = new ArrowStreamReader(inputStream, allocator);
//        VectorSchemaRoot schemaRoot = streamReader.getVectorSchemaRoot();
//        return StreamSupport.stream(new ArrowSpliterator(streamReader, schemaRoot), false)
//                .onClose(() -> {
//                    Util.close(allocator);
//                    Util.close(streamReader);
//                    Util.close(schemaRoot);
//                    Util.close(inputStream);
//                });
//    }

    @Procedure(name = "apoc.load.parquet")
    @Description("Imports nodes and relationships from the provided arrow file.")
    public Stream<MapResult> file(
            @Name("file") String fileName,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws IOException {
//        final SeekableByteChannel channel = FileUtils.inputStreamFor(fileName, null, null, null)
//                .asChannel();
//        RootAllocator allocator = new RootAllocator();
//        ArrowFileReader streamReader = new ArrowFileReader(channel, allocator);
//        VectorSchemaRoot schemaRoot = streamReader.getVectorSchemaRoot();



        ParquetReader<GenericData.Record> reader = AvroParquetReader
                .<GenericData.Record>builder(new Path(fileName))
                .withDataModel(genericDataLoad)
                .withConf(new Configuration())
                .build();

        registerCustomTypes();

        return StreamSupport.stream(new ParquetSpliterator(reader), false)
                .onClose(() -> Util.close(reader));
    }

    public static void registerCustomTypes() {
        LogicalTypes.LogicalTypeFactory factory = new LogicalTypes.LogicalTypeFactory() {
            private final LogicalType convertLongLogicalType = new ParquetUtil.NodeType();

            @Override
            public LogicalType fromSchema(Schema schema) {
                return convertLongLogicalType;
            }
        };

        LogicalTypes.LogicalTypeFactory factory2 = new LogicalTypes.LogicalTypeFactory() {
            private final LogicalType convertLongLogicalType = new ParquetUtil.DurationType();

            @Override
            public LogicalType fromSchema(Schema schema) {
                return convertLongLogicalType;
            }
        };


//        TODO --> FARE UN CUSTOMCONVERSION DI NODO SOLO PER IL LOAD --> DA STRINGA A VIRTUALNODE....


        LogicalTypes.LogicalTypeFactory factory3 = new LogicalTypes.LogicalTypeFactory() {
            private final LogicalType convertLongLogicalType = new ParquetUtil.PointType();

            @Override
            public LogicalType fromSchema(Schema schema) {
                return convertLongLogicalType;
            }
        };

        LogicalTypes.LogicalTypeFactory factory4 = new LogicalTypes.LogicalTypeFactory() {
            private final LogicalType convertLongLogicalType = new ParquetUtil.RelationshipType();

            @Override
            public LogicalType fromSchema(Schema schema) {
                return convertLongLogicalType;
            }
        };

        // todo - foreach???
        LogicalTypes.register(NEO4J_NODE, factory);
        LogicalTypes.register(DURATION_VALUE, factory2);
        LogicalTypes.register(POINT_VALUE, factory3);
        LogicalTypes.register(NEO4J_REL, factory4);
    }


}
