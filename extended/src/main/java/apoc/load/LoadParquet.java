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
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

//import static apoc.export.parquet.ExportParquetFileStrategy.genericData;
//import static apoc.export.parquet.ExportParquetFileStrategy.genericDataLoad;
import static apoc.export.parquet.ParquetUtil.DurationType.DURATION_VALUE;
import static apoc.export.parquet.ParquetUtil.NodeType.NEO4J_NODE;
import static apoc.export.parquet.ParquetUtil.PointType.POINT_VALUE;
import static apoc.export.parquet.ParquetUtil.RelationshipType.NEO4J_REL;
import static apoc.export.parquet.ParquetUtil.TYPE_SEP;
import static apoc.export.parquet.ParquetUtil.genericDataLoad;
import static org.neo4j.values.storable.NoValue.NO_VALUE;

public class LoadParquet {

    // todo - create a ReadParquetUtil

    @Context public Log log;


    private static Object toValidValue(Object object, Schema.Field field) {
        if (object != null && object.getClass().isArray()) {
            // TODO...
            return null;//Arrays.stream(object)
        }
        if (object instanceof Collection) { // todo - if array...

            String first = field.schema().getTypes().stream()
                    .filter(i -> !i.getType().equals(Schema.Type.NULL))
                    .findFirst()
                    .map(i -> i.getLogicalType() != null ? i.getLogicalType().getName() : i.getType().name() )
                    .orElse(Schema.Type.STRING.getName());
//            Schema s = first
//                    .orElse(Schema.create(Schema.Type.STRING));
            final IntFunction<Object[]> prototype = getPrototypeFor(first);

            return ((Collection<?>) object).stream()
                    .map(i -> LoadParquet.toValidValue(i, field))
                    .toArray(prototype);
        }
        if (object instanceof Map) {
            return ((Map<String, Object>) object).entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> toValidValue(e.getValue(), field)));
        }
//        if (object instanceof Text) {
//            // todo - maybe delete...
//            System.out.println("object = " + object);
//            return object.toString();
//        }
        try {
            // we test if is a valid Neo4j type
            return Values.of(object);
        } catch (Exception e) {
            // otherwise we try to coerce it
            return object.toString();
        }
    }

    private static IntFunction<Object[]> getPrototypeFor(String type) {
        switch (type) {
            case "INT":
            case "LONG":
                return Long[]::new;
            case "FLOAT":
            case "DOUBLE":
                return Double[]::new;
            case "BOOLEAN":
                return Boolean[]::new;
            case "BYTES":
                return Byte[]::new;
            case "DATETIME":
                return ZonedDateTime[]::new;
            case "time-micros":
                return LocalTime[]::new;
            case "local-timestamp-micros":
                return LocalDateTime[]::new;
            case POINT_VALUE:
                return PointValue[]::new;
            case "date":
                return LocalDate[]::new;
            case DURATION_VALUE:
                return DurationValue[]::new;
            default:
                return String[]::new;
        }
    }

    public static Map<String, Object> mapFromRecord(GenericRecord record) {
        return record.getSchema()
                .getFields()
                .stream()
                .collect(HashMap::new, // workaround for https://bugs.openjdk.java.net/browse/JDK-8148463
                        (mapAccumulator, field) -> {
                            String name = field.name();

                            Object object = record.get(name);
                            Object value = toValidValue(object, field);
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
    public Stream<MapResult> load(
            @Name("input") Object input,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws IOException {

        ParquetReader<GenericData.Record> reader = getBuilder(input)
                .withDataModel(genericDataLoad)
                .withConf(new Configuration())
                .build();

        registerCustomTypes();

        return StreamSupport.stream(new ParquetSpliterator(reader), false)
                .onClose(() -> Util.close(reader));
    }

    public static AvroParquetReader.Builder<GenericData.Record> getBuilder(Object source) {
        if (source instanceof String) {
            Path file = new Path((String) source);
            return AvroParquetReader.builder(file);
        }
        ParquetStream file = new LoadParquet.ParquetStream((byte[]) source);
        return AvroParquetReader.builder(file);
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

    public static class ParquetStream implements InputFile {
//        private final String streamId;
        private final byte[] data;

        private static class SeekableByteArrayInputStream extends ByteArrayInputStream {
            public SeekableByteArrayInputStream(byte[] buf) {
                super(buf);
            }

            public void setPos(int pos) {
                this.pos = pos;
            }

            public int getPos() {
                return this.pos;
            }
        }

        //        public ParquetStream(String streamId, ByteArrayOutputStream stream) {
//
//        }
        public ParquetStream(/*String streamId, */byte[] stream) {
//            this.streamId = streamId;
            this.data = stream;//.toByteArray();
        }

        @Override
        public long getLength() throws IOException {
            return this.data.length;
        }

        @Override
        public SeekableInputStream newStream() throws IOException {
            return new DelegatingSeekableInputStream(new SeekableByteArrayInputStream(this.data)) {
                @Override
                public void seek(long newPos) throws IOException {
                    ((SeekableByteArrayInputStream) this.getStream()).setPos((int) newPos);
                }

                @Override
                public long getPos() throws IOException {
                    return ((SeekableByteArrayInputStream) this.getStream()).getPos();
                }
            };
        }

//        @Override
//        public String toString() {
//            return "ParquetStream[" + streamId + "]";
//        }
    }



}
