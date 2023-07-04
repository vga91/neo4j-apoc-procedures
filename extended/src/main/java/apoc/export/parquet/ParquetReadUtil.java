package apoc.export.parquet;

import apoc.load.LoadParquet;
import org.apache.parquet.example.data.Group;
//import org.apache.avro.Schema;
//import org.apache.avro.data.TimeConversions;
//import org.apache.avro.generic.GenericData;
//import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
//import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.Values;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

//import static apoc.export.parquet.CustomTypes.DurationType.DURATION_VALUE;
//import static apoc.export.parquet.CustomTypes.PointType.POINT_VALUE;
import static apoc.export.parquet.ParquetUtil.TYPE_SEP;
import static org.neo4j.values.storable.NoValue.NO_VALUE;

public class ParquetReadUtil {

//    public static GenericData genericDataLoad;
//    static {
//        genericDataLoad = new GenericData();
//        genericDataLoad.addLogicalTypeConversion(new TimeConversions.DateConversion());
//        genericDataLoad.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
//        genericDataLoad.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
//        genericDataLoad.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());
//        for (ParquetTypes type: ParquetTypes.values()) {
//            genericDataLoad.addLogicalTypeConversion(type.getReadConversion());
//        }
//    }
//
    private static Object toValidValue(Object object/*, Schema.Field field*/) {
        if (object instanceof Collection) {
//            final IntFunction<Object[]> prototype = getPrototypeFor(field);
            return ((Collection<?>) object).stream()/*.map(i -> toValidValue(i, field))*/.toArray(/*prototype*/);
        }
        if (object instanceof Map) {
            return ((Map<String, Object>) object).entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> toValidValue(e.getValue()/*, field*/)));
        }
        try {
            // we test if is a valid Neo4j type
            Values.of(object);
            return object;
        } catch (Exception e) {
            // otherwise we try to coerce it
            return object.toString();
        }
    }

//    private static IntFunction<Object[]> getPrototypeFor(Schema.Field field) {
//        String type = field.schema().getTypes().stream()
//                .filter(i -> !i.getType().equals(Schema.Type.NULL))
//                .findFirst()
//                .map(i -> i.getLogicalType() != null ? i.getLogicalType().getName() : i.getElementType().getName() )
//                .orElse(Schema.Type.STRING.getName());
//
//        switch (type) {
//            case "INT":
//            case "LONG":
//                return Long[]::new;
//            case "FLOAT":
//            case "DOUBLE":
//                return Double[]::new;
//            case "BOOLEAN":
//                return Boolean[]::new;
//            case "BYTES":
//                return Byte[]::new;
//            case "DATETIME":
//                return ZonedDateTime[]::new;
//            case "time-micros":
//                return LocalTime[]::new;
//            case "local-timestamp-micros":
//                return LocalDateTime[]::new;
//            case POINT_VALUE:
//                return PointValue[]::new;
//            case "date":
//                return LocalDate[]::new;
//            case DURATION_VALUE:
//                return DurationValue[]::new;
//            default:
//                return String[]::new;
//        }
//    }

    public static Map<String, Object> mapFromRecord(Group record) {
//    public static Map<String, Object> mapFromRecord(GenericRecord record) {

        return Map.of();
//        return record.getSchema()
//                .getFields()
//                .stream()
//                .collect(HashMap::new, // workaround for https://bugs.openjdk.java.net/browse/JDK-8148463
//                        (mapAccumulator, field) -> {
//                            String name = field.name();
//
//                            Object object = record.get(name);
//                            Object value = toValidValue(object, field);
//                            if (value != null && !NO_VALUE.equals(value)) {
//                                // we remove the possible `__<TYPE_FIELD>` suffix
//                                String key = name.split(TYPE_SEP)[0];
//                                mapAccumulator.put(key, value);
//                            }
//                        },
//                        HashMap::putAll);
    }


    public static ParquetReader.Builder<Group> getReaderBuilder(Object source) {
        if (source instanceof String) {
            Path file = new Path((String) source);
            return ParquetReader.builder(new GroupReadSupport(), file);
        }
        LoadParquet.ParquetStream file = new LoadParquet.ParquetStream((byte[]) source);
        try {
            return ParquetReader.read(file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

