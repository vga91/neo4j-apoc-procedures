package apoc.export.parquet;

import apoc.load.LoadParquet;
import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.example.data.Group;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.jetbrains.annotations.Nullable;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Values;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

//import static apoc.export.parquet.CustomTypes.DurationType.DURATION_VALUE;
//import static apoc.export.parquet.CustomTypes.PointType.POINT_VALUE;
import static apoc.export.parquet.ParquetUtil.TYPE_SEP;
import static org.neo4j.values.storable.NoValue.NO_VALUE;

public class ParquetReadUtil {


    private static Object convertValue(String value, String typeName) {
        switch (typeName) {
            case "Point":
                // replace all eventual `\` char, see testPointList()
                value = value.replaceAll("\\\\,", ",");
                return PointValue.parse(value);
            case "LocalDateTime":
                return LocalDateTimeValue.parse(value).asObjectCopy();
            case "LocalTime":
                return LocalTimeValue.parse(value).asObjectCopy();
            case "DateTime":
                return DateTimeValue.parse(value, () -> ZoneId.of("Z")).asObjectCopy();
            case "Time":
                return TimeValue.parse(value, () -> ZoneId.of("Z")).asObjectCopy();
            case "Date":
                return DateValue.parse(value).asObjectCopy();
            case "Duration":
                return DurationValue.parse(value);
            case "Char":
                return value.charAt(0);
            case "Byte":
                return value.getBytes();
            case "Double":
                return Double.parseDouble(value);
            case "Float":
                return Float.parseFloat(value);
            case "Short":
                return Short.parseShort(value);
            case "NO_VALUE":
                return null;
            default:
                // If ends with "Array", for example StringArray
                if (typeName.endsWith("Array")) {
                    value = StringUtils.removeStart(value, "[");
                    value = StringUtils.removeEnd(value, "]");
                    String array = typeName.replace("Array", "");

                    final Object[] prototype = getPrototypeFor(array);
                    return Arrays.stream(value.split(","))
                            .map(item -> convertValue(StringUtils.trim(item), array))
                            .collect(Collectors.toList())
                            .toArray(prototype);
                }
                return value;
        }
    }

    static Object[] getPrototypeFor(String type) {
        switch (type) {
            case "Long":
            case "Integer":
                return new Long[]{};
            case "Double":
                return new Double[]{};
            case "Float":
                return new Float[]{};
            case "Boolean":
                return new Boolean[]{};
            case "Byte":
                return new Byte[]{};
            case "Short":
                return new Short[]{};
            case "Char":
                return new Character[]{};
            case "String":
                return new String[]{};
            case "DateTime":
                return new ZonedDateTime[]{};
            case "LocalTime":
                return new LocalTime[]{};
            case "LocalDateTime":
                return new LocalDateTime[]{};
            case "Point":
                return new PointValue[]{};
            case "Time":
                return new OffsetTime[]{};
            case "Date":
                return new LocalDate[]{};
            case "Duration":
                return new DurationValue[]{};
            default:
                throw new IllegalStateException("Type " + type + " not supported.");
        }
    }

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
    private static Object toValidValue(Object object, Type field, ParquetConfig config) {
        Object fieldName = config.getMapping().get(field.getName());
        if (fieldName != null) {
            return convertValue(object.toString(), fieldName.toString());
        }

        if (object instanceof Collection) {
            final IntFunction<Object[]> prototype = getPrototypeFor(field);
            return ((Collection<?>) object).stream().map(i -> toValidValue(i, field, config)).toArray(prototype);
        }
        if (object instanceof Map) {
            return ((Map<String, Object>) object).entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> toValidValue(e.getValue(), field, config)));
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

    private static java.util.concurrent.TimeUnit toTimeUnitJava(LogicalTypeAnnotation.TimeUnit unit) {
        return switch (unit) {
            case NANOS -> TimeUnit.NANOSECONDS;
            case MICROS -> TimeUnit.MICROSECONDS;
            case MILLIS -> TimeUnit.MILLISECONDS;
        };
    }

    private static Object getValue(Type field, Group record) {
        PrimitiveType.PrimitiveTypeName typeName;
        System.out.println("typeName = ");
        try {
            typeName = ((PrimitiveType) field).getPrimitiveTypeName();
        } catch (Exception e) {
            System.out.println("e = " + e);
            try {

            Group kids = record.getGroup(field.getName(), 0);
            List<Type> fields = ((GroupType) kids.getType().getFields().get(0)).getFields();

            List<Object> list = new ArrayList<>();
            for (int i = 0; i < kids.getFieldRepetitionCount("list"); i++) {
                Object list1 = getObject("element", kids.getGroup("list", i), PrimitiveType.PrimitiveTypeName.BINARY, null);
                list.add(list1);
            }
            return list;
            } catch (Exception e2) {
                System.out.println("e2 = " + e2);
                return null;
            }

        }

        // todo
        LogicalTypeAnnotation logicalTypeAnnotation = field.getLogicalTypeAnnotation();

        return getObject(field.getName(), record, typeName, logicalTypeAnnotation);
    }

    private static Object getObject(String field, Group record, PrimitiveType.PrimitiveTypeName typeName, LogicalTypeAnnotation logicalTypeAnnotation) {
        try {
            return switch (typeName) {
                case FLOAT -> record.getFloat(field, 0);
                case INT32 -> record.getInteger(field, 0);
                case INT64 -> {
                    long aLong = record.getLong(field, 0);
                    if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
                        LogicalTypeAnnotation.TimestampLogicalTypeAnnotation logicalTypeAnnotation1 = (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalTypeAnnotation;
                        if (logicalTypeAnnotation1.isAdjustedToUTC()) {
                            yield Instant.EPOCH.plus(aLong, toTimeUnitJava(logicalTypeAnnotation1.getUnit()).toChronoUnit());
                        } else {
                            yield LocalDateTime.ofInstant(Instant.EPOCH.plus(aLong, toTimeUnitJava(logicalTypeAnnotation1.getUnit()).toChronoUnit()), ZoneId.of("UTC"));//  logicalTypeAnnotation1.getUnit()
                        }
                    }
                    yield aLong;
                }
                case INT96 -> record.getInt96(field, 0);
                case DOUBLE -> record.getDouble(field, 0);
                case BOOLEAN -> record.getBoolean(field, 0);
                // todo - if logical type = STRING, convert to string
                //      else to byte[]
                case BINARY -> record.getString(field, 0);
                default -> null;
            };
        } catch (RuntimeException e) {
            System.out.println("e = " + e);
            return null;
        }
    }


    private static IntFunction<Object[]> getPrototypeFor(Type field) {
        return String[]::new;
//        field.asGroupType().getFields().get(0).

//        String type = field.get().getTypes().stream()
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
    }

    public static Map<String, Object> mapFromRecord(Group record, ParquetConfig config) {
//    public static Map<String, Object> mapFromRecord(GenericRecord record) {

//        return Map.of();
        return record.getType()
                .getFields()
                .stream()
                .collect(HashMap::new, // workaround for https://bugs.openjdk.java.net/browse/JDK-8148463
                        (mapAccumulator, field) -> {
                            String name = field.getName();

                            Object object = null;//record.get(name);
                            Object value = getValue(field, record);// toValidValue(object, field);
                            value = toValidValue(value, field, config);
                            if (value != null && !NO_VALUE.equals(value)) {
                                // we remove the possible `__<TYPE_FIELD>` suffix
                                String key = name.split(TYPE_SEP)[0];
                                mapAccumulator.put(key, value);
                            }

//                            try {
//                                record.getGroup(name, 0)
//                            } catch (RuntimeException e) {
//                                // todo not found
//                            }
                        },
                        HashMap::putAll);
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
