package apoc.export.parquet;

import apoc.ApocConfig;
import apoc.load.LoadParquet;
import apoc.util.JsonUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
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
import java.util.stream.Collectors;


import static apoc.util.FileUtils.changeFileUrlIfImportDirectoryConstrained;
import static org.neo4j.values.storable.NoValue.NO_VALUE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

public class ParquetReadUtil {

    private static Object convertValue(String value, String typeName) {
        switch (typeName) {
            case "Point":
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
            case "Int":
                return Integer.parseInt(value);
            case "Long":
                return Long.parseLong(value);
            case "Node", "Relationship":
                return JsonUtil.parse(value, null, Map.class);
            // todo - needed
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
                            .toList()
                            .toArray(prototype);
                }
                return value;
        }
    }

    // similar to CsvPropertyConverter
    static Object[] getPrototypeFor(String type) {
        switch (type) {
            case "Long":
                return new Long[]{};
            case "Integer":
                return new Integer[]{};
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

    public static Object toValidValue1(Object object, Type field, ParquetConfig config) {
        // if there is a mapping config, we use that one to convert the current object
        Object fieldName = config.getMapping().get(field.getName());
        if (object != null && fieldName != null) {
            return convertValue(object.toString(), fieldName.toString());
        }

        if (object instanceof Collection) {
            // if there isn't a mapping config, we convert the list to a String[]
            return ((Collection<?>) object).stream()
                    .map(i -> toValidValue1(i, field, config))
                    .collect(Collectors.toList())
                    .toArray(new String[0]);
        }
        if (object instanceof Map) {
            return ((Map<String, Object>) object).entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> toValidValue1(e.getValue(), field, config)));
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

//    public static Object toValidValue(Object object, ColumnReader columnReader, ParquetConfig config) {
        // if there is a mapping config, we use that one to convert the current object
//        Object field = columnReader.getDescriptor().getPath()[0];// config.getMapping().get(field.getName());
    public static Object toValidValue(Object object, String field, ParquetConfig config) {
        Object fieldName = config.getMapping().get(field);
        if (object != null && fieldName != null) {
            return convertValue(object.toString(), fieldName.toString());
        }

        if (object instanceof Collection) {
            // if there isn't a mapping config, we convert the list to a String[]
            return ((Collection<?>) object).stream()
                    .map(i -> toValidValue(i, field, config))
                    .collect(Collectors.toList())
                    .toArray(new String[0]);
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

    public static java.util.concurrent.TimeUnit toTimeUnitJava(LogicalTypeAnnotation.TimeUnit unit) {
        return switch (unit) {
            case NANOS -> TimeUnit.NANOSECONDS;
            case MICROS -> TimeUnit.MICROSECONDS;
            case MILLIS -> TimeUnit.MILLISECONDS;
        };
    }

//    private static Object getValue(Type field, Group group) {
//        if (field instanceof PrimitiveType) {
//            PrimitiveTypeName typeName = field.asPrimitiveType().getPrimitiveTypeName();
//            LogicalTypeAnnotation logicalTypeAnnotation = field.getLogicalTypeAnnotation();
//
//            return getObject(field.getName(), group, typeName, logicalTypeAnnotation);
//        }
////        return "";
//
//
//        try {
//
//            Group subGroup = group.getGroup(field.getName(), 0);
//
//            List<Object> list = new ArrayList<>();
//            for (int i = 0; i < subGroup.getFieldRepetitionCount("list"); i++) {
//                // todo - handle array data types
//                Group listItem = subGroup.getGroup("list", i);
//                Object list1 = getObject("element", listItem, PrimitiveTypeName.BINARY, null);
//                list.add(list1);
//            }
//            return list;
//        } catch (RuntimeException e) {
//            // todo - common - when element is not found in the current group
//            if (e.getMessage().contains("not found")) {
//                return null;
//            }
//            throw e;
//        }
//    }

//    private static Object getObject(String field, Group record, PrimitiveTypeName typeName, LogicalTypeAnnotation logicalTypeAnnotation) {
//        try {
//            return switch (typeName) {
//                case FLOAT -> record.getFloat(field, 0);
//                case INT32 -> record.getInteger(field, 0);
//                case INT64 -> {
//////                    long recordLong = record.getLong(field, 0);
//////                    if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
//////                        LogicalTypeAnnotation.TimestampLogicalTypeAnnotation logicalTypeAnnotation1 = (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalTypeAnnotation;
//////                        if (logicalTypeAnnotation1.isAdjustedToUTC()) {
//////                            yield Instant.EPOCH.plus(recordLong, toTimeUnitJava(logicalTypeAnnotation1.getUnit()).toChronoUnit());
//////                        } else {
//////                            yield LocalDateTime.ofInstant(Instant.EPOCH.plus(recordLong, toTimeUnitJava(logicalTypeAnnotation1.getUnit()).toChronoUnit()), ZoneId.of("UTC"));//  logicalTypeAnnotation1.getUnit()
//////                        }
//////                    }
//                    yield record.getLong(field, 0);
//                }
////                case INT96 -> record.getInt96(field, 0);
////                case DOUBLE -> record.getDouble(field, 0);
//                case BOOLEAN -> record.getBoolean(field, 0);
//                case BINARY -> record.getString(field, 0);
//                default -> null;
//            };
//        } catch (RuntimeException e) {
//            if (e.getMessage().contains("not found")) {
//                return null;
//            }
//            throw e;
//        }
//    }

    public static Object getValue(Type field, Group group) {
        if (field instanceof PrimitiveType) {
            PrimitiveTypeName typeName = field.asPrimitiveType().getPrimitiveTypeName();
            LogicalTypeAnnotation logicalTypeAnnotation = field.getLogicalTypeAnnotation();

            return getObject(field.getName(), group, typeName, logicalTypeAnnotation);
        }

        try {

            Group subGroup = group.getGroup(field.getName(), 0);

            List<Object> list = new ArrayList<>();
            for (int i = 0; i < subGroup.getFieldRepetitionCount("list"); i++) {
                // todo - handle array data types
                Group listItem = subGroup.getGroup("list", i);
                Object list1 = getObject("element", listItem, PrimitiveTypeName.BINARY, null);
                list.add(list1);
            }
            return list;
        } catch (RuntimeException e) {
            // todo - common - when element is not found in the current group
            if (e.getMessage().contains("not found")) {
                return null;
            }
            throw e;
        }
    }

    private static Object getObject(String field, Group record, PrimitiveTypeName typeName, LogicalTypeAnnotation logicalTypeAnnotation) {
        try {
            return switch (typeName) {
                case FLOAT -> record.getFloat(field, 0);
                case INT32 -> record.getInteger(field, 0);
                case INT64 -> {
                    long recordLong = record.getLong(field, 0);
                    if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
                        LogicalTypeAnnotation.TimestampLogicalTypeAnnotation logicalTypeAnnotation1 = (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalTypeAnnotation;
                        if (logicalTypeAnnotation1.isAdjustedToUTC()) {
                            yield Instant.EPOCH.plus(recordLong, toTimeUnitJava(logicalTypeAnnotation1.getUnit()).toChronoUnit());
                        } else {
                            yield LocalDateTime.ofInstant(Instant.EPOCH.plus(recordLong, toTimeUnitJava(logicalTypeAnnotation1.getUnit()).toChronoUnit()), ZoneId.of("UTC"));//  logicalTypeAnnotation1.getUnit()
                        }
                    }
                    yield recordLong;
                }
                case INT96 -> record.getInt96(field, 0);
                case DOUBLE -> record.getDouble(field, 0);
                case BOOLEAN -> record.getBoolean(field, 0);
                case BINARY -> record.getString(field, 0);
                default -> null;
            };
        } catch (RuntimeException e) {
            if (e.getMessage().contains("not found")) {
                return null;
            }
            throw e;
        }
    }

    public static Map<String, Object> mapFromRecord(Group record, ParquetConfig config) {
        return record.getType()
                .getFields()
                .stream()
                .collect(HashMap::new, // workaround for https://bugs.openjdk.java.net/browse/JDK-8148463
                        (mapAccumulator, field) -> {
                            String name = field.getName();

                            Object value = getValue(field, record);
                            value = toValidValue1(value, field, config);
                            if (value != null && !NO_VALUE.equals(value)) {
                                mapAccumulator.put(name, value);
                            }
                        },
                        HashMap::putAll);
    }

    public static Map<String, Object> mapFromRecord(PageReadStore rowGroup, ParquetConfig config) {
//        ColumnReadStore columnReadStore = new ColumnReadStoreImpl(rowGroup, this.recordConverter, this.schema, this.createdBy);

//        schema.getColumns().stream()
//                .filter(c -> columnNames.isEmpty() || columnNames.contains(c.getPath()[0]))
//                .collect(Collectors.toList());

        long currentRowGroupSize = rowGroup.getRowCount();
//        List<ColumnReader> currentRowGroupColumnReaders = columns.stream().map(columnReadStore::getColumnReader).collect(Collectors.toList());
//        long currentRowIndex = 0L;

        return null;
//        return record.getType()
//                .getFields()
//                .stream()
//                .collect(HashMap::new, // workaround for https://bugs.openjdk.java.net/browse/JDK-8148463
//                        (mapAccumulator, field) -> {
//                            String name = field.getName();
//
//                            Object value = getValue(field, record);
////                            value = toValidValue(value, field, config);
////                            if (value != null && !NO_VALUE.equals(value)) {
//                                mapAccumulator.put(name, value);
////                            }
//                        },
//                        HashMap::putAll);
    }

//    public static ApocParquetReaderBuilder getReaderBuilder(Object source) {
//        try {
//            if (source instanceof String) {
//                ApocConfig.apocConfig().isImportFileEnabled();
//                String fileName = changeFileUrlIfImportDirectoryConstrained((String) source);
//                Path file = new Path(fileName);
//                return ApocParquetReaderBuilder.builder(file);
//            }
//            LoadParquet.ParquetStream file = new LoadParquet.ParquetStream((byte[]) source);
//                return ApocParquetReaderBuilder.read(file);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
}
