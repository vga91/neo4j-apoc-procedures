package apoc.export.parquet;


import apoc.convert.ConvertUtils;
import apoc.util.JsonUtil;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static apoc.util.Util.labelStrings;
import static org.apache.parquet.schema.Types.optionalList;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.apache.parquet.schema.LogicalTypeAnnotation.*;
import static org.apache.parquet.schema.Types.*;

public class ParquetUtil {
    public static class TimeConversions {
        public static LocalDate dateFromInt(Integer daysFromEpoch) {
            return LocalDate.ofEpochDay(daysFromEpoch);
        }

        public static LocalTime timeFromInt(Integer millisFromMidnight) {
            return LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(millisFromMidnight));
        }

        public static LocalTime localTimeFromLong(Long microsFromMidnight) {
            return LocalTime.ofNanoOfDay(TimeUnit.MICROSECONDS.toNanos(microsFromMidnight));
        }

        public static Instant instantFromLong(Long millisFromEpoch) {
            return Instant.ofEpochMilli(millisFromEpoch);
        }

        public static Instant fromLong(Long microsFromEpoch) {
            long epochSeconds = microsFromEpoch / (1_000_000L);
            long nanoAdjustment = (microsFromEpoch % (1_000_000L)) * 1_000L;

            return Instant.ofEpochSecond(epochSeconds, nanoAdjustment);
        }

        public static LocalDateTime localDateTimeFromLongMillis(Long millisFromEpoch) {
            Instant instant = instantFromLong(millisFromEpoch);
            return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        }

        public static LocalDateTime localDateTimeFromLongMicros(Long microsFromEpoch) {
            Instant instant = instantFromLong(microsFromEpoch);
            return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        }

//        public static LocalDateTime localDateTimeFromLongMicros(Long microsFromEpoch) {
//            Instant instant = instantFromLong(microsFromEpoch);
//            return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
//        }
    }




//    public static GenericData genericData;
//    static {
//        genericData = new GenericData();
//        genericData.addLogicalTypeConversion(new TimeConversions.DateConversion());
//        genericData.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
//        genericData.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
//        genericData.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());
//        for (ParquetTypes type: ParquetTypes.values()) {
//            genericData.addLogicalTypeConversion(type.getWriteConversion());
//        }
//    }

    // TODO - make configurable
    public static final String TYPE_SEP = "___";


    public static String fromMetaType(apoc.meta.Types type) {
        switch (type) {
            case INTEGER:
                return "LONG";
            case FLOAT:
                return "DOUBLE";
            case LIST:
                String inner = type.toString().substring("LIST OF ".length()).trim();
                final apoc.meta.Types innerType = apoc.meta.Types.from(inner);
                if (innerType == apoc.meta.Types.LIST || innerType == apoc.meta.Types.MAP ) {
                    return "ANYARRAY";
                }
                return fromMetaType(innerType) + "ARRAY";
            default:
                return type.name().replaceAll("_", "").toUpperCase();
        }
    }

    public static Group mapToRecord(MessageType schema, Map<String, Object> map) {
        GroupFactory factory = new SimpleGroupFactory(schema);
        Group group = factory.newGroup();

        map.forEach((k, v)-> {
            try {
                Type type = schema.getType(k);
                if (type.getLogicalTypeAnnotation() instanceof ListLogicalTypeAnnotation) {
                    appendList(group, k, v);
                } else {
                    append(group, k, v, schema);
                }
            } catch (Exception e2) {
                System.out.println("error during write = " + e2);
            }
        });
        return group;
    }

    public static void appendList(Group group, String k, Object v) {
        // todo - other data types handling
        Group group1 = group.addGroup(k);
        ConvertUtils.convertToList(v).forEach(item -> {
            Group group2 = group1.addGroup(0);
            group2.add(0, item.toString());
        });
    }

    private static long writeDateMilliVector(Object value) {
        if (value instanceof Date) {
            return ((Date) value).getTime();
        } else if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value)
                    .toInstant(ZoneOffset.UTC)
                    .toEpochMilli();
        } else if (value instanceof ZonedDateTime) {
            return ((ZonedDateTime) value)
                    .toInstant()
                    .toEpochMilli();
        } else if (value instanceof OffsetDateTime) {
            return ((OffsetDateTime) value)
                    .toInstant()
                    .toEpochMilli();
        } else {
            return (long) value;
        }
    }

    public static <T> void append(Group group, String fieldName, Object value, MessageType schema) {

        if (schema.getType(fieldName).asPrimitiveType().getPrimitiveTypeName().equals(INT64)) {
            group.append(fieldName, writeDateMilliVector(value));
        } else if (schema.getType(fieldName).asPrimitiveType().getPrimitiveTypeName().equals(BINARY)) {
            group.append(fieldName, serializeValue(value));
        } else {

                if (value instanceof Integer) {
                    group.append(fieldName, (int) value);
                } else if (value instanceof Float) {
                    group.append(fieldName, (float) value);
                } else if (value instanceof Double) {
                    group.append(fieldName, (double) value);
                } else if (value instanceof Long) {
                    group.append(fieldName, (long) value);
                } else if (value instanceof NanoTime) {
                    group.append(fieldName, (NanoTime) value);
                } else if (value instanceof Boolean) {
                    group.append(fieldName, (boolean) value);
                } else if (value instanceof Binary) {
                    group.append(fieldName, (Binary) value);
                } else if (value == null) {
                    // todo do stuff?
                    throw new RuntimeException("stuff");
                } else {
                    group.append(fieldName, serializeValue(value));
                }

        }
    }

    private static String serializeValue(Object val){
        if (val instanceof Node) {
            Node value = (Node) val;
            Map<String, Object> allProperties = value.getAllProperties();
            allProperties.put(FIELD_ID, value.getId());
            allProperties.put(FIELD_LABELS, labelStrings(value));
            return JsonUtil.writeValueAsString(allProperties);
        }
        if (val instanceof Relationship) {
            Relationship value = (Relationship) val;
            Map<String, Object> allProperties = value.getAllProperties();
            allProperties.put(FIELD_ID, value.getId());
            allProperties.put(FIELD_SOURCE_ID, value.getStartNodeId());
            allProperties.put(FIELD_TARGET_ID, value.getEndNodeId());
            allProperties.put(FIELD_TYPE, value.getType().name());
            return JsonUtil.writeValueAsString(allProperties);
        }
        if (val instanceof Map) {
            return JsonUtil.writeValueAsString(val);
        }
        return val.toString();
    }

    // todo - try putting MessageTypeBuilder instead of GroupBuilder
    public static void getItems(String fieldName, org.apache.parquet.schema.Types.GroupBuilder test, PrimitiveType.PrimitiveTypeName type, LogicalTypeAnnotation logicalType) {
        PrimitiveBuilder<PrimitiveType> optional = optional(type);
        if (type == null) {
            optional.as(logicalType);
        }
        test.addField(optionalList().element(Types.optional(BINARY).named("element")).named(fieldName));
    }

    public static void getItems(String fieldName, org.apache.parquet.schema.Types.GroupBuilder test, PrimitiveType.PrimitiveTypeName type) {
        getItems(fieldName, test, type, null);
    }

    static void toField(String fieldName, Set<String> propertyTypes, org.apache.parquet.schema.Types.GroupBuilder builder) {

        if (propertyTypes.size() > 1) {
            // multi type handled as a string
            getSchemaFieldAssembler(builder, fieldName, "String");
        } else {
            getSchemaFieldAssembler(builder, fieldName, propertyTypes.iterator().next());
        }
    }

//    private static void getSchemaFieldAssembler(String fieldName, String propertyType, org.apache.parquet.schema.Types.GroupBuilder builder) {
//        getSchemaFieldAssembler(fieldName, propertyType, builder, false);
//    }

//    public static void addItem(String fieldName, org.apache.parquet.schema.Types.GroupBuilder builder, PrimitiveType.PrimitiveTypeName type) {
//        builder.addField(Types.optional(type).as().named(fieldName));
//    }

    public static void getField(GroupBuilder builder, PrimitiveType.PrimitiveTypeName type, String fieldName) {
        builder.addField(optional(type).named(fieldName));
    }

    private static void getSchemaFieldAssembler(GroupBuilder builder, String fieldName, String propertyType) {
        propertyType = propertyType.toUpperCase();

        switch (propertyType) {

            case "BOOLEAN" -> builder.addField(optional(BOOLEAN).named(fieldName));
            case "LONG" -> builder.addField(optional(INT64).named(fieldName));
            case "DOUBLE" -> builder.addField(optional(DOUBLE).named(fieldName));
            case "DATETIME" -> {
                // todo - evaluate DateTimeValue.parse(), maybe is better to convert...
                //  in case add to List.of("DURATION"....)
                builder.addField(optional(INT64).as(TimestampLogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)).named(fieldName));
            }
            case "LOCALDATETIME" -> {
                builder.addField(optional(INT64).as(TimestampLogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS)).named(fieldName));
            }
            case "DATE" -> {
                builder.addField(optional(INT64).as(DateLogicalTypeAnnotation.dateType()).named(fieldName));
            }
            case "DURATION", "NODE", "RELATIONSHIP", "POINT" -> {
//                if (!multiType) {
//                    fieldName = getFieldName(fieldName, propertyType);
//                }
                builder.addField(optional(BINARY).named(fieldName));
//                getSchemaFieldAssembler(fieldName, assembler, ParquetTypes.DURATION.getType(), BaseTypeBuilder::stringType);
            }
            //                getSchemaFieldAssembler(fieldName, assembler, ParquetTypes.NODE.getType(), BaseTypeBuilder::stringType);
            //                getSchemaFieldAssembler(fieldName, assembler, ParquetTypes.RELATIONSHIP.getType(), BaseTypeBuilder::stringType);
            //                getSchemaFieldAssembler(fieldName, assembler, ParquetTypes.POINT.getType(), BaseTypeBuilder::stringType);

            // todo - vedere se intanto funziona il resto
//            case "DATETIMEARRAY" -> {
//                getArraySchemaFieldAssembler(fieldName, builder, LogicalTypes.timestampMicros(), BaseTypeBuilder::longType);
//            }
//            case "LOCALDATETIMEARRAY" -> {
//                getArraySchemaFieldAssembler(fieldName, assembler, LogicalTypes.localTimestampMicros(), BaseTypeBuilder::longType);
//            }
//            case "DATEARRAY" -> {
//                getArraySchemaFieldAssembler(fieldName, assembler, LogicalTypes.date(), BaseTypeBuilder::intType);
//            }
//            case "BOOLEANARRAY" -> {
//                getItems(fieldName, assembler).booleanType();
//            }
//            case "LONGARRAY" -> {
//                getItems(fieldName, assembler).longType();
//            }
//            case "DOUBLEARRAY" -> {
//                getItems(fieldName, assembler).doubleType();
//            }
//            case "DURATIONARRAY" -> {
//                getArraySchemaFieldAssembler(fieldName, assembler, ParquetTypes.DURATION.getType(), BaseTypeBuilder::stringType);
//            }
//            case "STRINGARRAY" -> {
//                getItems(fieldName, assembler).stringType();
//            }
//            case "POINTARRAY" -> {
//                getArraySchemaFieldAssembler(fieldName, assembler, ParquetTypes.POINT.getType(), BaseTypeBuilder::stringType);
//            }
            default -> {
                if (propertyType.endsWith("ARRAY")) {
                    // todo - cambiare questo // todo
                    getItems(fieldName, builder, BINARY);//.stringType();
//// todo
                    // todo
//                    builder.addField(optional(BINARY).named(fieldName));
                } else {
                    // todo - in case, put here POINT, DURATION, NODE and RELATIONSHIP, WITH --> fieldName = getFieldName(fieldName, propertyType);
//                    assembler.optionalString(fieldName);
                    builder.addField(optional(BINARY).named(fieldName));
                }
            }
        }
    }

    public static String getFieldName(String fieldName, String propertyType) {
        // in case of multiple types with the same name, we add a suffix, i.e. `fieldName__<TYPEFIELD>`
        return fieldName + TYPE_SEP + propertyType;
    }

    public static String FIELD_ID = "_id";
    public static String FIELD_LABELS = "_labels";
    public static String FIELD_SOURCE_ID = "_source_id";
    public static String FIELD_TARGET_ID = "_target_id";
    public static String FIELD_TYPE = "_type";
}
