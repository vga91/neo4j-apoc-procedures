package apoc.export.parquet;

//import apoc.meta.Types;
//import org.apache.avro.LogicalType;
//import org.apache.avro.LogicalTypes;
//import org.apache.avro.Schema;
//import org.apache.avro.SchemaBuilder;
//import org.apache.avro.data.TimeConversions;
//import org.apache.avro.generic.GenericData;
//import org.apache.avro.generic.GenericRecord;
import apoc.convert.ConvertUtils;
import org.apache.avro.Schema;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

//import static org.apache.avro.SchemaBuilder.BaseTypeBuilder;
import static org.apache.parquet.schema.Types.MessageTypeBuilder;
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

//        GenericRecord flattened = new GenericData.Record(schema);
        map.forEach((k, v)-> {
            try {
                Type type = schema.getType(k);
                    if (type.getLogicalTypeAnnotation() instanceof ListLogicalTypeAnnotation) {
//                    SimpleGroup simpleGroup = new SimpleGroup((GroupType) type);
//                    simpleGroup.add("list", "prova");
//                    simpleGroup.add("list", "prova2");

//                    group.addGroup("kids");
                    extracted(group, k, v);
//                    Group group2 = group1.addGroup(0);
//                    group2.add(0, "test");
//                    group2 = group1.addGroup(0);
//                    group2.add(0, "test33");
//                    group2.add(1, "test2");
//                    Group group22 = group1.addGroup(0);
//                    group22.add(0, "test23");
//                    group22.add(0, "test234444");
//                    group22.add(1, "test24");
//                    ConvertUtils.convertToList(v).forEach(item -> group1.add("list", item.toString()));
//                    ConvertUtils.convertToList(v).forEach(item -> group2.add(0, item.toString()));//.add("list", item.toString()));
                } else {
                    append(group, k, v, schema);
                }
//                group.append(k, (String) v);

//                group.addGroup(k, v);
            } catch (Exception e) {

                try {
                    // todo - creare config, per gestire sia multitype che type "speciali" ... altrimento casto tutto e vaffanculo
                    // todo - decommentare questo e.getMessage() probabilmente?
//                    if (!e.getMessage().contains("Not a valid schema field")) {
//                        throw new RuntimeException(e);
//                    }
                    String s = fromMetaType(apoc.meta.Types.of(v));

                    String fieldName = getFieldName(k, s);
                    Type type = schema.getType(fieldName);
//                logicalTypeAnnotation.accept();

//                    if (type instanceof PrimitiveType && ((PrimitiveType) type).getPrimitiveTypeName().equals(INT64)) {
//                        // todo - demock it, just to see if converts well
//                        append(group, fieldName, (long) 123L);
//                    } else if (type instanceof PrimitiveType && ((PrimitiveType) type).getPrimitiveTypeName().equals(INT32)) {
//                        // todo - demock it, just to see if converts well
//                        append(group, fieldName, (int) 456);
//                    } else
                        if (type.getLogicalTypeAnnotation() instanceof ListLogicalTypeAnnotation) {
                        System.out.println("type = " + type);
//                        Group group1 = group.addGroup(k);
//                        ConvertUtils.convertToList(v).forEach(item -> group1.add("list", item.toString()));
                    } else {
                        append(group, fieldName, v, schema);
                    }
//                group.append(k, s);
//                flattened.put(getFieldName(k, s), v);
                } catch (Exception e2) {
                    System.out.println("e = " + e2);
                }
            }
        });
        return group;
    }

    public static void extracted(Group group, String k, Object v) {
        Group group1 = group.addGroup(k);
        ConvertUtils.convertToList(v).forEach(item -> {
            Group group2 = group1.addGroup(0);
            group2.add(0, item.toString());
//                        group1.add("list", item.toString())
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
                group.append(fieldName, value.toString());
            }
        }
    }

    // todo - try putting MessageTypeBuilder instead of GroupBuilder
    public static void getItems(String fieldName, org.apache.parquet.schema.Types.GroupBuilder test, PrimitiveType.PrimitiveTypeName type, LogicalTypeAnnotation logicalType) {
        PrimitiveBuilder<PrimitiveType> optional = optional(type);
        if (type == null) {
            optional.as(logicalType);
        }
        test.addField(optionalList().element(Types.optional(BINARY).named("element"))/*.setElementType(optional.named("element"))*/.named(fieldName));
    }

    public static void getItems(String fieldName, org.apache.parquet.schema.Types.GroupBuilder test, PrimitiveType.PrimitiveTypeName type) {
        getItems(fieldName, test, type, null);
    }

//    private static void getSchemaFieldAssembler(String fieldName,
//                                                SchemaBuilder.FieldAssembler<Schema> test,
//                                                LogicalType timestampMicros,
//                                                Function<SchemaBuilder.TypeBuilder<Schema>, Schema> function) {
////        Schema schema1 = timestampMicros.addToSchema(function.apply(SchemaBuilder.builder()));
//        test.name(fieldName).type().optional().type(schema1);
//    }
//
//    private static void getArraySchemaFieldAssembler(String fieldName,
//                                                     SchemaBuilder.FieldAssembler<Schema> test,
//                                                     LogicalType timestampMicros,
//                                                     Function<SchemaBuilder.TypeBuilder<Schema>, Schema> function) {
//        Schema schema1 = timestampMicros.addToSchema(function.apply(SchemaBuilder.builder()));
//        getItems(fieldName, test, schema1);//.type(schema1);
//    }
//
    static void toField(String fieldName, Set<String> propertyTypes, org.apache.parquet.schema.Types.GroupBuilder builder) {

        if (propertyTypes.size() > 1) {
            // todo - change here
            propertyTypes.forEach(type -> {
                getSchemaFieldAssembler(fieldName, type, builder, true);
            });
        } else {
            getSchemaFieldAssembler(fieldName, propertyTypes.iterator().next(), builder);
        }
    }

    private static void getSchemaFieldAssembler(String fieldName, String propertyType, org.apache.parquet.schema.Types.GroupBuilder builder) {
        getSchemaFieldAssembler(fieldName, propertyType, builder, false);
    }

//    public static void addItem(String fieldName, org.apache.parquet.schema.Types.GroupBuilder builder, PrimitiveType.PrimitiveTypeName type) {
//        builder.addField(Types.optional(type).as().named(fieldName));
//    }

    public static void getField(GroupBuilder builder, PrimitiveType.PrimitiveTypeName type, String fieldName) {
        builder.addField(optional(type).named(fieldName));
    }

    private static void getSchemaFieldAssembler(String fieldName, String propertyType, org.apache.parquet.schema.Types.GroupBuilder builder, boolean multiType) {
        propertyType = propertyType.toUpperCase();

//        if (multiType) {
//            // todo - change here -- with point and duration as well
//            fieldName = getFieldName(fieldName, propertyType);
//        }
        List<String> neo4jTypes = List.of("DURATION", "NODE", "RELATIONSHIP", "POINT");
        if(multiType || neo4jTypes.contains(propertyType)) {
            fieldName = getFieldName(fieldName, propertyType);
        }
        switch (propertyType) {

            case "BOOLEAN" -> builder.addField(optional(BOOLEAN).named(fieldName));
            case "LONG" -> builder.addField(optional(INT64).named(fieldName));
            case "DOUBLE" -> builder.addField(optional(DOUBLE).named(fieldName));
            case "DATETIME" -> {
                // todo - evaluate DateTimeValue.parse(), maybe is better to convert...
                //  in case add to List.of("DURATION"....)

                builder.addField(optional(INT64).as(TimestampLogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)).named(fieldName));
//                addItem(fieldName, builder, BINARY);
            }
            case "LOCALDATETIME" -> {
                builder.addField(optional(INT64).as(TimestampLogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS)).named(fieldName));
//                getSchemaFieldAssembler(fieldName, assembler, LogicalTypes.localTimestampMicros(), BaseTypeBuilder::longType);
            }
            case "DATE" -> {
                builder.addField(optional(INT64).as(DateLogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS)).named(fieldName));

//                getSchemaFieldAssembler(fieldName, assembler, LogicalTypes.date(), BaseTypeBuilder::intType);
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
