package apoc.export.parquet;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;

import java.util.Set;
import java.util.function.Function;

import static apoc.export.parquet.ParquetUtil.DurationType.DURATION_VALUE;
import static apoc.export.parquet.ParquetUtil.PointType.POINT_VALUE;
import static org.apache.avro.SchemaBuilder.BaseTypeBuilder;

public class ParquetUtil {

    // todo - ParquetUtil
    public static GenericData genericDataLoad;
    static {
        genericDataLoad = new GenericData();
        // need to add logicalTime Support
        genericDataLoad.addLogicalTypeConversion(new TimeConversions.DateConversion());
//        timeSupport.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
        genericDataLoad.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
        genericDataLoad.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
        genericDataLoad.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());
//        genericData.addLogicalTypeConversion(new ParquetUtil.DurationValueConversion());
        for (ParquetTypes type: ParquetTypes.values()) {
            genericDataLoad.addLogicalTypeConversion(type.getReadConversion());
        }
    }

    // todo - ParquetUtil
    public static GenericData genericData;
    static {
        genericData = new GenericData();
        // need to add logicalTime Support
        genericData.addLogicalTypeConversion(new TimeConversions.DateConversion());
//        timeSupport.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());
//        genericData.addLogicalTypeConversion(new ParquetUtil.DurationValueConversion());
        for (ParquetTypes type: ParquetTypes.values()) {
            genericData.addLogicalTypeConversion(type.getWriteConversion());//new ParquetUtil.PointValueConversion());
        }
    }

    public static final String TYPE_SEP = "___";

    // todo - creare analogo senza apoc.schema, perché senno non funziona mai senza jar core....
    //  renderlo configurabile...

    public static SchemaBuilder.TypeBuilder<SchemaBuilder.FieldAssembler<Schema>> getItems(String fieldName, SchemaBuilder.FieldAssembler<Schema> test) {
        return test.name(fieldName).type().optional().array().items();
    }

    private static SchemaBuilder.FieldAssembler<Schema> getSchemaFieldAssembler(String fieldName,
                                                                                SchemaBuilder.FieldAssembler<Schema> test,
                                                                                LogicalType timestampMicros,
                                                                                Function<SchemaBuilder.TypeBuilder<Schema>, Schema> function) {
        Schema schema1 = timestampMicros.addToSchema(function.apply(SchemaBuilder.builder()));
        return test.name(fieldName).type().optional().type(schema1);
    }

    private static SchemaBuilder.FieldAssembler<Schema> getArraySchemaFieldAssembler(String fieldName,
                                                                                     SchemaBuilder.FieldAssembler<Schema> test,
                                                                                     LogicalType timestampMicros,
                                                                                     Function<SchemaBuilder.TypeBuilder<Schema>, Schema> function) {
        Schema schema1 = timestampMicros.addToSchema(function.apply(SchemaBuilder.builder()));
        return getItems(fieldName, test).type(schema1);
    }

//    static SchemaBuilder.FieldAssembler<Schema> toField(String fieldName, Set<String> propertyTypes, SchemaBuilder.FieldAssembler<Schema> test) {
//        return toField(fieldName, propertyTypes, test, false);
//    }

    static SchemaBuilder.FieldAssembler<Schema> toField(String fieldName, Set<String> propertyTypes, SchemaBuilder.FieldAssembler<Schema> test) {

        // TODO - IF OPTIONALSTRING --> UTIL.TOJSON(...) AND UTIL.FROM(JSON)

        if (propertyTypes.size() > 1) {
            // return string type
            // todo - maybe just return FieldAssembler??
            propertyTypes.forEach(type -> {
                getSchemaFieldAssembler(fieldName, type, test, true);
            });
            // todo - maybe void
            return null;
//            return test.optionalString(fieldName);
        } else {
            // convert to RelatedType
//            final String type = propertyTypes.iterator().next().toUpperCase();
            return getSchemaFieldAssembler(fieldName, propertyTypes.iterator().next(), test);
        }
    }

    private static SchemaBuilder.FieldAssembler<Schema> getSchemaFieldAssembler(String fieldName, String propertyType, SchemaBuilder.FieldAssembler<Schema> test) {
        return getSchemaFieldAssembler(fieldName, propertyType, test, false);
    }

    private static SchemaBuilder.FieldAssembler<Schema> getSchemaFieldAssembler(String fieldName, String propertyType, SchemaBuilder.FieldAssembler<Schema> test, boolean multiType) {
        propertyType = propertyType.toUpperCase();

        if (multiType) {
            fieldName = getFieldName(fieldName, propertyType);
        }
        switch (propertyType) {
            case "BOOLEAN" -> {
                return test.optionalBoolean(fieldName);
            }

            // todo - LogicalType??? --> maybe integer as well...
            case "LONG" -> {
                return test.optionalLong(fieldName);
            }
            case "DOUBLE" -> {
                return test.optionalDouble(fieldName);
            }
            case "DATETIME" -> {
                return getSchemaFieldAssembler(fieldName, test, LogicalTypes.timestampMicros(), BaseTypeBuilder::longType);
            }
            case "LOCALTIME" -> {
                // todo
                return null;
            }
            case "TIME" -> {
                // todo
                return null;
            }
            case "LOCALDATETIME" -> {
                Schema schema2 = LogicalTypes.localTimestampMicros().addToSchema(SchemaBuilder.builder().longType());
                return test.name(fieldName).type().optional().type(schema2);
            }
            case "DATE" -> {
                // todo - check that...
                Schema schema3 = LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType());
                return test.name(fieldName).type().optional().type(schema3);
            }
//                    return new Field(fieldName, FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null);
            case "DURATION" -> {
                return getSchemaFieldAssembler(fieldName, test, ParquetTypes.DURATION.getType(), BaseTypeBuilder::stringType);
            }
            case "NODE" -> {
//                Schema schema11 = SchemaBuilder.builder().stringType();
//
//                 todo - is needed "new NodeType()" --> alternative????
//                Schema schema21 = new NodeType().addToSchema(schema11);
//                return test.name(fieldName).type().optional().type(schema21);
                return getSchemaFieldAssembler(fieldName, test, ParquetTypes.NODE.getType(), BaseTypeBuilder::stringType);
            }
            // todo...
//                    return new Field(fieldName, FieldType.nullable(Types.MinorType.STRUCT.getType()), null);
            case "RELATIONSHIP" -> {
                return getSchemaFieldAssembler(fieldName, test, ParquetTypes.RELATIONSHIP.getType(), BaseTypeBuilder::stringType);
            }
            case "POINT" -> {
                // todo...
                return getSchemaFieldAssembler(fieldName, test, ParquetTypes.POINT.getType(), BaseTypeBuilder::stringType);
            }
            case "MAP" ->
                // todo - test with this export.query, since map is not allowed as a property
                    throw new RuntimeException("todo - how to deal with it??");
            case "DATETIMEARRAY" -> {
                // todo...
                return null;
            }
            case "LOCALTIMEARRAY" -> {
                // todo...
                return null;
            }
            case "TIME_ARRAY" -> {
                return null;
            }
            // todo...
            case "LOCALDATETIMEARRAY" -> {
                return null;
            }
            // todo...
            case "DATEARRAY" -> {
                return getArraySchemaFieldAssembler(fieldName, test, LogicalTypes.localTimestampMicros(), BaseTypeBuilder::intType);
            }
            // todo...
            case "BOOLEANARRAY" -> {
                return getItems(fieldName, test).booleanType();
            }
            // todo...
            case "LONGARRAY" -> {
                return getItems(fieldName, test).longType();
            }
            // todo...
            case "DOUBLEARRAY" -> {
                return getItems(fieldName, test).doubleType();
            }
            case "DURATIONARRAY" -> {
                return getArraySchemaFieldAssembler(fieldName, test, ParquetTypes.DURATION.getType(), BaseTypeBuilder::stringType);
            }
            // todo...
            case "STRINGARRAY" -> {
                return getItems(fieldName, test).stringType();
            }
            case "POINTARRAY" -> {
                return getArraySchemaFieldAssembler(fieldName, test, ParquetTypes.POINT.getType(), BaseTypeBuilder::stringType);
            }
            // todo...
            default -> {
                return propertyType.endsWith("ARRAY")
                        ? getItems(fieldName, test).stringType()
                        : test.optionalString(fieldName);
            }
        }
    }

    public static String getFieldName(String fieldName, String propertyType) {
        return fieldName + TYPE_SEP + propertyType;
    }







    public static String FIELD_ID = "_id";
    public static String FIELD_LABELS = "_labels";
//          todo?  List.of(new Field("$data$", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null)));
    public static String FIELD_SOURCE_ID = "_source_id";
    public static String FIELD_TARGET_ID = "_target_id";
    public static String FIELD_TYPE = "_type";

//    public static Field FIELD_ID = new Field("<id>", FieldType.nullable(Types.MinorType.BIGINT.getType()), null);
//    public static Field FIELD_LABELS = new Field("labels", FieldType.nullable(Types.MinorType.LIST.getType()),
//            List.of(new Field("$data$", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null)));
//    public static Field FIELD_SOURCE_ID = new Field("<source.id>", FieldType.nullable(Types.MinorType.BIGINT.getType()), null);
//    public static Field FIELD_TARGET_ID = new Field("<target.id>", FieldType.nullable(Types.MinorType.BIGINT.getType()), null);
//    public static Field FIELD_TYPE = new Field("<type>", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
}
