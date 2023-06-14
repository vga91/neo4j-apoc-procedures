package apoc.export.parquet;

import apoc.meta.Types;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.avro.SchemaBuilder.BaseTypeBuilder;

public class ParquetUtil {
    public static GenericData genericData;
    static {
        genericData = new GenericData();
        genericData.addLogicalTypeConversion(new TimeConversions.DateConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());
        for (ParquetTypes type: ParquetTypes.values()) {
            genericData.addLogicalTypeConversion(type.getWriteConversion());
        }
    }

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
                if (innerType == Types.LIST || innerType == Types.MAP ) {
                    return "ANYARRAY";
                }
                return fromMetaType(innerType) + "ARRAY";
            default:
                return type.name().replaceAll("_", "").toUpperCase();
        }
    }

    public static GenericRecord mapToRecord(Schema schema, Map<String, Object> map) {
        GenericRecord flattened = new GenericData.Record(schema);
        map.forEach((k, v)-> {
            try {
                flattened.put(k, v);
            } catch (Exception e) {
                if (!e.getMessage().contains("Not a valid schema field")) {
                    throw new RuntimeException(e);
                }
                String s = fromMetaType(Types.of(v));
                flattened.put(getFieldName(k, s), v);
            }
        });
        return flattened;
    }

    public static SchemaBuilder.TypeBuilder<SchemaBuilder.FieldAssembler<Schema>> getItems(String fieldName, SchemaBuilder.FieldAssembler<Schema> test) {
        return test.name(fieldName).type().optional().array().items();
    }

    private static void getSchemaFieldAssembler(String fieldName,
                                                SchemaBuilder.FieldAssembler<Schema> test,
                                                LogicalType timestampMicros,
                                                Function<SchemaBuilder.TypeBuilder<Schema>, Schema> function) {
        Schema schema1 = timestampMicros.addToSchema(function.apply(SchemaBuilder.builder()));
        test.name(fieldName).type().optional().type(schema1);
    }

    private static void getArraySchemaFieldAssembler(String fieldName,
                                                     SchemaBuilder.FieldAssembler<Schema> test,
                                                     LogicalType timestampMicros,
                                                     Function<SchemaBuilder.TypeBuilder<Schema>, Schema> function) {
        Schema schema1 = timestampMicros.addToSchema(function.apply(SchemaBuilder.builder()));
        getItems(fieldName, test).type(schema1);
    }

    static void toField(String fieldName, Set<String> propertyTypes, SchemaBuilder.FieldAssembler<Schema> assembler) {

        if (propertyTypes.size() > 1) {
            propertyTypes.forEach(type -> {
                getSchemaFieldAssembler(fieldName, type, assembler, true);
            });
        } else {
            getSchemaFieldAssembler(fieldName, propertyTypes.iterator().next(), assembler);
        }
    }

    private static void getSchemaFieldAssembler(String fieldName, String propertyType, SchemaBuilder.FieldAssembler<Schema> assembler) {
        getSchemaFieldAssembler(fieldName, propertyType, assembler, false);
    }

    private static void getSchemaFieldAssembler(String fieldName, String propertyType, SchemaBuilder.FieldAssembler<Schema> assembler, boolean multiType) {
        propertyType = propertyType.toUpperCase();

        if (multiType) {
            fieldName = getFieldName(fieldName, propertyType);
        }
        switch (propertyType) {
            case "BOOLEAN" -> assembler.optionalBoolean(fieldName);
            case "LONG" -> assembler.optionalLong(fieldName);
            case "DOUBLE" -> assembler.optionalDouble(fieldName);
            case "DATETIME" -> {
                getSchemaFieldAssembler(fieldName, assembler, LogicalTypes.timestampMicros(), BaseTypeBuilder::longType);
            }
            case "LOCALDATETIME" -> {
                getSchemaFieldAssembler(fieldName, assembler, LogicalTypes.localTimestampMicros(), BaseTypeBuilder::longType);
            }
            case "DATE" -> {
                getSchemaFieldAssembler(fieldName, assembler, LogicalTypes.date(), BaseTypeBuilder::intType);
            }
            case "DURATION" -> {
                getSchemaFieldAssembler(fieldName, assembler, ParquetTypes.DURATION.getType(), BaseTypeBuilder::stringType);
            }
            case "NODE" -> {
                getSchemaFieldAssembler(fieldName, assembler, ParquetTypes.NODE.getType(), BaseTypeBuilder::stringType);
            }
            case "RELATIONSHIP" -> {
                getSchemaFieldAssembler(fieldName, assembler, ParquetTypes.RELATIONSHIP.getType(), BaseTypeBuilder::stringType);
            }
            case "POINT" -> {
                getSchemaFieldAssembler(fieldName, assembler, ParquetTypes.POINT.getType(), BaseTypeBuilder::stringType);
            }
            case "DATETIMEARRAY" -> {
                getArraySchemaFieldAssembler(fieldName, assembler, LogicalTypes.timestampMicros(), BaseTypeBuilder::longType);
            }
            case "LOCALDATETIMEARRAY" -> {
                getArraySchemaFieldAssembler(fieldName, assembler, LogicalTypes.localTimestampMicros(), BaseTypeBuilder::longType);
            }
            case "DATEARRAY" -> {
                getArraySchemaFieldAssembler(fieldName, assembler, LogicalTypes.date(), BaseTypeBuilder::intType);
            }
            case "BOOLEANARRAY" -> {
                getItems(fieldName, assembler).booleanType();
            }
            case "LONGARRAY" -> {
                getItems(fieldName, assembler).longType();
            }
            case "DOUBLEARRAY" -> {
                getItems(fieldName, assembler).doubleType();
            }
            case "DURATIONARRAY" -> {
                getArraySchemaFieldAssembler(fieldName, assembler, ParquetTypes.DURATION.getType(), BaseTypeBuilder::stringType);
            }
            case "STRINGARRAY" -> {
                getItems(fieldName, assembler).stringType();
            }
            case "POINTARRAY" -> {
                getArraySchemaFieldAssembler(fieldName, assembler, ParquetTypes.POINT.getType(), BaseTypeBuilder::stringType);
            }
            default -> {
                if (propertyType.endsWith("ARRAY")) {
                    getItems(fieldName, assembler).stringType();
                } else {
                    assembler.optionalString(fieldName);
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
