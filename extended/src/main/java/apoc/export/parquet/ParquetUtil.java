package apoc.export.parquet;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphdb.Node;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static apoc.export.parquet.ParquetUtil.DurationType.DURATION_VALUE;
import static apoc.export.parquet.ParquetUtil.PointType.POINT_VALUE;
import static org.apache.avro.SchemaBuilder.BaseTypeBuilder;

public class ParquetUtil {

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
            case "DATE_TIME" -> {
                return getSchemaFieldAssembler(fieldName, test, LogicalTypes.timestampMicros(), BaseTypeBuilder::longType);
            }
            case "LOCAL_TIME" -> {
                // todo
                return null;
            }
            case "TIME" -> {
                // todo
                return null;
            }
            case "LOCAL_DATE_TIME" -> {
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
                Schema schema11 = SchemaBuilder.builder().map().values().stringType();

                // todo - is needed "new NodeType()" --> alternative????
                Schema schema21 = new NodeType().addToSchema(schema11);
                return test.name(fieldName).type().optional().type(schema21);
            }
            // todo...
//                    return new Field(fieldName, FieldType.nullable(Types.MinorType.STRUCT.getType()), null);
            case "RELATIONSHIP", "POINT" -> {
                // todo...
                return getSchemaFieldAssembler(fieldName, test, ParquetTypes.POINT.getType(), BaseTypeBuilder::stringType);
            }
            case "MAP" ->
                // todo - test with this export.query, since map is not allowed as a property
                    throw new RuntimeException("todo - how to deal with it??");
            case "DATE_TIME_ARRAY" -> {
                // todo...
                return null;
            }
            case "LOCAL_TIME_ARRAY" -> {
                // todo...
                return null;
            }
            case "TIME_ARRAY" -> {
                return null;
            }
            // todo...
            case "LOCAL_DATE_TIME_ARRAY" -> {
                return null;
            }
            // todo...
            case "DATE_ARRAY" -> {
                return getArraySchemaFieldAssembler(fieldName, test, LogicalTypes.localTimestampMicros(), BaseTypeBuilder::intType);
            }
            // todo...
            case "BOOLEAN_ARRAY" -> {
                return getItems(fieldName, test).booleanType();
            }
            // todo...
            case "LONG_ARRAY" -> {
                return getItems(fieldName, test).longType();
            }
            // todo...
            case "DOUBLE_ARRAY" -> {
                return getItems(fieldName, test).doubleType();
            }
            case "DURATION_ARRAY" -> {
                return getArraySchemaFieldAssembler(fieldName, test, ParquetTypes.DURATION.getType(), BaseTypeBuilder::stringType);
            }
            // todo...
            case "STRING_ARRAY" -> {
                return getItems(fieldName, test).stringType();
            }
            case "POINT_ARRAY" -> {
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
        return fieldName + "__" + propertyType;
    }


    public static abstract class CustomConversion<T> extends Conversion<T> {

        @Override
        public Class<T> getConvertedType() {
            return null;
        }

//        @Override
//        public String getLogicalTypeName() {
//            return null;
//        }

        public abstract T parseValue(CharSequence value);

        public CharSequence serializeValue(T value) {
            return value.toString();
        }

        @Override
        public T fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
            if (value == null) {
                return null;
            }
            return parseValue(value);
        }

        @Override
        public CharSequence toCharSequence(T value, Schema schema, LogicalType type) {
            return serializeValue(value);
        }
    }


    public static abstract class CustomType extends LogicalType {

        public CustomType(String logicalTypeName) {
            super(logicalTypeName);
        }

        public abstract String getLogicalTypeName();

        @Override
        public void validate(Schema schema) {
            super.validate(schema);
            if (schema.getType() != Schema.Type.STRING) {
                throw new IllegalArgumentException(getLogicalTypeName() + " can only be used with an underlying string type");
            }
        }
    }

    public static class NodeEntityConversion extends Conversion<Node> {
//        public NodeEntityConversion() {
//            super();
//        }

        @Override
        public Class<Node> getConvertedType() {
            return null;
        }

        @Override
        public String getLogicalTypeName() {
            return null;
        }

        @Override
        public Node fromMap(Map<?, ?> value, Schema schema, LogicalType type) {
            // todo - for import????
            return super.fromMap(value, schema, type);
        }

        @Override
        public Map<?, ?> toMap(Node value, Schema schema, LogicalType type) {
            // todo - for export????
            return super.toMap(value, schema, type);
        }
    }


    public static class PointValueConversion extends CustomConversion<PointValue> {

        @Override
        public Class<PointValue> getConvertedType() {
            return PointValue.class;
        }

        @Override
        public PointValue parseValue(CharSequence value) {
            return PointValue.parse(value);
        }

        @Override
        public String getLogicalTypeName() {
            return POINT_VALUE;
        }
//
//        @Override
//        public PointValue fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
//            if (value == null) {
//                return null;
//            }
//            return PointValue.parse(value);
//        }
//
//        @Override
//        public CharSequence toCharSequence(PointValue value, Schema schema, LogicalType type) {
//            return value.toString();
//        }
    }

    public static class PointType extends CustomType {

        public static final String POINT_VALUE = "point-value";

        @Override
        public String getLogicalTypeName() {
            return POINT_VALUE;
        }

        public PointType() {
            super(POINT_VALUE);
        }
        //        public static String logicalTypeName = "";

//        public PointType(String logicalTypeName) {
//            super(logicalTypeName);
//        }
//        private static PointType INSTANCE;
//
//        public static PointType getInstance() {
//            if(INSTANCE == null) {
//                INSTANCE = new PointType();
//            }
//            return INSTANCE;
//        }

//        public static final String POINT_VALUE = "point-value";

//        public PointType() {
//            super(POINT_VALUE);
//        }

//        @Override
//        public void validate(Schema schema) {
//            super.validate(schema);
//            if (schema.getType() != Schema.Type.STRING) {
//                // TODO - error..
//                throw new IllegalArgumentException("Local timestamp (micros) can only be used with an underlying long type");
//            }
//        }
    }


    public static class DurationValueConversion extends CustomConversion<DurationValue> {

        @Override
        public Class<DurationValue> getConvertedType() {
            return DurationValue.class;
        }

        @Override
        public String getLogicalTypeName() {
            return DURATION_VALUE;
        }

        @Override
        public DurationValue parseValue(CharSequence value) {
            return DurationValue.parse(value);
        }
    }

    public static class DurationType extends CustomType {

        public static final String DURATION_VALUE = "duration-value";

        public DurationType() {
            super(DURATION_VALUE);
        }

        @Override
        public String getLogicalTypeName() {
            return DURATION_VALUE;
        }
    }

    public static class NodeType extends LogicalType {

        public static final String NEO4J_NODE = "neo4j-node";

        public NodeType() {
            super(NEO4J_NODE);
        }

        @Override
        public void validate(Schema schema) {
            super.validate(schema);
            if (schema.getType() != Schema.Type.MAP) {
                // TODO - error..
                throw new IllegalArgumentException("Local timestamp (micros) can only be used with an underlying long type");
            }
        }
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
