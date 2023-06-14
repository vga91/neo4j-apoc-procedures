package apoc.export.parquet;

import apoc.util.JsonUtil;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.neo4j.kernel.impl.core.NodeEntity;
import org.neo4j.kernel.impl.core.RelationshipEntity;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;

import java.util.Map;

import static apoc.export.parquet.CustomTypes.DurationType.DURATION_VALUE;
import static apoc.export.parquet.CustomTypes.NodeType.NEO4J_NODE;
import static apoc.export.parquet.CustomTypes.PointType.POINT_VALUE;
import static apoc.export.parquet.ParquetUtil.*;
import static apoc.util.Util.labelStrings;

public class CustomConversions {

    public static abstract class CustomConversion<T> extends Conversion<T> {

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


    public static class RelationshipEntityConversion extends Conversion<RelationshipEntity> {

        @Override
        public CharSequence toCharSequence(RelationshipEntity value, Schema schema, LogicalType type) {
            Map<String, Object> allProperties = value.getAllProperties();
            allProperties.put(FIELD_ID, value.getId());
            allProperties.put(FIELD_SOURCE_ID, value.getStartNodeId());
            allProperties.put(FIELD_TARGET_ID, value.getEndNodeId());
            allProperties.put(FIELD_TYPE, value.getType().name());
            return JsonUtil.writeValueAsString(allProperties);
        }

        @Override
        public Class<RelationshipEntity> getConvertedType() {
            return RelationshipEntity.class;
        }

        @Override
        public String getLogicalTypeName() {
            return CustomTypes.RelationshipType.NEO4J_REL;
        }
    }

    public static class NodeLoadConversion extends EntityLoadConversion {
        @Override
        public String getLogicalTypeName() {
            return NEO4J_NODE;
        }
    }

    public static class RelationshipLoadConversion extends EntityLoadConversion {
        @Override
        public String getLogicalTypeName() {
            return CustomTypes.RelationshipType.NEO4J_REL;
        }
    }

    public abstract static class EntityLoadConversion extends CustomConversion<Map> {

        @Override
        public Map parseValue(CharSequence value) {
            return JsonUtil.parse(value.toString(), null, Map.class);
        }

        @Override
        public Class<Map> getConvertedType() {
            return Map.class;
        }
    }

    public static class NodeEntityConversion extends CustomConversion<NodeEntity> {

        @Override
        public Class<NodeEntity> getConvertedType() {
            return NodeEntity.class;
        }

        @Override
        public String getLogicalTypeName() {
            return NEO4J_NODE;
        }

        @Override
        public NodeEntity parseValue(CharSequence value) {
            return null;
        }

        @Override
        public CharSequence serializeValue(NodeEntity value) {
            Map<String, Object> allProperties = value.getAllProperties();
            allProperties.put(FIELD_ID, value.getId());
            allProperties.put(FIELD_LABELS, labelStrings(value));
            return JsonUtil.writeValueAsString(allProperties);
        }

//        @Override
//        public Node fromMap(Map<?, ?> value, Schema schema, LogicalType type) {
//            // todo - for import????
//            return super.fromMap(value, schema, type);
//        }
//
//        @Override
//        public Map<?, ?> toMap(Node value, Schema schema, LogicalType type) {
//            // todo - for export????
//            return super.toMap(value, schema, type);
//        }
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
}
