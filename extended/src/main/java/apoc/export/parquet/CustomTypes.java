//package apoc.export.parquet;
//
//import org.apache.avro.LogicalType;
//import org.apache.avro.Schema;
//
//public class CustomTypes {
//
//    public static abstract class AbstractCustomType extends LogicalType {
//
//        public AbstractCustomType(String logicalTypeName) {
//            super(logicalTypeName);
//        }
//
//        public abstract String getLogicalTypeName();
//
//        @Override
//        public void validate(Schema schema) {
//            super.validate(schema);
//            if (schema.getType() != Schema.Type.STRING) {
//                throw new IllegalArgumentException(getLogicalTypeName() + " can only be used with an underlying string type");
//            }
//        }
//    }
//
//    public static class PointType extends AbstractCustomType {
//
//        public static final String POINT_VALUE = "point-value";
//
//        @Override
//        public String getLogicalTypeName() {
//            return POINT_VALUE;
//        }
//
//        public PointType() {
//            super(POINT_VALUE);
//        }
//    }
//
//    public static class DurationType extends AbstractCustomType {
//
//        public static final String DURATION_VALUE = "duration-value";
//
//        public DurationType() {
//            super(DURATION_VALUE);
//        }
//
//        @Override
//        public String getLogicalTypeName() {
//            return DURATION_VALUE;
//        }
//    }
//
//    public static class NodeType extends AbstractCustomType {
//
//        public static final String NEO4J_NODE = "neo4j-node";
//
//        public NodeType() {
//            super(NEO4J_NODE);
//        }
//
//        @Override
//        public String getLogicalTypeName() {
//            return NEO4J_NODE;
//        }
//    }
//
//    public static class RelationshipType extends AbstractCustomType {
//
//        public static final String NEO4J_REL = "relationship-node";
//
//        public RelationshipType() {
//            super(NEO4J_REL);
//        }
//
//        @Override
//        public String getLogicalTypeName() {
//            return NEO4J_REL;
//        }
//    }
//}
