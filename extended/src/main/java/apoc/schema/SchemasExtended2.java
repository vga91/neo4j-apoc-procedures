//package apoc.schema;
//
//import apoc.Extended;
//import apoc.result.CompareIdxToCons;
//import apoc.result.CompareIdxToConsNodes;
//import apoc.result.CompareIdxToConsRels;
//import apoc.result.IndexConstraintEntityInfo;
//import apoc.result.IndexConstraintNodeInfo;
//import apoc.result.IndexConstraintRelationshipInfo;
//import apoc.util.Util;
//import org.apache.commons.collections4.CollectionUtils;
//import org.neo4j.common.EntityType;
//import org.neo4j.graphdb.Label;
//import org.neo4j.graphdb.Transaction;
//import org.neo4j.graphdb.schema.ConstraintDefinition;
//import org.neo4j.graphdb.schema.Schema;
//import org.neo4j.internal.kernel.api.SchemaRead;
//import org.neo4j.internal.kernel.api.TokenRead;
//import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
//import org.neo4j.internal.schema.IndexDescriptor;
//import org.neo4j.kernel.api.KernelTransaction;
//import org.neo4j.kernel.api.Statement;
//import org.neo4j.procedure.Context;
//import org.neo4j.procedure.Description;
//import org.neo4j.procedure.Mode;
//import org.neo4j.procedure.Name;
//import org.neo4j.procedure.Procedure;
//
//import java.util.*;
//import java.util.function.BiFunction;
//import java.util.function.Predicate;
//import java.util.stream.Collectors;
//import java.util.stream.Stream;
//import java.util.stream.StreamSupport;
//
////import static apoc.schema.Schemas.indexesAndConstraintsForNode;
//// import static apoc.schema.Schemas.indexesAndConstraintsForRelationships;
//import static org.neo4j.graphdb.schema.ConstraintType.UNIQUENESS;
//
//@Extended
//public class SchemasExtended2 {
//
//    @Context
//    public Transaction tx;
//
//    @Context
//    public KernelTransaction ktx;
//
//    @Procedure(value = "apoc.schema.node.compareIndexesAndConstraints", mode = Mode.SCHEMA)
//    @Description("CALL apoc.schema.node.compareIndexesAndConstraints($config) - to compare node constraints and indexes")
//    public Stream<CompareIdxToConsNodes> compareIndexesAndConstraints(@Name(value = "config",defaultValue = "{}") Map<String,Object> config) {
//        return indexesAndConstraintsForNode(config, tx, ktx,
//                compareConstraintIdxFunction(CompareIdxToConsNodes.class));
//    }
//
//    @Procedure(value = "apoc.schema.relationship.compareIndexesAndConstraints", mode = Mode.SCHEMA)
//    @Description("CALL apoc.schema.relationship.compareIndexesAndConstraints($config) - to compare rel constraints and indexes")
//    public Stream<CompareIdxToConsRels> compareIndexesAndConstraintsForRelationships(@Name(value = "config",defaultValue = "{}") Map<String,Object> config) {
//        return indexesAndConstraintsForRelationships(config, tx, ktx, compareConstraintIdxFunction(CompareIdxToConsRels.class));
//    }
//
//    private <T extends IndexConstraintEntityInfo, R extends CompareIdxToCons> BiFunction<Stream<T>, Stream<T>, Stream<R>> compareConstraintIdxFunction(Class<R> clazz) {
//        return (constraintNodeInfoStream, indexNodeInfoStream) -> {
//            final List<T> constraints = constraintNodeInfoStream.collect(Collectors.toList());
//            final List<T> indexes = indexNodeInfoStream.collect(Collectors.toList());
//
//            Map<String, R> resultMap = new TreeMap<>();
//
//            indexes.forEach(i -> {
//                final Object labelOrType = getInfoLabelOrType(i);
//                if (labelOrType instanceof String) {
//                    addCommonAndOnlyIdxProps(constraints, addObjectIfAbsent(resultMap, (String) labelOrType, clazz), i);
//                }
//                if (labelOrType instanceof List) {
//                    final List<String> labels = (List<String>) labelOrType;
//                    labels.forEach(lbl -> {
//                        addCommonAndOnlyIdxProps(constraints, addObjectIfAbsent(resultMap, lbl, clazz), i);
//                    });
//                }
//            });
//
//            constraints.forEach(i -> {
//                final Object labelOrType = getInfoLabelOrType(i);
//                addObjectIfAbsent(resultMap, (String) labelOrType, clazz).putOnlyConstraintsProps(i.name, i.properties);
//            });
//
//            return resultMap.values().stream();
//        };
//    }
//
//    private <T extends CompareIdxToCons> T addObjectIfAbsent(Map<String, T> map, String label, Class<T> clazz) {
//        return map.compute(label,
//                (k, v) -> Objects.requireNonNullElseGet(v, () -> {
//                    try {
//                        return clazz.getConstructor(String.class).newInstance(label);
//                    } catch (Exception e) {
//                        throw new RuntimeException(e);
//                    }
//                }));
//    }
//
//    private <T extends IndexConstraintEntityInfo> void addCommonAndOnlyIdxProps(List<T> constraints, CompareIdxToCons compareIdxToCons, IndexConstraintEntityInfo index) {
//        final List<String> props = index.properties;
//        // UNIQUENESS constraints also produce an analogous index, so the properties is necessary in common
//        if (UNIQUENESS.name().equals(index.type)) {
//            compareIdxToCons.addCommonProps(props);
//        } else {
//            constraints.stream()
//                    .filter(cons -> {
//                        final Object idxLabelOrType = getInfoLabelOrType(index);
//                        final Object constraintLabelOrType = getInfoLabelOrType(cons);
//                        List<String> indexProps = index.properties;
//                        List<String> consProps = cons.properties;
//                        return idxLabelOrType.equals(constraintLabelOrType)
//                                && indexProps != null
//                                && consProps != null
//                                && CollectionUtils.isEqualCollection(indexProps, consProps);
//                    })
//                    .findFirst()
//                    .ifPresentOrElse(pres -> {
//                                compareIdxToCons.addCommonProps(props);
//                                constraints.remove(pres);
//                            },
//                            () -> compareIdxToCons.putOnlyIdxProps(index.name, props)
//                    );
//        }
//    }
//
//    private <T extends IndexConstraintEntityInfo> Object getInfoLabelOrType(T idxOrCons) {
//        if (idxOrCons instanceof IndexConstraintNodeInfo) {
//            return ((IndexConstraintNodeInfo) idxOrCons).label;
//        }
//        return ((IndexConstraintRelationshipInfo) idxOrCons).relationshipType;
//    }
//
//    private Stream<IndexConstraintNodeInfo> indexesAndConstraintsForNode(
//            Map<String, Object> config, Boolean useStoredName) {
//        Schema schema = tx.schema();
//
//        SchemaConfig schemaConfig = new SchemaConfig(config);
//        Set<String> includeLabels = schemaConfig.getLabels();
//        Set<String> excludeLabels = schemaConfig.getExcludeLabels();
//
//        try (Statement ignore = ktx.acquireStatement()) {
//            TokenRead tokenRead = ktx.tokenRead();
//
//            SchemaRead schemaRead = ktx.schemaRead();
//            Iterable<IndexDescriptor> indexesIterator;
//            Iterable<ConstraintDefinition> constraintsIterator;
//            final Predicate<ConstraintDefinition> isNodeConstraint =
//                    constraintDefinition -> Util.isNodeCategory(constraintDefinition.getConstraintType());
//
//            if (includeLabels.isEmpty()) {
//
//                Iterator<IndexDescriptor> allIndex = schemaRead.indexesGetAll();
//
//                indexesIterator = getIndexesFromSchema(
//                        allIndex,
//                        index -> index.schema().entityType().equals(EntityType.NODE)
//                                && Arrays.stream(index.schema().getEntityTokenIds())
//                                .noneMatch(id -> {
//                                    try {
//                                        return excludeLabels.contains(tokenRead.nodeLabelName(id));
//                                    } catch (LabelNotFoundKernelException e) {
//                                        return false;
//                                    }
//                                }));
//
//                Iterable<ConstraintDefinition> allConstraints = schema.getConstraints();
//                constraintsIterator = StreamSupport.stream(allConstraints.spliterator(), false)
//                        .filter(isNodeConstraint)
//                        .filter(constraint ->
//                                !excludeLabels.contains(constraint.getLabel().name()))
//                        .collect(Collectors.toList());
//            } else {
//                constraintsIterator = includeLabels.stream()
//                        .filter(label -> !excludeLabels.contains(label) && tokenRead.nodeLabel(label) != -1)
//                        .flatMap(label -> {
//                            Iterable<ConstraintDefinition> constraintsForType =
//                                    schema.getConstraints(Label.label(label));
//                            return StreamSupport.stream(constraintsForType.spliterator(), false)
//                                    .filter(isNodeConstraint);
//                        })
//                        .collect(Collectors.toList());
//
//                indexesIterator = includeLabels.stream()
//                        .filter(label -> !excludeLabels.contains(label) && tokenRead.nodeLabel(label) != -1)
//                        .flatMap(label -> {
//                            Iterable<IndexDescriptor> indexesForLabel =
//                                    () -> schemaRead.indexesGetForLabel(tokenRead.nodeLabel(label));
//                            return StreamSupport.stream(indexesForLabel.spliterator(), false);
//                        })
//                        .collect(Collectors.toList());
//            }
//
////            Stream<IndexConstraintNodeInfo> constraintNodeInfoStream = StreamSupport.stream(
////                            constraintsIterator.spliterator(), false)
////                    .map(constraintDescriptor ->
////                            nodeInfoFromConstraintDefinition(constraintDescriptor, tokenRead, useStoredName))
////                    .sorted(Comparator.comparing(i -> i.label.toString()));
////
////            Stream<IndexConstraintNodeInfo> indexNodeInfoStream = StreamSupport.stream(
////                            indexesIterator.spliterator(), false)
////                    .map(indexDescriptor ->
////                            this.nodeInfoFromIndexDefinition(indexDescriptor, schemaRead, tokenRead, useStoredName))
////                    .sorted(Comparator.comparing(i -> i.label.toString()));
//
//            return Stream.of(indexesIterator, constraintsIterator).flatMap(e -> e);
//        }
//    }
//
//    private List<IndexDescriptor> getIndexesFromSchema(
//            Iterator<IndexDescriptor> allIndex, Predicate<IndexDescriptor> indexDescriptorPredicate) {
//        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(allIndex, Spliterator.ORDERED), false)
//                .filter(indexDescriptorPredicate)
//                .collect(Collectors.toList());
//    }
//}
