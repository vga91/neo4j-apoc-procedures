package apoc.schema;

import apoc.Extended;
import apoc.result.*;
import apoc.util.Util;
import apoc.util.collection.Iterables;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.procedure.*;
import org.neo4j.token.api.TokenConstants;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.schema.SchemasExtendedUtil.IDX_NOT_FOUND;
import static org.apache.arrow.vector.dictionary.DictionaryEncoder.getIndexType;
import static org.neo4j.graphdb.schema.ConstraintType.UNIQUENESS;
import static org.neo4j.internal.schema.SchemaUserDescription.TOKEN_LABEL;
import static org.neo4j.internal.schema.SchemaUserDescription.TOKEN_REL_TYPE;

@Extended
public class SchemasExtended {

    @Context
    public Transaction tx;

    @Context
    public KernelTransaction ktx;

    @Procedure(value = "apoc.schema.node.compareIndexesAndConstraints", mode = Mode.SCHEMA)
    @Description("CALL apoc.schema.node.compareIndexesAndConstraints($config) - to compare node constraints and indexes")
    public Stream<CompareIdxToConsNodes> compareIndexesAndConstraints(@Name(value = "config",defaultValue = "{}") Map<String,Object> config) {
        return indexesAndConstraintsForNode(config, tx, ktx,
                compareConstraintIdxFunction(CompareIdxToConsNodes.class));
    }

    @Procedure(value = "apoc.schema.relationship.compareIndexesAndConstraints", mode = Mode.SCHEMA)
    @Description("CALL apoc.schema.relationship.compareIndexesAndConstraints($config) - to compare rel constraints and indexes")
    public Stream<CompareIdxToConsRels> compareIndexesAndConstraintsForRelationships(@Name(value = "config",defaultValue = "{}") Map<String,Object> config) {
        return indexesAndConstraintsForRelationships(config, tx, ktx, compareConstraintIdxFunction(CompareIdxToConsRels.class));
    }
    
    // TODO - put in another class??

    private Object getInfoLabelOrType(Object idxOrCons) {
        if (idxOrCons instanceof IndexConstraintNodeInfo) {
            return ((IndexConstraintNodeInfo) idxOrCons).label;
        }
        return ((IndexConstraintRelationshipInfo) idxOrCons).relationshipType;
    }

    // TODO - needed?
    private <T extends CompareIdxToCons> T addObjectIfAbsent(Map<String, T> map, String label, Class<T> clazz) {
        return map.compute(label,
                (k, v) -> Objects.requireNonNullElseGet(v, () -> {
                    try {
                        return clazz.getConstructor(String.class).newInstance(label);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
    }

    private <R extends CompareIdxToCons> BiFunction<Stream<Object>, Stream<Object>, Stream<R>> compareConstraintIdxFunction(Class<R> clazz) {
        return (constraintNodeInfoStream, indexNodeInfoStream) -> {
            final List<Object> constraints = constraintNodeInfoStream.toList();
            final List<Object> indexes = indexNodeInfoStream.toList();

            Map<String, R> resultMap = new TreeMap<>();

            indexes.forEach(i -> {
                final Object labelOrType = getInfoLabelOrType(i);
                if (labelOrType instanceof String) {
                    addCommonAndOnlyIdxProps(constraints, addObjectIfAbsent(resultMap, (String) labelOrType, clazz), i);
                }
                if (labelOrType instanceof List) {
                    final List<String> labels = (List<String>) labelOrType;
                    labels.forEach(lbl -> {
                        addCommonAndOnlyIdxProps(constraints, addObjectIfAbsent(resultMap, lbl, clazz), i);
                    });
                }
            });

            constraints.forEach(i -> {
                final Object labelOrType = getInfoLabelOrType(i);
                addObjectIfAbsent(resultMap, (String) labelOrType, clazz).putOnlyConstraintsProps(i.name, i.properties);
            });

            return resultMap.values().stream();
        };
    }

    private <T extends Object> void addCommonAndOnlyIdxProps(List<T> constraints, CompareIdxToCons compareIdxToCons, Object index) {
        final List<String> props = index.properties;
//        if (index instanceof IndexConstraintNodeInfo) {
//
//        } else if (index instanceof IndexConstraintRelationshipInfo) {
//            
//        } else {
//            throw new RuntimeException("TODO");
//        }

        // UNIQUENESS constraints also produce an analogous index, so the properties is necessary in common
        if (UNIQUENESS.name().equals(index.type)) {
            compareIdxToCons.addCommonProps(indexProps);
        } else {
            constraints.stream()
                    .filter(cons -> {
                        final Object idxLabelOrType = getInfoLabelOrType(index);
                        final Object constraintLabelOrType = getInfoLabelOrType(cons);

                        List<String> indexProps = index.properties;
                        List<String> consProps = cons.properties;
                        return idxLabelOrType.equals(constraintLabelOrType)
                                && indexProps != null
                                && consProps != null;
                        
                        // TODO todo
                                //&&  CollectionUtils.isEqualCollection(indexProps, consProps);
                    })
                    .findFirst()
                    .ifPresentOrElse(pres -> {
                                compareIdxToCons.addCommonProps(props);
                                constraints.remove(pres);
                            },
                            () -> compareIdxToCons.putOnlyIdxProps(index.name, props)
                    );
        }
    }

    public static <T> T indexesAndConstraintsForNode(Map<String,Object> config, Transaction tx, KernelTransaction ktx, BiFunction<Stream<IndexConstraintNodeInfo>, Stream<IndexConstraintNodeInfo>, T> function) {
        Schema schema = tx.schema();

        SchemaConfig schemaConfig = new SchemaConfig(config);
        Set<String> includeLabels = schemaConfig.getLabels();
        Set<String> excludeLabels = schemaConfig.getExcludeLabels();

        try (Statement ignore = ktx.acquireStatement()) {
            TokenRead tokenRead = ktx.tokenRead();

            SchemaRead schemaRead = ktx.schemaRead();
            Iterable<IndexDescriptor> indexesIterator;
            Iterable<ConstraintDefinition> constraintsIterator;
            final Predicate<ConstraintDefinition> isNodeConstraint =
                    constraintDefinition -> Util.isNodeCategory(constraintDefinition.getConstraintType());

            if (includeLabels.isEmpty()) {

                Iterator<IndexDescriptor> allIndex = schemaRead.indexesGetAll();

                indexesIterator = getIndexesFromSchema(
                        allIndex,
                        index -> index.schema().entityType().equals(EntityType.NODE)
                                && Arrays.stream(index.schema().getEntityTokenIds())
                                .noneMatch(id -> {
                                    try {
                                        return excludeLabels.contains(tokenRead.nodeLabelName(id));
                                    } catch (LabelNotFoundKernelException e) {
                                        return false;
                                    }
                                }));

                Iterable<ConstraintDefinition> allConstraints = schema.getConstraints();
                constraintsIterator = StreamSupport.stream(allConstraints.spliterator(), false)
                        .filter(isNodeConstraint)
                        .filter(constraint ->
                                !excludeLabels.contains(constraint.getLabel().name()))
                        .collect(Collectors.toList());
            } else {
                constraintsIterator = includeLabels.stream()
                        .filter(label -> !excludeLabels.contains(label) && tokenRead.nodeLabel(label) != -1)
                        .flatMap(label -> {
                            Iterable<ConstraintDefinition> constraintsForType =
                                    schema.getConstraints(Label.label(label));
                            return StreamSupport.stream(constraintsForType.spliterator(), false)
                                    .filter(isNodeConstraint);
                        })
                        .collect(Collectors.toList());

                indexesIterator = includeLabels.stream()
                        .filter(label -> !excludeLabels.contains(label) && tokenRead.nodeLabel(label) != -1)
                        .flatMap(label -> {
                            Iterable<IndexDescriptor> indexesForLabel =
                                    () -> schemaRead.indexesGetForLabel(tokenRead.nodeLabel(label));
                            return StreamSupport.stream(indexesForLabel.spliterator(), false);
                        })
                        .collect(Collectors.toList());
            }

            Stream<IndexConstraintNodeInfo> constraintNodeInfoStream = StreamSupport.stream(
                            constraintsIterator.spliterator(), false)
                    .map(constraintDescriptor ->
                            nodeInfoFromConstraintDefinition(constraintDescriptor, tokenRead, false, ktx))
                    .sorted(Comparator.comparing(i -> i.label.toString()));

            Stream<IndexConstraintNodeInfo> indexNodeInfoStream = StreamSupport.stream(
                            indexesIterator.spliterator(), false)
                    .map(indexDescriptor ->
                            nodeInfoFromIndexDefinition(indexDescriptor, schemaRead, tokenRead, false))
                    .sorted(Comparator.comparing(i -> i.label.toString()));

            return function.apply(constraintNodeInfoStream, indexNodeInfoStream);
        }
    }

    public static  <T> T indexesAndConstraintsForRelationships(Map<String,Object> config, Transaction tx, KernelTransaction ktx, BiFunction<Stream<IndexConstraintRelationshipInfo>, Stream<IndexConstraintRelationshipInfo>, T> function) {
        Schema schema = tx.schema();

        SchemaConfig schemaConfig = new SchemaConfig(config);
        Set<String> includeRelationships = schemaConfig.getRelationships();
        Set<String> excludeRelationships = schemaConfig.getExcludeRelationships();

        try (Statement ignore = ktx.acquireStatement()) {
            TokenRead tokenRead = ktx.tokenRead();
            SchemaRead schemaRead = ktx.schemaRead();
            Iterable<ConstraintDefinition> constraintsIterator;
            Iterable<IndexDescriptor> indexesIterator;

            final Predicate<ConstraintDefinition> isRelConstraint =
                    constraintDefinition -> Util.isRelationshipCategory(constraintDefinition.getConstraintType());

            if (!includeRelationships.isEmpty()) {
                constraintsIterator = includeRelationships.stream()
                        .filter(type -> !excludeRelationships.contains(type)
                                && tokenRead.relationshipType(type) != TokenConstants.NO_TOKEN)
                        .flatMap(type -> {
                            Iterable<ConstraintDefinition> constraintsForType =
                                    schema.getConstraints(RelationshipType.withName(type));
                            return StreamSupport.stream(constraintsForType.spliterator(), false)
                                    .filter(isRelConstraint);
                        })
                        .collect(Collectors.toList());

                indexesIterator = includeRelationships.stream()
                        .filter(type -> !excludeRelationships.contains(type)
                                && tokenRead.relationshipType(type) != TokenConstants.NO_TOKEN)
                        .flatMap(type -> {
                            Iterable<IndexDescriptor> indexesForRelType =
                                    () -> schemaRead.indexesGetForRelationshipType(tokenRead.relationshipType(type));
                            return StreamSupport.stream(indexesForRelType.spliterator(), false);
                        })
                        .collect(Collectors.toList());
            } else {
                Iterable<ConstraintDefinition> allConstraints = schema.getConstraints();
                constraintsIterator = StreamSupport.stream(allConstraints.spliterator(), false)
                        .filter(isRelConstraint)
                        .filter(constraint -> !excludeRelationships.contains(
                                constraint.getRelationshipType().name()))
                        .collect(Collectors.toList());

                Iterator<IndexDescriptor> allIndex = schemaRead.indexesGetAll();
                indexesIterator = getIndexesFromSchema(
                        allIndex,
                        index -> index.schema().entityType().equals(EntityType.RELATIONSHIP)
                                && Arrays.stream(index.schema().getEntityTokenIds())
                                .noneMatch(id ->
                                        excludeRelationships.contains(tokenRead.relationshipTypeGetName(id))));
            }

            Stream<IndexConstraintRelationshipInfo> constraintRelationshipInfoStream = StreamSupport.stream(
                            constraintsIterator.spliterator(), false)
                    .map(c -> relationshipInfoFromConstraintDefinition(c, useStoredName));

            Stream<IndexConstraintRelationshipInfo> indexRelationshipInfoStream = StreamSupport.stream(
                            indexesIterator.spliterator(), false)
                    .map(index -> relationshipInfoFromIndexDescription(index, tokenRead, schemaRead, useStoredName));

            return function.apply(constraintRelationshipInfoStream, indexRelationshipInfoStream);
        }
    }

    private static IndexConstraintRelationshipInfo relationshipInfoFromIndexDescription(
            IndexDescriptor indexDescriptor, TokenNameLookup tokens, SchemaRead schemaRead, Boolean useStoredName) {
        int[] relIds = indexDescriptor.schema().getEntityTokenIds();
        int length = relIds.length;
        // to handle LOOKUP indexes
        final Object relName;
        if (length == 0) {
            relName = TOKEN_REL_TYPE;
        } else {
            final List<String> rels = IntStream.of(relIds)
                    .mapToObj(tokens::relationshipTypeGetName)
                    .sorted()
                    .collect(Collectors.toList());
            relName = rels.size() > 1 ? rels : rels.get(0);
        }
        final List<String> properties = Arrays.stream(indexDescriptor.schema().getPropertyIds())
                .mapToObj(tokens::propertyKeyGetName)
                .collect(Collectors.toList());

        // Pretty print for index name
        final String name = useStoredName ? indexDescriptor.getName() : getSchemaInfoName(relName, properties);
        final String schemaType = getIndexType(indexDescriptor);

        String indexStatus;
        try {
            indexStatus = schemaRead.indexGetState(indexDescriptor).toString();
        } catch (IndexNotFoundKernelException e) {
            indexStatus = IDX_NOT_FOUND;
        }

        return new IndexConstraintRelationshipInfo(name, schemaType, properties, indexStatus, relName);
    }

    private static String getIndexType(IndexDescriptor indexDescriptor) {
        return indexDescriptor.getIndexType().name();
    }
    
    private static IndexConstraintRelationshipInfo relationshipInfoFromConstraintDefinition(
            ConstraintDefinition constraintDefinition, Boolean useStoredName) {
        return new IndexConstraintRelationshipInfo(
                useStoredName
                        ? constraintDefinition.getName()
                        : String.format("CONSTRAINT %s", constraintDefinition.toString()),
                constraintDefinition.getConstraintType().name(),
                Iterables.asList(constraintDefinition.getPropertyKeys()),
                "",
                constraintDefinition.getRelationshipType().name());
    }

    private static IndexConstraintNodeInfo nodeInfoFromIndexDefinition(
            IndexDescriptor indexDescriptor, SchemaRead schemaRead, TokenNameLookup tokens, Boolean useStoredName) {
        int[] labelIds = indexDescriptor.schema().getEntityTokenIds();
        int length = labelIds.length;
        final Object labelName;
        if (length == 0) {
            labelName = TOKEN_LABEL;
        } else {
            final List<String> labels = IntStream.of(labelIds)
                    .mapToObj(tokens::labelGetName)
                    .sorted()
                    .collect(Collectors.toList());
            labelName = labels.size() > 1 ? labels : labels.get(0);
        }
        // to handle LOOKUP indexes
        List<String> properties = IntStream.of(indexDescriptor.schema().getPropertyIds())
                .mapToObj(tokens::propertyKeyGetName)
                .collect(Collectors.toList());

        // Pretty print for index name
        final String schemaInfoName = getSchemaInfoName(labelName, properties);
        final String userDescription = indexDescriptor.userDescription(tokens);
        try {
            return new IndexConstraintNodeInfo(
                    useStoredName ? indexDescriptor.getName() : schemaInfoName,
                    labelName,
                    properties,
                    schemaRead.indexGetState(indexDescriptor).toString(),
                    getIndexType(indexDescriptor),
                    schemaRead.indexGetState(indexDescriptor).equals(InternalIndexState.FAILED)
                            ? schemaRead.indexGetFailure(indexDescriptor)
                            : "NO FAILURE",
                    getPopulationProgress(indexDescriptor, schemaRead),
                    schemaRead.indexSize(indexDescriptor),
                    schemaRead.indexUniqueValuesSelectivity(indexDescriptor),
                    userDescription);
        } catch (IndexNotFoundKernelException e) {
            return new IndexConstraintNodeInfo(
                    schemaInfoName,
                    labelName,
                    properties,
                    IDX_NOT_FOUND,
                    getIndexType(indexDescriptor),
                    IDX_NOT_FOUND,
                    0,
                    0,
                    0,
                    userDescription);
        }
    }

    private static long getPopulationProgress(IndexDescriptor indexDescriptor, SchemaRead schemaRead)
            throws IndexNotFoundKernelException {
        PopulationProgress populationProgress = schemaRead.indexGetPopulationProgress(indexDescriptor);
        // when the index is failed the getTotal() is equal to 0
        long populationTotal = populationProgress.getTotal();
        if (populationTotal == 0) {
            return 0L;
        }
        return populationProgress.getCompleted() / populationTotal * 100;
    }


    private static String getSchemaInfoName(Object labelOrType, List<String> properties) {
        final String labelOrTypeAsString =
                labelOrType instanceof String ? (String) labelOrType : StringUtils.join(labelOrType, ",");
        return String.format(":%s(%s)", labelOrTypeAsString, StringUtils.join(properties, ","));
    }
    /**
     * ConstraintInfo info from ConstraintDefinition
     *
     * @param constraintDefinition
     * @param tokens
     * @return
     */
    private static IndexConstraintNodeInfo nodeInfoFromConstraintDefinition(
            ConstraintDefinition constraintDefinition, TokenNameLookup tokens, Boolean useStoredName, KernelTransaction ktx) {
        String labelName = constraintDefinition.getLabel().name();
        List<String> properties = Iterables.asList(constraintDefinition.getPropertyKeys());
        return new IndexConstraintNodeInfo(
                // Pretty print for index name
                useStoredName
                        ? constraintDefinition.getName()
                        : String.format(":%s(%s)", labelName, StringUtils.join(properties, ",")),
                labelName,
                properties,
                StringUtils.EMPTY,
                constraintDefinition.getConstraintType().name(),
                "NO FAILURE",
                0,
                0,
                0,
                nodeConstraintCypher5Compatibility(
                        ktx.schemaRead()
                                .constraintGetForName(constraintDefinition.getName())
                                .userDescription(tokens),
                        useStoredName));
    }
    
    private static String nodeConstraintCypher5Compatibility(String userDescription, Boolean useStoredName) {
        if (useStoredName) {
            return userDescription;
        } else {
            // Revert to old description on Cypher 5 for backwards compatibility.
            return userDescription.replace("'NODE PROPERTY UNIQUENESS'", "'UNIQUENESS'");
        }
    }
    public static List<IndexDescriptor> getIndexesFromSchema(
            Iterator<IndexDescriptor> allIndex, Predicate<IndexDescriptor> indexDescriptorPredicate) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(allIndex, Spliterator.ORDERED), false)
                .filter(indexDescriptorPredicate)
                .collect(Collectors.toList());
    }
    
    public static class SchemaConfig {
        private static final String LABELS_KEY = "labels";
        private static final String EXCLUDE_LABELS_KEY = "excludeLabels";
        private static final String RELATIONSHIPS_KEY = "relationships";
        private static final String EXCLUDE_RELATIONSHIPS_KEY = "excludeRelationships";

        private final Set<String> labels;
        private final Set<String> excludeLabels;
        private final Set<String> relationships;
        private final Set<String> excludeRelationships;

        public Set<String> getLabels() {
            return labels;
        }

        public Set<String> getExcludeLabels() {
            return excludeLabels;
        }

        public Set<String> getRelationships() {
            return relationships;
        }

        public Set<String> getExcludeRelationships() {
            return excludeRelationships;
        }

        public SchemaConfig(Map<String, Object> config) {
            config = config != null ? config : Collections.emptyMap();
            this.labels = new HashSet<>((Collection<String>) config.getOrDefault(LABELS_KEY, Collections.EMPTY_SET));
            this.excludeLabels =
                    new HashSet<>((Collection<String>) config.getOrDefault(EXCLUDE_LABELS_KEY, Collections.EMPTY_SET));
            validateParameters(this.labels, this.excludeLabels, LABELS_KEY, EXCLUDE_LABELS_KEY);
            this.relationships =
                    new HashSet<>((Collection<String>) config.getOrDefault(RELATIONSHIPS_KEY, Collections.EMPTY_SET));
            this.excludeRelationships = new HashSet<>(
                    (Collection<String>) config.getOrDefault(EXCLUDE_RELATIONSHIPS_KEY, Collections.EMPTY_SET));
            validateParameters(this.relationships, this.excludeRelationships, RELATIONSHIPS_KEY, EXCLUDE_RELATIONSHIPS_KEY);
        }

        private void validateParameters(
                Set<String> include, Set<String> exclude, String includeParameterType, String excludeParameterType) {
            if (!include.isEmpty() && !exclude.isEmpty())
                throw new IllegalArgumentException(String.format(
                        "Parameters %s and %s are both valuated. Please check parameters and valuate only one.",
                        includeParameterType, excludeParameterType));
        }
    }



    // TODO - collection utils
    public static <T> boolean isEqualCollection(Collection<T> col1, Collection<T> col2) {
        if (col1 == null || col2 == null) {
            return col1 == col2; // Both must be null to be equal
        }
        if (col1.size() != col2.size()) {
            return false;
        }

        Map<T, Integer> countMap1 = getElementCounts(col1);
        Map<T, Integer> countMap2 = getElementCounts(col2);

        return countMap1.equals(countMap2);
    }

    private static <T> Map<T, Integer> getElementCounts(Collection<T> collection) {
        Map<T, Integer> countMap = new HashMap<>();
        for (T item : collection) {
            countMap.put(item, countMap.getOrDefault(item, 0) + 1);
        }
        return countMap;
    }

}
