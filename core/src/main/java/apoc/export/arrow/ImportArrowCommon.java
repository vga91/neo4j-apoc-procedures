package apoc.export.arrow;

import apoc.export.util.ProgressReporter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Iterables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.export.arrow.ArrowConstants.END_FIELD;
import static apoc.export.arrow.ArrowConstants.ID_FIELD;
import static apoc.export.arrow.ArrowConstants.LABELS_FIELD;
import static apoc.export.arrow.ArrowConstants.START_FIELD;
import static apoc.export.arrow.ArrowConstants.TYPE_FIELD;
import static apoc.export.json.JsonImporter.flatMap;
import static apoc.util.JsonUtil.OBJECT_MAPPER;
import static java.util.Arrays.asList;

public class ImportArrowCommon {

    public static Map<String, ValueVector> getDecodedVectorMap(ArrowReader streamReader, VectorSchemaRoot schemaRoot) throws IOException {
        Map<Long, Dictionary> dictionaryMap = streamReader.getDictionaryVectors();
        return schemaRoot.getFieldVectors().stream().collect(Collectors.toMap(ValueVector::getName, vector -> {
            long idDictionary = vector.getField().getDictionary().getId();
            return DictionaryEncoder.decode(vector, dictionaryMap.get(idDictionary));
        }));
    }


    public static void createRelFromArrow(Map<String, ValueVector> decodedVectorsMap, Node from, Node to, VarCharVector type, int index, String normalizeKey, ProgressReporter reporter) {
        final String typeName = readLabelAsString(new String(type.get(index)));
        RelationshipType relationshipType = RelationshipType.withName(typeName);
        Relationship relationship = from.createRelationshipTo(to, relationshipType);

        decodedVectorsMap.entrySet().stream()
                .filter(i -> filterPropertyEntries(normalizeKey, i, List.of(START_FIELD, END_FIELD, TYPE_FIELD)))
                .forEach(propVector -> setCurrentVector(index, relationship, propVector, normalizeKey, reporter));

        reporter.update(0, 1, 0);

    }

    public static void createNodeFromArrow(Node node, Map<String, ValueVector> decodedVectorsMap, int index, String normalizeKey, ProgressReporter reporter) {
        VarCharVector labelVector = (VarCharVector) decodedVectorsMap.get(LABELS_FIELD);

        asList(new String(labelVector.get(index)).split(":")).forEach(label -> {
            node.addLabel(Label.label(readLabelAsString(label)));
        });

        // properties
        decodedVectorsMap.entrySet().stream()
                .filter(i -> filterPropertyEntries(normalizeKey, i, List.of(LABELS_FIELD, ID_FIELD)))
                .forEach(propVector -> setCurrentVector(index, node, propVector, normalizeKey, reporter));

        reporter.update(1, 0, 0);
    }

    public static String readLabelAsString(String label) {
        try {
            return OBJECT_MAPPER.readValue(label, String.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error during reading Label");
        }
    }

    public static Object getCurrentIndex(byte[] value) {
        try {
            return OBJECT_MAPPER.readValue(value, Object.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private static void setCurrentVector(int index, Entity entity, Map.Entry<String, ValueVector> propVector, String normalizeKey, ProgressReporter reporter) {
        VarCharVector vector = (VarCharVector) propVector.getValue();
        byte[] value = vector.get(index);
        if (value != null) {
            Object valueRead = getCurrentIndex(value);
            setPropertyByValue(valueRead, propVector.getKey().replace(normalizeKey, ""), entity);
            reporter.update(0,0,1);
        }
    }

    public static void setPropertyByValue(Object value, String name, Entity entity) {

        if (value instanceof Map) {
            Stream<Map.Entry<String, Object>> entryStream = flatMap((Map<String, Object>) value, name);
            entryStream.filter(e -> e.getValue() != null)
                    .forEach(entry -> setPropertyByValue(entry.getValue(), entry.getKey(), entity));
        } else if (value instanceof Collection) {
            final Collection valueReadAsList = (Collection) value;
            final Object iterator = valueReadAsList.iterator().next();
            if (iterator instanceof Map || iterator instanceof Collection) {
                setPropertyByValue(iterator, name, entity);
            } else {
                entity.setProperty(name, Iterables.toArray(valueReadAsList, iterator.getClass()));
            }
        } else {
            entity.setProperty(name, value);
        }
    }

    public static void closeVectors(VectorSchemaRoot schemaRoot, Map<String, ValueVector> decodedVectorsMap) {
        schemaRoot.getFieldVectors().forEach(FieldVector::close);
        decodedVectorsMap.values().forEach(ValueVector::close);
    }

    private static boolean filterPropertyEntries(String normalizeKey, Map.Entry<String, ValueVector> i, List<String> startField) {
        return StringUtils.isBlank(normalizeKey) ? !startField.contains(i.getKey()) : i.getKey().startsWith(normalizeKey);
    }
}
