package apoc.systemdb.metadata;

import apoc.SystemPropertyKeys;
import apoc.util.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.helpers.collection.Pair;

import java.util.Map;
import java.util.stream.Stream;

import static apoc.util.Util.toCypherMap;

public class ExportDataVirtualization implements ExportMetadata {

    @Override
    public Stream<Pair<String, String>> export(Node node) {
        final String dvName = (String) node.getProperty(SystemPropertyKeys.name.name());
        try {
            final String data = toCypherMap(JsonUtil.OBJECT_MAPPER.readValue((String) node.getProperty(SystemPropertyKeys.data.name()), Map.class));
            final String statement = String.format("CALL apoc.dv.catalog.add('%s', %s)", dvName, data);
            return Stream.of(Pair.of(getFileName(node, Type.DataVirtualizationCatalog.name()), statement));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
