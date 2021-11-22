package apoc.systemdb.metadata;

import apoc.SystemPropertyKeys;
import apoc.util.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.neo4j.graphdb.Node;

import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.systemdb.SystemDbConfig.DV_CATALOGS;

public class ExportDataVirtualization implements ExportMetadata {

    @Override
    public Stream export(Node node) {
        final String dvName = (String) node.getProperty(SystemPropertyKeys.name.name());
        try {
            final String data = toNeo4jStringMap(JsonUtil.OBJECT_MAPPER.readValue((String) node.getProperty(SystemPropertyKeys.data.name()), Map.class));
            final String statement = String.format("CALL apoc.dv.catalog.add('%s', %s)", dvName, data);
            return Stream.of(new AbstractMap.SimpleEntry<>(getFileName(node, DV_CATALOGS), statement));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
