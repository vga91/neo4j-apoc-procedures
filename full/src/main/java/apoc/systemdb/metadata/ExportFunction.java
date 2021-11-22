package apoc.systemdb.metadata;

import apoc.SystemPropertyKeys;
import apoc.custom.CypherProceduresHandler;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.procs.FieldSignature;

import java.util.stream.Collectors;
import java.util.stream.Stream;


public class ExportFunction implements ExportMetadata {

    @Override
    public Stream<Pair<String, String>> export(Node node) {
        final String inputs = getSignature(node, SystemPropertyKeys.inputs.name());

        final String outputName = SystemPropertyKeys.output.name();
        final String outputs = node.hasProperty(outputName)
                ? (String) node.getProperty(outputName)
                : getSignature(node, SystemPropertyKeys.outputs.name());

        String statement = String.format("CALL apoc.custom.declareFunction('%s(%s) :: (%s)', '%s' , %s, '%s');",
                node.getProperty(SystemPropertyKeys.name.name()), inputs, outputs,
                node.getProperty(SystemPropertyKeys.statement.name()),
                node.getProperty(SystemPropertyKeys.forceSingle.name()),
                node.getProperty(SystemPropertyKeys.description.name()));

        return Stream.of(Pair.of(getFileName(node, Type.CypherFunction.name()), statement));
    }

    
    static String getSignature(Node node, String name) {
        return CypherProceduresHandler.deserializeSignatures((String) node.getProperty(name))
                .stream().map(FieldSignature::toString)
                .collect(Collectors.joining(", "));
    }
}
