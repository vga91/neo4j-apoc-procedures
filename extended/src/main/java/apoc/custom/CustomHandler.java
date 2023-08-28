package apoc.custom;

import apoc.ExtendedSystemPropertyKeys;
import apoc.SystemPropertyKeys;
import apoc.util.SystemDbUtil;
import apoc.util.Util;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.ExtendedSystemLabels.ApocCypherProcedures;
import static apoc.ExtendedSystemLabels.ApocCypherProceduresMeta;
import static apoc.ExtendedSystemLabels.Function;
import static apoc.ExtendedSystemLabels.Procedure;
import static apoc.SystemPropertyKeys.*;
import static apoc.ExtendedSystemPropertyKeys.*;
import static apoc.custom.CypherProceduresHandler.*;
import static apoc.util.SystemDbUtil.getSystemNodes;
import static apoc.util.SystemDbUtil.withSystemDb;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

public class CustomHandler {

    public static void installProcedure(String databaseName, ProcedureSignature signature, String statement) {

        withSystemDb(tx -> {
            Node node = Util.mergeNode(tx, ApocCypherProcedures, Procedure,
                    Pair.of(database.name(), databaseName),
                    Pair.of(name.name(), signature.name().name()),
                    Pair.of(prefix.name(), signature.name().namespace())
            );
            node.setProperty(description.name(), signature.description().orElse(null));
            node.setProperty(SystemPropertyKeys.statement.name(), statement);
            node.setProperty(inputs.name(), serializeSignatures(signature.inputSignature()));
            node.setProperty(outputs.name(), serializeSignatures(signature.outputSignature()));
            node.setProperty(mode.name(), signature.mode().name());

            setLastUpdate(tx, databaseName);
        });
    }

    public static void installFunction(String databaseName, UserFunctionSignature signature, String statement, boolean forceSingle) {
        withSystemDb(tx -> {
            Node node = Util.mergeNode(tx, ApocCypherProcedures, Function,
                    Pair.of(database.name(), databaseName),
                    Pair.of(name.name(), signature.name().name()),
                    Pair.of(prefix.name(), signature.name().namespace())
            );
            node.setProperty(description.name(), signature.description().orElse(null));
            node.setProperty(SystemPropertyKeys.statement.name(), statement);
            node.setProperty(inputs.name(), serializeSignatures(signature.inputSignature()));
            node.setProperty(output.name(), signature.outputType().toString());
            node.setProperty(ExtendedSystemPropertyKeys.forceSingle.name(), forceSingle);

            setLastUpdate(tx, databaseName);
        });
    }

    public static Mode mode(String s) {
        return s == null ? Mode.READ : Mode.valueOf(s.toUpperCase());
    }

    // todo - join show and dropAll???
    public static List<CustomProcedureInfo> dropAll(String databaseName) {
        return withSystemDb(tx -> {
            List<CustomProcedureInfo> previous = getCustomNodes(databaseName, tx)
                    .stream()
                    .map(node -> {
                        // we'll return previous uuid info
                        CustomProcedureInfo info = CustomProcedureInfo.fromNode(node);
                        node.delete();
                        return info;
                    })
                    .sorted(Comparator.comparing((CustomProcedureInfo i) -> i.name)
                            .thenComparing(i -> i.type)
                    )
                    .collect(Collectors.toList());

            setLastUpdate(tx, databaseName);

            return previous;
        });
    }

    public static Stream<CustomProcedureInfo> show(String databaseName, Transaction tx) {
        return getCustomNodes(databaseName, tx).stream()
                .map(CustomProcedureInfo::fromNode)
                .sorted(Comparator.comparing((CustomProcedureInfo i) -> i.name)
                        .thenComparing(i -> i.type)
                );
    }

    public static ResourceIterator<Node> getCustomNodes(String databaseName, Transaction tx) {
        return getCustomNodes(databaseName, tx, null);
    }

    public static ResourceIterator<Node> getCustomNodes(String databaseName, Transaction tx, Map<String, Object> props) {
        return getSystemNodes(tx, databaseName, ApocCypherProcedures, props);
    }

    public static CustomProcedureInfo getFunctionInfo(Node node) {
        String statement = (String) node.getProperty(SystemPropertyKeys.statement.name());
        boolean forceSingle = (boolean) node.getProperty(ExtendedSystemPropertyKeys.forceSingle.name(), false);
        UserFunctionSignature signature = getUserFunctionSignature(node);

        return CustomProcedureInfo.getCustomFunctionInfo(signature, forceSingle, statement);
    }

    public static CustomProcedureInfo getProcedureInfo(Node node) {
        String statement = (String) node.getProperty(SystemPropertyKeys.statement.name());
        ProcedureSignature signature = getProcedureSignature(node);

        return CustomProcedureInfo.getCustomProcedureInfo(signature, statement);
    }

    public static void dropFunction(String databaseName, String name) {
        withSystemDb(tx -> {
            QualifiedName qName = qualifiedName(name);
            getCustomNodes(databaseName, tx,
                    Map.of(SystemPropertyKeys.name.name(), qName.name(),
                            prefix.name(), qName.namespace())
            )
                    .stream()
                    .filter(n -> n.hasLabel(Function)).forEach(node -> {
                        node.delete();
                        setLastUpdate(tx, databaseName);
                    });
        });
    }

    public static void dropProcedure(String databaseName, String name) {
        withSystemDb(tx -> {
            QualifiedName qName = qualifiedName(name);
            tx.findNodes(ApocCypherProcedures,
                    SystemPropertyKeys.database.name(), databaseName,
                    SystemPropertyKeys.name.name(), qName.name(),
                    prefix.name(), qName.namespace()
            ).stream().filter(n -> n.hasLabel(Procedure)).forEach(node -> {
                node.delete();
                setLastUpdate(tx, databaseName);
            });
        });
    }

    public static QualifiedName qualifiedName(@Name("name") String name) {
        String[] names = name.split("\\.");
        List<String> namespace = new ArrayList<>(names.length);
        namespace.add(PREFIX);
        namespace.addAll(Arrays.asList(names));
        return new QualifiedName(namespace.subList(0, namespace.size() - 1), names[names.length - 1]);
    }

    public static String serializeSignatures(List<FieldSignature> signatures) {
        List<Map<String, Object>> mapped = signatures.stream().map(fs -> {
            final Map<String, Object> map = map(
                    "name", fs.name(),
                    "type", fs.neo4jType().toString()
            );
            fs.defaultValue().map(defVal -> map.put("default", defVal.value()));
            return map;
        }).collect(Collectors.toList());
        return Util.toJson(mapped);
    }

    private static void setLastUpdate(Transaction tx, String databaseName) {
        SystemDbUtil.setLastUpdate(tx, databaseName, ApocCypherProceduresMeta);
    }

    public static UserFunctionSignature getUserFunctionSignature(Node node) {
        String name = (String) node.getProperty(SystemPropertyKeys.name.name());
        String description = (String) node.getProperty(ExtendedSystemPropertyKeys.description.name(), null);
        String[] prefix = (String[]) node.getProperty(ExtendedSystemPropertyKeys.prefix.name(), new String[]{PREFIX});

        String property = (String) node.getProperty(ExtendedSystemPropertyKeys.inputs.name());
        List<FieldSignature> inputs = deserializeSignatures(property);

        UserFunctionSignature signature = new UserFunctionSignature(
                new QualifiedName(prefix, name),
                inputs,
                typeof((String) node.getProperty(ExtendedSystemPropertyKeys.output.name())),
                null,
                description,
                "apoc.custom",
                false,
                false,
                false,
                false
        );
        return signature;
    }

    public static ProcedureSignature getProcedureSignature(Node node) {
        String name = (String) node.getProperty(SystemPropertyKeys.name.name());
        String description = (String) node.getProperty( ExtendedSystemPropertyKeys.description.name(), null);
        String[] prefix = (String[]) node.getProperty(ExtendedSystemPropertyKeys.prefix.name(), new String[]{PREFIX});

        String property = (String) node.getProperty(ExtendedSystemPropertyKeys.inputs.name());
        List<FieldSignature> inputs = deserializeSignatures(property);

        List<FieldSignature> outputSignature = deserializeSignatures((String) node.getProperty(ExtendedSystemPropertyKeys.outputs.name()));
        ProcedureSignature procedureSignature = Signatures.createProcedureSignature(
                new QualifiedName(prefix, name),
                inputs,
                outputSignature,
                Mode.valueOf((String) node.getProperty(ExtendedSystemPropertyKeys.mode.name())),
                false,
                null,
                description,
                null,
                false,
                false,
                false,
                false,
                false,
                false);
        return procedureSignature;
    }

}