package apoc.custom;

import apoc.ApocConfig;
import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.util.SystemDbUtil;
import apoc.util.Util;
import apoc.uuid.UuidInfo;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.ApocConfig.APOC_UUID_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.SystemLabels.*;
import static apoc.SystemPropertyKeys.*;
import static apoc.SystemLabels.ApocUuidMeta;
//import static apoc.SystemPropertyKeys.database;
import static apoc.custom.CustomProcedureInfo.convertInputSignature;
import static apoc.custom.CustomProcedureInfo.prettyPrintType;
import static apoc.custom.CypherProceduresHandler.*;
import static apoc.util.SystemDbUtil.getSystemNodes;
import static apoc.util.SystemDbUtil.withSystemDb;
import static apoc.uuid.UuidConfig.*;
import static apoc.uuid.UuidConfig.ADD_TO_SET_LABELS_KEY;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

public class CypherProceduresHandlerNewProcedures {
    public static final String PREFIX = "custom";

    // todo - installFunction
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

            setLastUpdate(databaseName, tx);

//            registerProcedure(signature, statement);
//            return null;
        });
    }

    public static Mode mode(String s) {
        return s == null ? Mode.READ : Mode.valueOf(s.toUpperCase());
    }

    // todo - installProcedure
    public static void installFunction(String databaseName, UserFunctionSignature signature, String statement, boolean forceSingle) {
        withSystemDb(tx -> {
            Node node = Util.mergeNode(tx, SystemLabels.ApocCypherProcedures, SystemLabels.Function,
                    Pair.of(database.name(), databaseName),
                    Pair.of(name.name(), signature.name().name()),
                    Pair.of(prefix.name(), signature.name().namespace())
            );
            node.setProperty(description.name(), signature.description().orElse(null));
            node.setProperty(SystemPropertyKeys.statement.name(), statement);
            node.setProperty(inputs.name(), serializeSignatures(signature.inputSignature()));
            node.setProperty(output.name(), signature.outputType().toString());
            node.setProperty(SystemPropertyKeys.forceSingle.name(), forceSingle);

            setLastUpdate(databaseName, tx);
//            registerFunction(signature, statement, forceSingle);
//            return null;
        });
    }

    // todo - list
    public static Stream<CustomProcedureInfo> show(String databaseName, Transaction tx) {
        /*List<CypherProceduresHandler.ProcedureOrFunctionDescriptor> descriptors;*/
//        return /*Stream<CypherProceduresHandler.ProcedureOrFunctionDescriptor> stream =*/ withSystemDb(apocConfig(), tx -> {
        return getCustomNodes(databaseName, tx).stream().map(node -> {
            if (node.hasLabel(SystemLabels.Procedure)) {
                return getProcedureInfo(node);
            } else if (node.hasLabel(SystemLabels.Function)) {
                return getFunctionInfo(node);
            } else {
                throw new IllegalStateException("don't know what to do with systemdb node " + node);
            }
        });
//        })
//        .map(CustomProcedureInfo::getInfoFromDescriptor);

//        return descriptors.stream();
    }
    public static ResourceIterator<Node> getCustomNodes(String databaseName, Transaction tx) {
        return getCustomNodes(databaseName, tx, null);
    }

    // todo - common method in SystemDbUtils
    public static ResourceIterator<Node> getCustomNodes(String databaseName, Transaction tx, Map<String, Object> props) {
        return getSystemNodes(databaseName, tx, SystemLabels.ApocCypherProcedures, props);
    }

    // todo - common method in SystemDbUtils
    public static Stream<UuidInfo> getCustomNodesList(String databaseName, Transaction tx) {
        return getCustomNodes(databaseName, tx)
                .stream()
                .map(UuidInfo::fromNode);
    }

    public static CustomProcedureInfo getFunctionInfo(Node node) {
        String statement = (String) node.getProperty(SystemPropertyKeys.statement.name());

        String name = (String) node.getProperty(SystemPropertyKeys.name.name());
        String description = (String) node.getProperty(SystemPropertyKeys.description.name(), null);
        String[] prefix = (String[]) node.getProperty(SystemPropertyKeys.prefix.name(), new String[]{PREFIX});

        String property = (String) node.getProperty(SystemPropertyKeys.inputs.name());
        List<FieldSignature> inputs = deserializeSignatures(property);

        boolean forceSingle = (boolean) node.getProperty(SystemPropertyKeys.forceSingle.name(), false);

        UserFunctionSignature signature = new UserFunctionSignature(
                new QualifiedName(prefix, name),
                inputs,
                typeof((String) node.getProperty(SystemPropertyKeys.output.name())),
                null,
                new String[0],
                description,
                "apoc.custom",
                false
        );

//        CypherProceduresHandler.UserFunctionDescriptor userFunctionDescriptor = (CypherProceduresHandler.UserFunctionDescriptor) descriptor;
//        UserFunctionSignature signature = userFunctionDescriptor.getSignature();
        return new CustomProcedureInfo(
                FUNCTION,
                signature.name().toString().substring(PREFIX.length() + 1),
                signature.description().orElse(null),
                null,
                statement,
                convertInputSignature(signature.inputSignature()),
                prettyPrintType(signature.outputType()),
                forceSingle);
    }

    public static CustomProcedureInfo getProcedureInfo(Node node) {
        String statement = (String) node.getProperty(SystemPropertyKeys.statement.name());

        String name = (String) node.getProperty(SystemPropertyKeys.name.name());
        String description = (String) node.getProperty(SystemPropertyKeys.description.name(), null);
        String[] prefix = (String[]) node.getProperty(SystemPropertyKeys.prefix.name(), new String[]{PREFIX});

        String property = (String) node.getProperty(SystemPropertyKeys.inputs.name());
        List<FieldSignature> inputs = deserializeSignatures(property);

        List<FieldSignature> outputSignature = deserializeSignatures((String) node.getProperty(SystemPropertyKeys.outputs.name()));

        ProcedureSignature signature = Signatures.createProcedureSignature(
                new QualifiedName(prefix, name),
                inputs,
                outputSignature,
                Mode.valueOf((String) node.getProperty(SystemPropertyKeys.mode.name())),
                false,
                null,
                new String[0],
                description,
                null,
                false,
                false,
                false,
                false,
                false);

            return new CustomProcedureInfo(
                    PROCEDURE,
                    signature.name().toString().substring(PREFIX.length() + 1),
                    signature.description().orElse(null),
                    signature.mode().toString().toLowerCase(),
                    statement,
                    convertInputSignature(signature.inputSignature()),
                    Iterables.asList(Iterables.map(f -> Arrays.asList(f.name(), prettyPrintType(f.neo4jType())), signature.outputSignature())),
                    null);

    }

//    private static CypherProceduresHandler.UserFunctionDescriptor userFunctionDescriptor(Node node) {
//        String statement = (String) node.getProperty(SystemPropertyKeys.statement.name());
//
//        String name = (String) node.getProperty(SystemPropertyKeys.name.name());
//        String description = (String) node.getProperty(SystemPropertyKeys.description.name(), null);
//        String[] prefix = (String[]) node.getProperty(SystemPropertyKeys.prefix.name(), new String[]{PREFIX});
//
//        String property = (String) node.getProperty(SystemPropertyKeys.inputs.name());
//        List<FieldSignature> inputs = deserializeSignatures(property);
//
//        boolean forceSingle = (boolean) node.getProperty(SystemPropertyKeys.forceSingle.name(), false);
//        return new CypherProceduresHandler.UserFunctionDescriptor(new UserFunctionSignature(
//                new QualifiedName(prefix, name),
//                inputs,
//                typeof((String) node.getProperty(SystemPropertyKeys.output.name())),
//                null,
//                new String[0],
//                description,
//                "apoc.custom",
//                false
//        ), statement, forceSingle);
//    }

    // todo - dropFunction
    public static void dropFunction(String databaseName, String name) {
        withSystemDb(tx -> {
            QualifiedName qName = qualifiedName(name);
            getCustomNodes(databaseName, tx,
                    Map.of(SystemPropertyKeys.name.name(), qName.name(),
                            SystemPropertyKeys.prefix.name(), qName.namespace())
            )
            .stream()
            .filter(n -> n.hasLabel(SystemLabels.Function)).forEach(node -> {
                node.delete();
                setLastUpdate(databaseName, tx);
            });
        });
    }

    // todo - dropProcedure
    public static void dropProcedure(String databaseName, String name) {
        withSystemDb(tx -> {
            QualifiedName qName = qualifiedName(name);
            tx.findNodes(SystemLabels.ApocCypherProcedures,
                    SystemPropertyKeys.database.name(), databaseName,
                    SystemPropertyKeys.name.name(), qName.name(),
                    SystemPropertyKeys.prefix.name(), qName.namespace()
            ).stream().filter(n -> n.hasLabel(SystemLabels.Procedure)).forEach(node -> {
//                CypherProceduresHandler.ProcedureDescriptor descriptor = procedureDescriptor(node);
//                registerProcedure(descriptor.getSignature(), null);
//                registeredProcedureSignatures.remove(descriptor.getSignature());
                node.delete();

                setLastUpdate(databaseName, tx);
            });
//            return null;
        });
    }

    public static QualifiedName qualifiedName(@Name("name") String name) {
        String[] names = name.split("\\.");
        List<String> namespace = new ArrayList<>(names.length);
        namespace.add(PREFIX);
        namespace.addAll(Arrays.asList(names));
        return new QualifiedName(namespace.subList(0, namespace.size() - 1), names[names.length - 1]);
    }

    // todo - common with CypherProceduresHandler
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

    // todo - common
    private static void setLastUpdate(String databaseName, Transaction tx) {
        SystemDbUtil.setLastUpdate(databaseName, tx, ApocCypherProceduresMeta);
    }

}
