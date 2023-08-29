package apoc.custom;

import apoc.ExtendedSystemPropertyKeys;
import apoc.SystemPropertyKeys;
import apoc.util.Util;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.procs.DefaultParameterValue;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
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

import static apoc.custom.CypherProceduresHandler.PREFIX;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.*;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTDuration;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTGeometry;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTPoint;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString;

public class CypherProceduresUtil {

    public static QualifiedName qualifiedName(@Name("name") String name) {
        String[] names = name.split("\\.");
        List<String> namespace = new ArrayList<>(names.length);
        namespace.add(PREFIX);
        namespace.addAll(Arrays.asList(names));
        return new QualifiedName(namespace.subList(0, namespace.size() - 1), names[names.length - 1]);
    }

    public static Mode mode(String s) {
        return s == null ? Mode.READ : Mode.valueOf(s.toUpperCase());
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

    public static UserFunctionSignature getUserFunctionSignature(Node node) {
        String name = (String) node.getProperty(SystemPropertyKeys.name.name());
        String description = (String) node.getProperty(ExtendedSystemPropertyKeys.description.name(), null);
        String[] prefix = (String[]) node.getProperty(ExtendedSystemPropertyKeys.prefix.name(), new String[]{PREFIX});

        String property = (String) node.getProperty(ExtendedSystemPropertyKeys.inputs.name());
        List<FieldSignature> inputs = deserializeSignatures(property);

        return new UserFunctionSignature(
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
    }

    public static ProcedureSignature getProcedureSignature(Node node) {
        String name = (String) node.getProperty(SystemPropertyKeys.name.name());
        String description = (String) node.getProperty( ExtendedSystemPropertyKeys.description.name(), null);
        String[] prefix = (String[]) node.getProperty(ExtendedSystemPropertyKeys.prefix.name(), new String[]{PREFIX});

        String property = (String) node.getProperty(ExtendedSystemPropertyKeys.inputs.name());
        List<FieldSignature> inputs = deserializeSignatures(property);

        List<FieldSignature> outputSignature = deserializeSignatures((String) node.getProperty(ExtendedSystemPropertyKeys.outputs.name()));
        return Signatures.createProcedureSignature(
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
    }

    public static List<FieldSignature> deserializeSignatures(String s) {
        List<Map<String, Object>> mapped = Util.fromJson(s, List.class);
        if (mapped.isEmpty()) return ProcedureSignature.VOID;
        return mapped.stream().map(map -> {
            String typeString = (String) map.get("type");
            if (typeString.endsWith("?")) {
                typeString = typeString.substring(0, typeString.length() - 1);
            }
            Neo4jTypes.AnyType type = typeof(typeString);
            // we insert the default value only if is present
            if (map.containsKey("default")) {
                return FieldSignature.inputField((String) map.get("name"), type, new DefaultParameterValue(map.get("default"), type));
            } else {
                return FieldSignature.inputField((String) map.get("name"), type);
            }
        }).collect(Collectors.toList());
    }

    public static Neo4jTypes.AnyType typeof(String typeName) {
        typeName = typeName.replaceAll("\\?", "");
        typeName = typeName.toUpperCase();
        if (typeName.startsWith("LIST OF ")) return NTList(typeof(typeName.substring(8)));
        if (typeName.startsWith("LIST ")) return NTList(typeof(typeName.substring(5)));
        switch (typeName) {
            case "ANY":
                return NTAny;
            case "MAP":
                return NTMap;
            case "NODE":
                return NTNode;
            case "REL", "RELATIONSHIP", "EDGE":
                return NTRelationship;
            case "PATH":
                return NTPath;
            case "NUMBER":
                return NTNumber;
            case "LONG", "INT", "INTEGER":
                return NTInteger;
            case "FLOAT", "DOUBLE":
                return NTFloat;
            case "BOOL", "BOOLEAN":
                return NTBoolean;
            case "DATE":
                return NTDate;
            case "TIME":
                return NTTime;
            case "LOCALTIME":
                return NTLocalTime;
            case "DATETIME":
                return NTDateTime;
            case "LOCALDATETIME":
                return NTLocalDateTime;
            case "DURATION":
                return NTDuration;
            case "POINT":
                return NTPoint;
            case "GEO", "GEOMETRY":
                return NTGeometry;
            default:
                return NTString;
        }
    }
}
