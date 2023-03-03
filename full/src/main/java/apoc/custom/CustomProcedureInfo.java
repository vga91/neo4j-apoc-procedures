package apoc.custom;

import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.procs.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static apoc.custom.CypherProceduresHandler.FUNCTION;
import static apoc.custom.CypherProceduresHandler.PREFIX;
import static apoc.custom.CypherProceduresHandler.PROCEDURE;

public class CustomProcedureInfo {
    public String type;
    public String name;
    public String description;
    public String mode;
    public String statement;
    public List<List<String>>inputs;
    public Object outputs;
    public Boolean forceSingle;

    public CustomProcedureInfo(String type, String name, String description, String mode,
                               String statement, List<List<String>> inputs, Object outputs,
                               Boolean forceSingle){
        this.type = type;
        this.name = name;
        this.description = description;
        this.statement = statement;
        this.outputs = outputs;
        this.inputs = inputs;
        this.forceSingle = forceSingle;
        this.mode = mode;
    }

    public static CustomProcedureInfo getInfoFromDescriptor(CypherProceduresHandler.ProcedureOrFunctionDescriptor descriptor) {
        if (descriptor instanceof CypherProceduresHandler.ProcedureDescriptor) {
            CypherProceduresHandler.ProcedureDescriptor procedureDescriptor = (CypherProceduresHandler.ProcedureDescriptor) descriptor;
            ProcedureSignature signature = procedureDescriptor.getSignature();
            return new CustomProcedureInfo(
                    PROCEDURE,
                    signature.name().toString().substring(PREFIX.length() + 1),
                    signature.description().orElse(null),
                    signature.mode().toString().toLowerCase(),
                    procedureDescriptor.getStatement(),
                    convertInputSignature(signature.inputSignature()),
                    Iterables.asList(Iterables.map(f -> Arrays.asList(f.name(), prettyPrintType(f.neo4jType())), signature.outputSignature())),
                    null);
        } else {
            CypherProceduresHandler.UserFunctionDescriptor userFunctionDescriptor = (CypherProceduresHandler.UserFunctionDescriptor) descriptor;
            UserFunctionSignature signature = userFunctionDescriptor.getSignature();
            return new CustomProcedureInfo(
                    FUNCTION,
                    signature.name().toString().substring(PREFIX.length() + 1),
                    signature.description().orElse(null),
                    null,
                    userFunctionDescriptor.getStatement(),
                    convertInputSignature(signature.inputSignature()),
                    prettyPrintType(signature.outputType()),
                    userFunctionDescriptor.isForceSingle());
        }
    }

    public static List<List<String>> convertInputSignature(List<FieldSignature> signatures) {
        return Iterables.asList(Iterables.map(f -> {
            List<String> list = new ArrayList<>(3);
            list.add(f.name());
            list.add(prettyPrintType(f.neo4jType()));
            final Optional<DefaultParameterValue> defaultParameterValue = f.defaultValue();
            defaultParameterValue.map(DefaultParameterValue::value).ifPresent(v -> list.add(v.toString()));
            return list;
        }, signatures));
    }

    public static String prettyPrintType(Neo4jTypes.AnyType type) {
        String s = type.toString().toLowerCase();
        if (s.endsWith("?")) {
            s = s.substring(0, s.length()-1);
        }
        return s;
    }
}
