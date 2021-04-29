package apoc.custom;

import apoc.Extended;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Mode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.custom.CypherProceduresHandler.*;
import static java.util.Collections.emptyMap;

/**
 * @author mh
 * @since 18.08.18
 */
@Extended
public class CypherProcedures {

    // visible for testing
    public static final String ERROR_DUPLICATED_INPUT = "There are duplicated input parameter name";
    public static final String ERROR_MISMATCHED_INPUTS = "Required query parameters and input parameters don't correspond.";
    
    @Context
    public GraphDatabaseAPI api;

    @Context
    public KernelTransaction ktx;

    @Context
    public Log log;

    @Context
    public CypherProceduresHandler cypherProceduresHandler;

    /*
     * store in graph properties, load at startup
     * allow to register proper params as procedure-params
     * allow to register proper return columns
     * allow to register mode
     */
    @Procedure(value = "apoc.custom.asProcedure",mode = Mode.WRITE, deprecatedBy = "apoc.custom.declareProcedure")
    @Description("apoc.custom.asProcedure(name, statement, mode, outputs, inputs, description) - register a custom cypher procedure")
    @Deprecated
    public void asProcedure(@Name("name") String name, @Name("statement") String statement,
                            @Name(value = "mode",defaultValue = "read") String mode,
                            @Name(value= "outputs", defaultValue = "null") List<List<String>> outputs,
                            @Name(value= "inputs", defaultValue = "null") List<List<String>> inputs,
                            @Name(value= "description", defaultValue = "") String description
    ) throws ProcedureException {
        ProcedureSignature signature = cypherProceduresHandler.procedureSignature(name, mode, outputs, inputs, description);
        validateInputs(statement, signature.inputSignature());
        cypherProceduresHandler.storeProcedure(signature, statement);
    }

    @Procedure(value = "apoc.custom.declareProcedure", mode = Mode.WRITE)
    @Description("apoc.custom.declareProcedure(signature, statement, mode, description) - register a custom cypher procedure")
    public void declareProcedure(@Name("signature") String signature, @Name("statement") String statement,
                                 @Name(value = "mode", defaultValue = "read") String mode,
                                 @Name(value = "description", defaultValue = "") String description
    ) {
        ProcedureSignature procedureSignature = new Signatures(PREFIX).asProcedureSignature(signature, description, cypherProceduresHandler.mode(mode));
        validateInputs(statement, procedureSignature.inputSignature());
        if (!cypherProceduresHandler.registerProcedure(procedureSignature, statement)) {
            throw new IllegalStateException("Error registering procedure " + procedureSignature.name() + ", see log.");
        }
        cypherProceduresHandler.storeProcedure(procedureSignature, statement);
    }


    @Procedure(value = "apoc.custom.asFunction",mode = Mode.WRITE, deprecatedBy = "apoc.custom.declareFunction")
    @Description("apoc.custom.asFunction(name, statement, outputs, inputs, forceSingle, description) - register a custom cypher function")
    @Deprecated
    public void asFunction(@Name("name") String name, @Name("statement") String statement,
                           @Name(value= "outputs", defaultValue = "") String output,
                           @Name(value= "inputs", defaultValue = "null") List<List<String>> inputs,
                           @Name(value = "forceSingle", defaultValue = "false") boolean forceSingle,
                           @Name(value = "description", defaultValue = "") String description) throws ProcedureException {
        UserFunctionSignature signature = cypherProceduresHandler.functionSignature(name, output, inputs, description);
        validateInputs(statement, signature.inputSignature());
        cypherProceduresHandler.storeFunction(signature, statement, forceSingle);
    }

    @Procedure(value = "apoc.custom.declareFunction", mode = Mode.WRITE)
    @Description("apoc.custom.declareFunction(signature, statement, forceSingle, description) - register a custom cypher function")
    public void declareFunction(@Name("signature") String signature, @Name("statement") String statement,
                           @Name(value = "forceSingle", defaultValue = "false") boolean forceSingle,
                           @Name(value = "description", defaultValue = "") String description) throws ProcedureException {
        UserFunctionSignature userFunctionSignature = new Signatures(PREFIX).asFunctionSignature(signature, description);
        validateInputs(statement, userFunctionSignature.inputSignature());
        if (!cypherProceduresHandler.registerFunction(userFunctionSignature, statement, forceSingle)) {
            throw new IllegalStateException("Error registering function " + signature + ", see log.");
        }
        cypherProceduresHandler.storeFunction(userFunctionSignature, statement, forceSingle);
    }


    @Procedure(value = "apoc.custom.list", mode = Mode.READ)
    @Description("apoc.custom.list() - provide a list of custom procedures/function registered")
    public Stream<CustomProcedureInfo> list() {
        return cypherProceduresHandler.readSignatures().map( descriptor -> {
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
        });
    }

    @Procedure(value = "apoc.custom.removeProcedure", mode = Mode.WRITE)
    @Description("apoc.custom.removeProcedure(name) - remove the targeted custom procedure")
    public void removeProcedure(@Name("name") String name) {
        Objects.requireNonNull(name, "name");
        cypherProceduresHandler.removeProcedure(name);
    }


    @Procedure(value = "apoc.custom.removeFunction", mode = Mode.WRITE)
    @Description("apoc.custom.removeFunction(name, type) - remove the targeted custom function")
    public void removeFunction(@Name("name") String name) {
        Objects.requireNonNull(name, "name");
        cypherProceduresHandler.removeFunction(name);
    }
    
    private void validateInputs(String statement, List<FieldSignature> inputSignatures) {
        if (isInputSignatureEmptyOrDefault(inputSignatures)) {
            return;
        }
        final Set<String> collect = inputSignatures.stream().map(FieldSignature::name).collect(Collectors.toSet());
        if (collect.size() != inputSignatures.size()) {
            throw new RuntimeException(ERROR_DUPLICATED_INPUT);
        }
        api.executeTransactionally("EXPLAIN " + statement, emptyMap(), result -> {
            StreamSupport.stream(result.getNotifications().spliterator(), false)
                    .filter(i -> i.getCode().equals(Status.Statement.ParameterMissing.code().serialize()))
                    .findFirst()
                    .ifPresent(missingParamNotification -> {
                        final String missingParamDescription = missingParamNotification.getDescription();
                        final String missingParamPrepend = "Missing parameters: ";
                        // we get set of all missing parameters
                        final String stringMissingParam = missingParamDescription
                                .substring(missingParamDescription.indexOf(missingParamPrepend) + missingParamPrepend.length())
                                .replace(")", "");
                        final Set<String> split = Set.of(stringMissingParam.split(", "));

                        if (!split.equals(collect)) {
                            throw new RuntimeException(ERROR_MISMATCHED_INPUTS +
                                    "\nRequired params: " + split +
                                    "\nInput params: " + collect); 
                        } 
                    });
            return null;
        });
    }

    private List<List<String>> convertInputSignature(List<FieldSignature> signatures) {
        return Iterables.asList(Iterables.map(f -> {
            List<String> list = new ArrayList<>(3);
            list.add(f.name());
            list.add(prettyPrintType(f.neo4jType()));
            f.defaultValue().ifPresent(v -> list.add(v.value().toString()));
            return list;
        }, signatures));
    }

    private String prettyPrintType(Neo4jTypes.AnyType type) {
        String s = type.toString().toLowerCase();
        if (s.endsWith("?")) {
            s = s.substring(0, s.length()-1);
        }
        return s;
    }

    public static class CustomProcedureInfo {
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
    }

}
