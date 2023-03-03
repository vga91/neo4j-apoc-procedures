package apoc.custom;

import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Objects;
import java.util.stream.Stream;

import static apoc.custom.CypherProceduresHandler.PREFIX;


public class CypherProceduresNewProcedures {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public Transaction tx;


    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(value = "apoc.custom.installProcedure", mode = Mode.WRITE)
    @Description("apoc.custom.declareProcedure(signature, statement, mode, description) - register a custom cypher procedure")
    public void declareProcedure(@Name("databaseName") String databaseName,
                                 @Name("signature") String signature,
                                 @Name("statement") String statement,
                                 @Name(value = "mode", defaultValue = "read") String mode,
                                 @Name(value = "description", defaultValue = "") String description
    ) {
        // todo - add initial check
        // todo - add preprocess in old procedures

        Mode modeProcedure = CypherProceduresHandlerNewProcedures.mode(mode);
        ProcedureSignature procedureSignature = new Signatures(PREFIX).asProcedureSignature(signature, description, modeProcedure);

        // todo - execute these validations in refresh() method
//        validateProcedure(statement, procedureSignature.inputSignature(), procedureSignature.outputSignature(), modeProcedure);
//        if (!cypherProceduresHandler.registerProcedure(procedureSignature, statement)) {
//            throw new IllegalStateException("Error registering procedure " + procedureSignature.name() + ", see log.");
//        }
        CypherProceduresHandlerNewProcedures.installProcedure(databaseName, procedureSignature, statement);
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(value = "apoc.custom.installFunction", mode = Mode.WRITE)
    @Description("apoc.custom.declareFunction(signature, statement, forceSingle, description) - register a custom cypher function")
    public void declareFunction(@Name("databaseName") String databaseName,
                                @Name("signature") String signature, @Name("statement") String statement,
                                @Name(value = "forceSingle", defaultValue = "false") boolean forceSingle,
                                @Name(value = "description", defaultValue = "") String description) throws ProcedureException {
        UserFunctionSignature userFunctionSignature = new Signatures(PREFIX).asFunctionSignature(signature, description);

        // todo - execute these validations in refresh() method
//        validateFunction(statement, userFunctionSignature.inputSignature());
//        if (!cypherProceduresHandler.registerFunction(userFunctionSignature, statement, forceSingle)) {
//            throw new IllegalStateException("Error registering function " + signature + ", see log.");
//        }
        CypherProceduresHandlerNewProcedures.installFunction(databaseName, userFunctionSignature, statement, forceSingle);
    }


    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(value = "apoc.custom.dropProcedure", mode = Mode.WRITE)
    @Description("apoc.custom.removeProcedure(name) - remove the targeted custom procedure")
    public void removeProcedure(@Name("databaseName") String databaseName, @Name("name") String name) {
        Objects.requireNonNull(name, "name");
        CypherProceduresHandlerNewProcedures.dropProcedure(databaseName, name);
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(value = "apoc.custom.dropFunction", mode = Mode.WRITE)
    @Description("apoc.custom.removeFunction(name, type) - remove the targeted custom function")
    public void removeFunction(@Name("databaseName") String databaseName, @Name("name") String name) {
        Objects.requireNonNull(name, "name");
        CypherProceduresHandlerNewProcedures.dropFunction(databaseName, name);
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(value = "apoc.custom.show", mode = Mode.READ)
    @Description("apoc.custom.show")
    public Stream<CustomProcedureInfo> show(@Name("databaseName") String databaseName) {
        return CypherProceduresHandlerNewProcedures.show(databaseName, tx);
    }
}


