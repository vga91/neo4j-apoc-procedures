package apoc.custom;

import apoc.util.SystemDbUtil;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Stream;

import static apoc.custom.CypherProceduresHandler.PREFIX;


public class CustomNewProcedures {

    @Context
    public GraphDatabaseAPI db;

    @Context
    public Log log;

    @Context
    public Transaction tx;

    private void checkInSystemLeader(String databaseName) {
//        checkEnabled(databaseName);
//        checkRefreshConfigSet();

        SystemDbUtil.checkInSystemLeader(db);
    }

    private void checkTargetDatabase(String databaseName) {
        SystemDbUtil.checkTargetDatabase(tx, databaseName, "Custom procedures/functions");
    }

//    private void checkRefreshConfigSet() {
//        if (!apocConfig().getConfig().containsKey(APOC_UUID_REFRESH)) {
//            throw new RuntimeException(UUID_NOT_SET);
//        }
//    }


    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(value = "apoc.custom.installProcedure", mode = Mode.WRITE)
    // todo - @Description
    @Description("apoc.custom.declareProcedure(signature, statement, mode, description) - register a custom cypher procedure")
    public void declareProcedure(@Name("signature") String signature,
                                 @Name("statement") String statement,
                                 @Name(value = "databaseName", defaultValue = "neo4j") String databaseName,
                                 @Name(value = "mode", defaultValue = "read") String mode,
                                 @Name(value = "description", defaultValue = "") String description) {
        checkInSystemLeader(databaseName);
        checkTargetDatabase(databaseName);
        // todo - add preprocess in old procedures

        Mode modeProcedure = CustomHandler.mode(mode);
        ProcedureSignature procedureSignature = new Signatures(PREFIX).asProcedureSignature(signature, description, modeProcedure);

        // todo - execute these validations in refresh() method
//        validateProcedure(statement, procedureSignature.inputSignature(), procedureSignature.outputSignature(), modeProcedure);
//        if (!cypherProceduresHandler.registerProcedure(procedureSignature, statement)) {
//            throw new IllegalStateException("Error registering procedure " + procedureSignature.name() + ", see log.");
//        }
        CustomHandler.installProcedure(databaseName, procedureSignature, statement);
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(value = "apoc.custom.installFunction", mode = Mode.WRITE)
    // todo - @Description
    @Description("apoc.custom.declareFunction(signature, statement, forceSingle, description) - register a custom cypher function")
    public void declareFunction(@Name("signature") String signature, @Name("statement") String statement,
                                @Name(value = "databaseName", defaultValue = "neo4j") String databaseName,
                                @Name(value = "forceSingle", defaultValue = "false") boolean forceSingle,
                                @Name(value = "description", defaultValue = "") String description) throws ProcedureException {

        checkInSystemLeader(databaseName);
        checkTargetDatabase(databaseName);


        UserFunctionSignature userFunctionSignature = new Signatures(PREFIX).asFunctionSignature(signature, description);

        // todo - execute these validations in refresh() method
//        validateFunction(statement, userFunctionSignature.inputSignature());
//        if (!cypherProceduresHandler.registerFunction(userFunctionSignature, statement, forceSingle)) {
//            throw new IllegalStateException("Error registering function " + signature + ", see log.");
//        }
        CustomHandler.installFunction(databaseName, userFunctionSignature, statement, forceSingle);
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(value = "apoc.custom.dropProcedure", mode = Mode.WRITE)
    // todo - @Description
    @Description("apoc.custom.removeProcedure(name) - remove the targeted custom procedure")
    public void removeProcedure(@Name("name") String name, @Name(value = "databaseName", defaultValue = "neo4j") String databaseName) {
        checkInSystemLeader(databaseName);

        Objects.requireNonNull(name, "name");
        CustomHandler.dropProcedure(databaseName, name);
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(value = "apoc.custom.dropFunction", mode = Mode.WRITE)
    // todo - @Description
    @Description("apoc.custom.removeFunction(name, type) - remove the targeted custom function")
    public void removeFunction(@Name("name") String name, @Name(value = "databaseName", defaultValue = "neo4j") String databaseName) {
        checkInSystemLeader(databaseName);

        Objects.requireNonNull(name, "name");
        CustomHandler.dropFunction(databaseName, name);
    }

    // not to change with @SystemOnlyProcedure because this procedure can be executed in user dbs as well
    // since is a read-only operation
    @SystemProcedure
    @Admin
    @Procedure(value = "apoc.custom.show", mode = Mode.READ)
    // todo - @Description
    @Description("apoc.custom.show")
    public Stream<CustomProcedureInfo> show(@Name(value = "databaseName", defaultValue = "neo4j") String databaseName) {
//        checkEnabled(databaseName);

        return CustomHandler.show(databaseName, tx);
    }

    @SystemProcedure
    @Admin
    @Procedure(value = "apoc.custom.dropAll", mode = Mode.WRITE)
    // todo - @Description
    @Description("apoc.custom.dropAll")
    public Stream<CustomProcedureInfo> dropAll(@Name(value = "databaseName", defaultValue = "neo4j") String databaseName) {

        return CustomHandler.dropAll(databaseName)
                .stream();
    }
}

