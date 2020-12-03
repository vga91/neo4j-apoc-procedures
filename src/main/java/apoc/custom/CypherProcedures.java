package apoc.custom;

import apoc.ApocConfiguration;
import apoc.Pools;
import apoc.util.JsonUtil;
import apoc.util.Util;
import org.neo4j.collection.PrefetchingRawIterator;
import org.neo4j.collection.RawIterator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.*;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.CallableUserFunction;
import org.neo4j.kernel.api.proc.Key;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.core.GraphProperties;
import org.neo4j.kernel.impl.core.GraphPropertiesProxy;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.util.Util.map;
import static java.util.Collections.singletonList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.*;

/**
 * @author mh
 * @since 18.08.18
 */
public class CypherProcedures {

    private static final String PREFIX = "custom";
    public static final String FUNCTIONS = "functions";
    public static final String FUNCTION = "function";
    public static final String PROCEDURES = "procedures";
    public static final String PROCEDURE = "procedure";
    public static final List<FieldSignature> DEFAULT_MAP_OUTPUT = singletonList(FieldSignature.inputField("row", NTMap));
    public static final List<FieldSignature> DEFAULT_INPUTS = singletonList(FieldSignature.inputField("params", NTMap, DefaultParameterValue.ntMap(Collections.emptyMap())));

    @Context
    public GraphDatabaseAPI api;
    @Context
    public KernelTransaction ktx;
    @Context
    public Log log;

    /*
     * store in graph properties, load at startup
     * allow to register proper params as procedure-params
     * allow to register proper return columns
     * allow to register mode
     */
    @Procedure(value = "apoc.custom.asProcedure",mode = Mode.WRITE)
    @Description("apoc.custom.asProcedure(name, statement, mode, outputs, inputs, description) - register a custom cypher procedure")
    public void asProcedure(@Name("name") String name, @Name("statement") String statement,
                            @Name(value = "mode",defaultValue = "read") String mode,
                            @Name(value= "outputs", defaultValue = "null") List<List<String>> outputs,
                            @Name(value= "inputs", defaultValue = "null") List<List<String>> inputs,
                            @Name(value= "description", defaultValue = "null") String description
    ) {
        CustomStatementRegistry registry = new CustomStatementRegistry(api, log);
        if (!registry.registerProcedure(name, statement, mode, outputs, inputs, description)) {
            throw new IllegalStateException("Error registering procedure "+name+", see log.");
        }
        CustomProcedureStorage.storeProcedure(api, name, statement, mode, outputs, inputs, description);
    }

    @Procedure(value = "apoc.custom.declareProcedure", mode = Mode.WRITE)
    @Description("apoc.custom.declareProcedure(signature, statement, mode, description) - register a custom cypher procedure")
    public void declareProcedure(@Name("signature") String signature, @Name("statement") String statement,
                                 @Name(value = "mode", defaultValue = "read") String mode,
                                 @Name(value = "description", defaultValue = "null") String description
    ) {
        ProcedureSignature procedureSignature = new Signatures(PREFIX).asProcedureSignature(signature, description, mode(mode));

        CustomStatementRegistry registry = new CustomStatementRegistry(api, log);
        if (!registry.registerProcedure(procedureSignature, statement)) {
            throw new IllegalStateException("Error registering procedure " + procedureSignature.name() + ", see log.");
        }
        CustomProcedureStorage.storeProcedure(api, procedureSignature, statement);
    }

    @Procedure(value = "apoc.custom.asFunction",mode = Mode.WRITE)
    @Description("apoc.custom.asFunction(name, statement, outputs, inputs, forceSingle, description) - register a custom cypher function")
    public void asFunction(@Name("name") String name, @Name("statement") String statement,
                           @Name(value= "outputs", defaultValue = "") String output,
                           @Name(value= "inputs", defaultValue = "null") List<List<String>> inputs,
                           @Name(value = "forceSingle", defaultValue = "false") boolean forceSingle,
                           @Name(value = "description", defaultValue = "null") String description) throws ProcedureException {
        CustomStatementRegistry registry = new CustomStatementRegistry(api, log);
        if (!registry.registerFunction(name, statement, output, inputs, forceSingle, description)) {
            throw new IllegalStateException("Error registering function "+name+", see log.");
        }
        CustomProcedureStorage.storeFunction(api, name, statement, output, inputs, forceSingle, description);
    }

    @Procedure(value = "apoc.custom.declareFunction", mode = Mode.WRITE)
    @Description("apoc.custom.declareFunction(signature, statement, forceSingle, description) - register a custom cypher function")
    public void declareFunction(@Name("signature") String signature, @Name("statement") String statement,
                           @Name(value = "forceSingle", defaultValue = "false") boolean forceSingle,
                           @Name(value = "description", defaultValue = "null") String description) throws ProcedureException {
        UserFunctionSignature userFunctionSignature = new Signatures(PREFIX).asFunctionSignature(signature, description);
        CustomStatementRegistry registry = new CustomStatementRegistry(api, log);
        if (!registry.registerFunction(userFunctionSignature, statement, forceSingle)) {
            throw new IllegalStateException("Error registering function " + signature + ", see log.");
        }
        CustomProcedureStorage.storeFunction(api, userFunctionSignature, statement, forceSingle);
    }

    @Procedure(value = "apoc.custom.list", mode = Mode.READ)
    @Description("apoc.custom.list() - provide a list of custom procedures/functions registered")
    public Stream<CustomProcedureInfo> list(){
        CustomProcedureStorage registry = new CustomProcedureStorage(Pools.NEO4J_SCHEDULER, api, log);
        return registry.list().stream();
    }

    @Procedure(value = "apoc.custom.removeProcedure", mode = Mode.WRITE)
    @Description("apoc.custom.removeProcedure(name) - remove the targeted custom procedure")
    public void removeProcedure(@Name("name") String name) {
        Objects.requireNonNull(name, "name");
        Map<String, Object> old = CustomProcedureStorage.remove(api, name, PROCEDURES);
        if (old != null && !old.isEmpty()) {
            CustomStatementRegistry registry = new CustomStatementRegistry(api, log);
            if (!registry.removeProcedure(name, old)) {
                throw new IllegalStateException("Error removing custom procedure:" + name + ", see log.");
            }
        }
    }

    @Procedure(value = "apoc.custom.removeFunction", mode = Mode.WRITE)
    @Description("apoc.custom.removeFunction(name, type) - remove the targeted custom function")
    public void removeFunction(@Name("name") String name) {
        Objects.requireNonNull(name, "name");
        Map<String, Object> old = CustomProcedureStorage.remove(api, name, FUNCTIONS);
        if (old != null && !old.isEmpty()) {
            CustomStatementRegistry registry = new CustomStatementRegistry(api, log);
            if (!registry.removeFunction(name, old)) {
                throw new IllegalStateException("Error removing custom function:" + name + ", see log.");
            }
        }
    }

    @Procedure(value = "apoc.custom.restore")
    @Description("apoc.custom.restore - refresh procedures and functions")
    public void restore() {
        new CustomProcedureStorage(Pools.NEO4J_SCHEDULER, api, log).restoreProceduresSync();
    }

    static class CustomStatementRegistry {
        GraphDatabaseAPI api;
        Procedures procedures;
        private final Log log;

        public CustomStatementRegistry(GraphDatabaseAPI api, Log log) {
            this.api = api;
            procedures = api.getDependencyResolver().resolveDependency(Procedures.class);
            this.log = log;
        }

        public boolean registerProcedure(String name, String statement, String mode, List<List<String>> outputs, List<List<String>> inputs, String description) {
            boolean admin = false; // TODO
            ProcedureSignature signature = Signatures.createProcedureSignature(qualifiedName(name), inputSignatures(inputs), outputSignatures(outputs),
                    mode(mode), admin, null, new String[0], description, null, false, true, false
            );
            return registerProcedure(signature, statement);

        }

        public boolean registerProcedure(ProcedureSignature signature, String statement) {
            try {
                Procedures procedures = api.getDependencyResolver().resolveDependency(Procedures.class);
                procedures.register(new CallableProcedure.BasicProcedure(signature) {
                    @Override
                    public RawIterator<Object[], ProcedureException> apply(org.neo4j.kernel.api.proc.Context ctx, Object[] input, ResourceTracker resourceTracker) throws ProcedureException {
                        KernelTransaction ktx = ctx.get(Key.key("KernelTransaction", KernelTransaction.class));
                        Map<String, Object> params = params(input, signature);
                        Result result = api.execute(statement, params);
                        resourceTracker.registerCloseableResource(result);
                        List<FieldSignature> outputs = signature.outputSignature();
                        String[] names = outputs == null ? null : outputs.stream().map(FieldSignature::name).toArray(String[]::new);
                        boolean defaultOutputs = outputs == null || outputs.equals(DEFAULT_MAP_OUTPUT);
                        return new PrefetchingRawIterator<Object[], ProcedureException>() {
                            @Override
                            protected Object[] fetchNextOrNull() {
                                if (!result.hasNext()) return null;
                                Map<String, Object> row = result.next();
                                return toResult(row, names, defaultOutputs);
                            }
                        };
                    }
                }, true);
                return true;
            } catch (Exception e) {
                log.error("Could not register procedure: " + signature.name() + " with " + statement + "\n accepting" + signature.inputSignature() + " resulting in " + signature.outputSignature() + " mode " + signature.mode(), e);
                return false;
            }
        }

        public boolean removeProcedure(String name, List<List<String>> inputs, List<List<String>> outputs, String description, String mode) {
            boolean admin = false; // TODO
            ProcedureSignature signature = Signatures.createProcedureSignature(qualifiedName(name), inputSignatures(inputs), outputSignatures(outputs),
                    mode(mode), admin, null, new String[0], description, null, false, true, false
            );
            return removeProcedure(signature);
        }

        private boolean removeProcedure(ProcedureSignature signature) {
            try {
                Procedures procedures = api.getDependencyResolver().resolveDependency(Procedures.class);
                procedures.register(new CallableProcedure.BasicProcedure(signature) {
                    @Override
                    public RawIterator<Object[], ProcedureException> apply(org.neo4j.kernel.api.proc.Context ctx, Object[] input, ResourceTracker resourceTracker) throws ProcedureException {
                        final String error = String.format("There is no procedure with the name `%s` registered for this database instance. " +
                                "Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed.", signature.name());
                        throw new QueryExecutionException(error, null, "Neo.ClientError.Procedure.ProcedureNotFound");
                    }
                }, true);
                return true;
            } catch (Exception e) {
                log.error("Could not remove procedure: " + signature, e);
                return false;
            }
        }

        private boolean removeProcedure(String procedureName, Map<String, Object> procedureMetadata) {
            boolean deleted;
            if (procedureMetadata.containsKey("signature")) {
                Signatures sigs = new Signatures("custom");
                ProcedureSignature procedureSignature = sigs.asProcedureSignature((String) procedureMetadata.get("signature"),
                        (String) procedureMetadata.get("description"),
                        Mode.valueOf((String) procedureMetadata.get("mode")));
                deleted = removeProcedure(procedureSignature);
            } else {
                deleted = removeProcedure(procedureName, (List<List<String>>) procedureMetadata.get("inputs"),
                        (List<List<String>>) procedureMetadata.get("outputs"),
                        (String) procedureMetadata.get("description"),
                        (String) procedureMetadata.get("mode"));
            }
            return deleted;
        }

        public boolean registerFunction(String name, String statement, String output, List<List<String>> inputs, boolean forceSingle, String description)  {
            AnyType outType = typeof(output.isEmpty() ? "LIST OF MAP" : output);
            UserFunctionSignature signature = new UserFunctionSignature(qualifiedName(name), inputSignatures(inputs), outType,
                    null, new String[0], description, false);
            return registerFunction(signature, statement, forceSingle);
        }

        public boolean registerFunction(UserFunctionSignature signature, String statement, boolean forceSingle) {
            try {
                DefaultValueMapper defaultValueMapper = new DefaultValueMapper(api.getDependencyResolver().resolveDependency(GraphDatabaseFacade.class));
                AnyType outType = signature.outputType();
                procedures.register(new CallableUserFunction.BasicUserFunction(signature) {
                    @Override
                    public AnyValue apply(org.neo4j.kernel.api.proc.Context ctx, AnyValue[] input) {
                        Map<String, Object> params = functionParams(input, signature, defaultValueMapper);
                        try (Result result = api.execute(statement, params)) {
//                resourceTracker.registerCloseableResource(result); // TODO
                            if (!result.hasNext()) return null;
                            if (outType.equals(NTAny)) {
                                return ValueUtils.of(result.stream().collect(Collectors.toList()));
                            }
                            List<String> cols = result.columns();
                            if (cols.isEmpty()) return null;
                            if (!forceSingle && outType instanceof ListType) {
                                ListType listType = (ListType) outType;
                                AnyType innerType = listType.innerType();
                                if (innerType instanceof MapType)
                                    return ValueUtils.of(result.stream().collect(Collectors.toList()));
                                if (cols.size() == 1)
                                    return ValueUtils.of(result.stream().map(row -> row.get(cols.get(0))).collect(Collectors.toList()));
                            } else {
                                Map<String, Object> row = result.next();
                                if (outType instanceof MapType) return ValueUtils.of(row);
                                if (cols.size() == 1) return ValueUtils.of(row.get(cols.get(0)));
                            }
                            throw new IllegalStateException("Result mismatch " + cols + " output type is " + outType);
                        }
                    }
                }, true);
                return true;
            } catch (Exception e) {
                log.error("Could not register function: " + signature + "\nwith: " + statement + "\n single result " + forceSingle, e);
                return false;
            }

        }

        public boolean removeFunction(String name, String output, List<List<String>> inputs, String description)  {
            AnyType outType = typeof(output.isEmpty() ? "LIST OF MAP" : output);
            UserFunctionSignature signature = new UserFunctionSignature(qualifiedName(name), inputSignatures(inputs), outType,
                    null, new String[0], description, false);
            return removeFunction(signature);
        }

        private boolean removeFunction(UserFunctionSignature signature) {
            try {
                procedures.register(new CallableUserFunction.BasicUserFunction(signature) {
                    @Override
                    public AnyValue apply(org.neo4j.kernel.api.proc.Context ctx, AnyValue[] input) {
                        final String error = String.format("Unknown function '%s'", signature.name());
                        throw new QueryExecutionException(error, null, "Neo.ClientError.Statement.SyntaxError");
                    }
                }, true);
                return true;
            } catch (Exception e) {
                log.error("Could not remove function: " + signature, e);
                return false;
            }
        }

        private boolean removeFunction(String functionName, Map<String, Object> functionMetadata) {
            boolean deleted;
            if (functionMetadata.containsKey("signature")) {
                Signatures sigs = new Signatures("custom");
                UserFunctionSignature userFunctionSignature = sigs.asFunctionSignature((String) functionMetadata.get("signature"),
                        (String) functionMetadata.get("description"));
                deleted = removeFunction(userFunctionSignature);
            } else {
                deleted = removeFunction(functionName, (String) functionMetadata.get("output"),
                        (List<List<String>>) functionMetadata.get("inputs"),
                        (String) functionMetadata.get("description"));
            }
            return deleted;
        }

        public static QualifiedName qualifiedName(@Name("name") String name) {
            String[] names = name.split("\\.");
            List<String> namespace = new ArrayList<>(names.length);
            namespace.add(PREFIX);
            namespace.addAll(Arrays.asList(names));
            return new QualifiedName(namespace.subList(0,namespace.size()-1), names[names.length-1]);
        }

        public List<FieldSignature> inputSignatures(@Name(value = "inputs", defaultValue = "null") List<List<String>> inputs) {
            List<FieldSignature> inputSignature = inputs == null ? DEFAULT_INPUTS :
                    inputs.stream().map(pair -> {
                        DefaultParameterValue defaultValue = defaultValue(pair.get(1), pair.size() > 2 ? pair.get(2) : null);
                        return defaultValue == null ?
                                FieldSignature.inputField(pair.get(0), typeof(pair.get(1))) :
                                FieldSignature.inputField(pair.get(0), typeof(pair.get(1)), defaultValue);
                    }).collect(Collectors.toList());
            ;
            return inputSignature;
        }

        public List<FieldSignature> outputSignatures(@Name(value = "outputs", defaultValue = "null") List<List<String>> outputs) {
            return outputs == null ? DEFAULT_MAP_OUTPUT :
                    outputs.stream().map(pair -> FieldSignature.outputField(pair.get(0),typeof(pair.get(1)))).collect(Collectors.toList());
        }

        private Neo4jTypes.AnyType typeof(String typeName) {
            typeName = typeName.toUpperCase();
            if (typeName.startsWith("LIST OF ")) return NTList(typeof(typeName.substring(8)));
            if (typeName.startsWith("LIST ")) return NTList(typeof(typeName.substring(5)));
            switch (typeName) {
                case "ANY": return NTAny;
                case "MAP": return NTMap;
                case "NODE": return NTNode;
                case "REL": return NTRelationship;
                case "RELATIONSHIP": return NTRelationship;
                case "EDGE": return NTRelationship;
                case "PATH": return NTPath;
                case "NUMBER": return NTNumber;
                case "LONG": return NTInteger;
                case "INT": return NTInteger;
                case "INTEGER": return NTInteger;
                case "FLOAT": return NTFloat;
                case "DOUBLE": return NTFloat;
                case "BOOL": return NTBoolean;
                case "BOOLEAN": return NTBoolean;
                case "DATE": return NTDate;
                case "TIME": return NTTime;
                case "LOCALTIME": return NTLocalTime;
                case "DATETIME": return NTDateTime;
                case "LOCALDATETIME": return NTLocalDateTime;
                case "DURATION": return NTDuration;
                case "POINT": return NTPoint;
                case "GEO": return NTGeometry;
                case "GEOMETRY": return NTGeometry;
                case "STRING": return NTString;
                case "TEXT": return NTString;
                default: return NTString;
            }
        }
        private DefaultParameterValue defaultValue(String typeName, String stringValue) {
            if (stringValue == null) return null;
            Object value = JsonUtil.parse(stringValue, null, Object.class);
            if (value == null) return null;
            typeName = typeName.toUpperCase();
            if (typeName.startsWith("LIST ")) return DefaultParameterValue.ntList((List<?>) value,typeof(typeName.substring(5)));
            switch (typeName) {
                case "MAP": return DefaultParameterValue.ntMap((Map<String, Object>) value);
                case "NODE":
                case "REL":
                case "RELATIONSHIP":
                case "EDGE":
                case "PATH": return null;
                case "NUMBER": return value instanceof Float || value instanceof Double ? DefaultParameterValue.ntFloat(((Number)value).doubleValue()) : DefaultParameterValue.ntInteger(((Number)value).longValue());
                case "LONG":
                case "INT":
                case "INTEGER": return DefaultParameterValue.ntInteger(((Number)value).longValue());
                case "FLOAT":
                case "DOUBLE": return DefaultParameterValue.ntFloat(((Number)value).doubleValue());
                case "BOOL":
                case "BOOLEAN": return DefaultParameterValue.ntBoolean((Boolean)value);
                case "DATE":
                case "TIME":
                case "LOCALTIME":
                case "DATETIME":
                case "LOCALDATETIME":
                case "DURATION":
                case "POINT":
                case "GEO":
                case "GEOMETRY": return null;
                case "STRING":
                case "TEXT": return DefaultParameterValue.ntString(value.toString());
                default: return null;
            }
        }

        private Object[] toResult(Map<String, Object> row, String[] names, boolean defaultOutputs) {
            if (defaultOutputs) return new Object[]{row};
            Object[] result = new Object[names.length];
            for (int i = 0; i < names.length; i++) {
                result[i] = row.get(names[i]);
            }
            return result;
        }

        public Map<String, Object> params(Object[] input, ProcedureSignature signature) {
            if (input == null || input.length == 0) return Collections.emptyMap();
            List<FieldSignature> inputs = signature.inputSignature();
            if (inputs == null || inputs.isEmpty() || inputs.equals(DEFAULT_INPUTS))
                return (Map<String, Object>) input[0];
            Map<String, Object> params = new HashMap<>(input.length);
            for (int i = 0; i < input.length; i++) {
                params.put(inputs.get(i).name(), input[i]);
            }
            return params;
        }

        public Map<String, Object> functionParams(Object[] input, UserFunctionSignature signature, DefaultValueMapper mapper) {
            if (input == null || input.length == 0) return Collections.emptyMap();
            List<FieldSignature> inputs = signature.inputSignature();
            if (inputs == null || inputs.isEmpty() || inputs.equals(DEFAULT_INPUTS))
                return (Map<String, Object>) ((MapValue) input[0]).map(mapper);
            Map<String, Object> params = new HashMap<>(input.length);
            for (int i = 0; i < input.length; i++) {
                params.put(inputs.get(i).name(), ((AnyValue) input[i]).map(mapper));
            }
            return params;
        }
    }

    public static Mode mode(String s) {
        return s == null ? Mode.READ : Mode.valueOf(s.toUpperCase());
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

    public static class CustomProcedureStorage implements AvailabilityListener {
        public static final String APOC_CUSTOM = "apoc.custom";
        public static final String APOC_CUSTOM_UPDATE = "apoc.custom.update";
        private GraphProperties properties;
        private final JobScheduler scheduler;
        private final GraphDatabaseAPI api;
        private final Log log;
        private long lastUpdate;
        public static Group REFRESH_GROUP = Group.STORAGE_MAINTENANCE;
        private JobHandle restoreProceduresHandle;

        public CustomProcedureStorage(JobScheduler neo4jScheduler, GraphDatabaseAPI api, Log log) {
            this.scheduler = neo4jScheduler;
            this.api = api;
            this.log = log;
        }

        private void restoreProceduresSync() {
            properties = getProperties(api);
            restoreProcedures();
        }

        @Override
        public void available() {
            restoreProceduresSync();
            long refreshInterval = Long.valueOf(ApocConfiguration.get("custom.procedures.refresh", "60000"));
            restoreProceduresHandle = scheduler.scheduleRecurring(REFRESH_GROUP, () -> restoreProcedures(), refreshInterval, refreshInterval, TimeUnit.MILLISECONDS);
        }

        public static GraphPropertiesProxy getProperties(GraphDatabaseAPI api) {
            return api.getDependencyResolver().resolveDependency(EmbeddedProxySPI.class).newGraphPropertiesProxy();
        }

        private void restoreProcedures() {
            if (getLastUpdate(properties) <= lastUpdate) return;
            lastUpdate = System.currentTimeMillis();
            CustomStatementRegistry registry = new CustomStatementRegistry(api, log);
            Map<String, Map<String, Map<String, Object>>> stored = readData(properties);
            Signatures sigs = new Signatures("custom");
            stored.get(FUNCTIONS).forEach((name, data) -> {
                String description = parseStoredDescription(data.get("description"));
                if (data.containsKey("signature")) {
                    UserFunctionSignature userFunctionSignature = sigs.asFunctionSignature((String) data.get("signature"), description);
                    registry.registerFunction(userFunctionSignature, (String) data.get("statement"), (Boolean) data.get("forceSingle"));
                } else {
                    registry.registerFunction(name, (String) data.get("statement"), (String) data.get("output"),
                            (List<List<String>>) data.get("inputs"), (Boolean) data.get("forceSingle"), description);
                }
            });
            stored.get(PROCEDURES).forEach((name, data) -> {
                String description = parseStoredDescription(data.get("description"));
                if (data.containsKey("signature")) {
                    ProcedureSignature procedureSignature = sigs.asProcedureSignature((String) data.get("signature"), description, Mode.valueOf((String) data.get("mode")));
                    registry.registerProcedure(procedureSignature, (String) data.get("statement"));
                } else {
                    registry.registerProcedure(name, (String) data.get("statement"), (String) data.get("mode"),
                            (List<List<String>>) data.get("outputs"), (List<List<String>>) data.get("inputs"), description);
                }
            });
            clearQueryCaches(api);
        }

        private String parseStoredDescription(Object rawDescription) {
            // Description was changed to be an Optional in the Neo4j internal API. Optional.None serialized to
            // JSON objects in various stores around the world; hence, deal with this value not being a string
            if(rawDescription instanceof String) {
                if("null".equals(rawDescription)) {
                    return null;
                }
                return (String)rawDescription;
            }
            return null;
        }

        @Override
        public void unavailable() {
            if (restoreProceduresHandle != null) {
                restoreProceduresHandle.cancel(false);
            }
            properties = null;
        }

        public static Map<String, Object> storeProcedure(GraphDatabaseAPI api, String name, String statement, String mode, List<List<String>> outputs, List<List<String>> inputs, String description) {

            Map<String, Object> data = map("statement", statement, "mode", mode, "inputs", inputs, "outputs", outputs, "description", description);
            return updateCustomData(getProperties(api), name, PROCEDURES, data);
        }

        public static Map<String, Object> storeProcedure(GraphDatabaseAPI api, ProcedureSignature signature, String statement) {
            Map<String, Object> data = map("statement", statement, "mode", signature.mode().toString(), "signature", signature.toString(), "description", signature.description().orElse(null));
            return updateCustomData(getProperties(api), signature.name().toString(), PROCEDURES, data);
        }
        public static Map<String, Object> storeFunction(GraphDatabaseAPI api, String name, String statement, String output, List<List<String>> inputs, boolean forceSingle, String description) {
            Map<String, Object> data = map("statement", statement, "forceSingle", forceSingle, "inputs", inputs, "output", output, "description", description);
            return updateCustomData(getProperties(api), name, FUNCTIONS, data);
        }

        public static Map<String, Object> storeFunction(GraphDatabaseAPI api, UserFunctionSignature signature, String statement, boolean forceSingle) {
            Map<String, Object> data = map("statement", statement, "forceSingle", forceSingle, "signature", signature.toString(), "description", signature.description().orElse(null));
            return updateCustomData(getProperties(api), signature.name().toString(), FUNCTIONS, data);
        }

        public synchronized static Map<String, Object> remove(GraphDatabaseAPI api, String name, String type) {
            return updateCustomData(getProperties(api),name, type,null);
        }

        private synchronized static Map<String, Object> updateCustomData(GraphProperties properties, String name, String type, Map<String, Object> value) {
            if (name == null || type==null) return null;
            try (Transaction tx = properties.getGraphDatabase().beginTx()) {
                Map<String, Map<String, Map<String, Object>>> data = readData(properties);
                Map<String, Map<String, Object>> procData = data.get(type);
                Map<String, Object> previous;
                if (value != null) {
                    previous = procData.put(name, value);
                } else {
                    previous = procData.remove(name);
                }
                if (value != null || previous != null) {
                    properties.setProperty(APOC_CUSTOM, Util.toJson(data));
                    properties.setProperty(APOC_CUSTOM_UPDATE, System.currentTimeMillis());
                }
                tx.success();
                return previous;
            }
        }

        private static long getLastUpdate(GraphProperties properties) {
            try (Transaction tx = properties.getGraphDatabase().beginTx()) {
                long lastUpdate = (long) properties.getProperty(APOC_CUSTOM_UPDATE, 0L);
                tx.success();
                return lastUpdate;
            }
        }
        private static Map<String, Map<String,Map<String, Object>>> readData(GraphProperties properties) {
            try (Transaction tx = properties.getGraphDatabase().beginTx()) {
                String procedurePropertyData = (String) properties.getProperty(APOC_CUSTOM, "{\"functions\":{},\"procedures\":{}}");
                Map result = Util.fromJson(procedurePropertyData, Map.class);
                tx.success();
                return result;
            }
        }

        private static void clearQueryCaches(GraphDatabaseService db) {
            try (Transaction tx = db.beginTx()) {
                db.execute("call dbms.clearQueryCaches()").close();
                tx.success();
            }
        }


        public List<CustomProcedureInfo> list() {
            return readData(getProperties(api)).entrySet().stream()
                    .flatMap(entryProcedureType -> {
                        Map<String, Map<String, Object>> procedures = entryProcedureType.getValue();
                        String type = entryProcedureType.getKey();
                        boolean isProcedure = PROCEDURES.equals(type);
                        return procedures.entrySet().stream().map(entryProcedure -> {
                            String typeLabel = isProcedure ? PROCEDURE : FUNCTION;
                            String outputs = isProcedure ? "outputs" : "output";
                            String procedureName = entryProcedure.getKey();
                            Map<String, Object> procedureParams = entryProcedure.getValue();
                            return new CustomProcedureInfo(typeLabel, procedureName,
                                    parseStoredDescription(procedureParams.get("description")),
                                    procedureParams.containsKey("mode")
                                            ? String.valueOf(procedureParams.get("mode")) : null,
                                    String.valueOf(procedureParams.get("statement")),
                                    (List<List<String>>) procedureParams.get("inputs"),
                                    procedureParams.get(outputs),
                                    (Boolean) procedureParams.get("forceSingle"));
                        });
                    })
                    .collect(Collectors.toList());
        }
    }
}
