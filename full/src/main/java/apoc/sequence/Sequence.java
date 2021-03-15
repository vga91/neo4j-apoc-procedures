package apoc.sequence;

import apoc.ApocConfig;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.SCHEMA;

public class Sequence {

    @Context
    public ApocConfig apocConfig;

    @Context
    public Transaction tx;

    @Context
    public SequenceHandler sequenceHandler;

    public static final String SEQUENCE_CONSTRAINT_PREFIX = "Sequence_";


    public static class SequenceResult {
        public final String name;
        public final Long value;

        public SequenceResult(String name, Long value) {
            this.name = name;
            this.value = value;
        }
    }

    @Procedure(mode = SCHEMA)
    @Description("CALL apoc.sequence.create(name, {config}) - create a sequence, save it in a custom node in the system db and return it")
    public Stream<SequenceResult> create(@Name("name") String name, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {

        SequenceConfig conf = new SequenceConfig(config);
        return sequenceHandler.create(name, conf);
    }

    @UserFunction
    @Description("apoc.sequence.currentValue(name) returns the targeted sequence")
    public long currentValue(@Name("name") String name) {

        return sequenceHandler.currentValue(name);
    }

    @UserFunction
    @Description("apoc.sequence.nextValue(name) increments the targeted sequence by one and returns it")
    public long nextValue(@Name("name") String name) {

        return sequenceHandler.nextValue(name);
    }

    @Procedure(mode = SCHEMA)
    @Description("CALL apoc.sequence.drop(name) - remove the targeted sequence and return remaining sequences")
    public Stream<SequenceResult> drop(@Name("name") String name, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {

        SequenceConfig conf = new SequenceConfig(config);
        sequenceHandler.drop(name, conf);

        return list();
    }

    @Procedure
    @Description("CALL apoc.sequence.list() - provide a list of sequences created")
    public Stream<SequenceResult> list() {

        return sequenceHandler.list();
    }



}