package apoc.sequence;

import apoc.ApocConfig;
import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.util.Util;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;

import static apoc.sequence.SequenceHandler.STORAGE;
import static org.neo4j.procedure.Mode.SCHEMA;

public class Sequence {

    @Context
    public ApocConfig apocConfig;

    @Context
    public Transaction tx;

    public static final String SEQUENCE_CONSTRAINT_PREFIX = "Sequence_";

    protected static Node getSequenceNode(Transaction tx, String name) {
        return tx.findNode(SystemLabels.Sequence, SystemPropertyKeys.name.name(), name);
    }

    public static class SequenceResult {
        public final String name;
        public final Long value;

        public SequenceResult(String name, Long value) {
            this.name = name;
            this.value = value;
        }
    }

    public synchronized long getAvailableId(String name){
        AtomicLong id = STORAGE.get(name);
        isSequenceExistent(name, id);
        return id.incrementAndGet();
    }

    @Procedure(mode = SCHEMA)
    @Description("CALL apoc.sequence.create(name, {config}) - create a sequence, save it in a custom node in the system db and return it")
    public Stream<SequenceResult> create(@Name("name") String name, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {

        SequenceConfig conf = new SequenceConfig(config);

        Stream<SequenceResult> resultStream = withSystemDbTx(tx -> {
            List<Pair<String, Object>> list = new ArrayList<>();
            list.add(Pair.of(SystemPropertyKeys.name.name(), name));
            if (conf.isCreateConstraint()) {
                list.add(Pair.of(SystemPropertyKeys.constraintPropertyName.name(), conf.getConstraintPropertyName()));
            }
            Node node = Util.mergeNode(tx,
                    SystemLabels.Sequence,
                    null,
                    list.toArray(Pair[]::new)
            );
            long init = conf.getInitialValue();
            node.setProperty(SystemPropertyKeys.value.name(), init);
            STORAGE.put(name, new AtomicLong(init));
            return Stream.of(new SequenceResult(name, init));
        });

        if (conf.isCreateConstraint()) {
            withSystemDbTx(tx -> {
                try {
                    tx.schema().constraintFor(SystemLabels.Sequence)
                            .withName(SEQUENCE_CONSTRAINT_PREFIX + name)
                            .assertPropertyIsUnique(conf.getConstraintPropertyName()).create();
                } catch (ConstraintViolationException ignored) {}

                return null;
            });
        }

        return resultStream;
    }

    @UserFunction
    @Description("apoc.sequence.currentValue(name) returns the targeted sequence")
    public long currentValue(@Name("name") String name) {

        final AtomicLong id = STORAGE.get(name);
        isSequenceExistent(name, id);
        return id.get();
    }

    @UserFunction
    @Description("apoc.sequence.nextValue(name) increments the targeted sequence by one and returns it")
    public long nextValue(@Name("name") String name) {

        return getAvailableId(name);
    }

    @Procedure(mode = SCHEMA)
    @Description("CALL apoc.sequence.drop(name) - remove the targeted sequence and return remaining sequences")
    public Stream<SequenceResult> drop(@Name("name") String name, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {

        SequenceConfig conf = new SequenceConfig(config);

        final AtomicLong removed = STORAGE.remove(name);
        isSequenceExistent(name, removed);

        withSystemDbTx(tx -> {
            Node node = getSequenceNode(tx, name);
            if (node != null) {
                node.delete();
            }
            return null;
        });

        if (conf.isDropConstraint()) {
            withSystemDbTx(tx -> {
                try {
                    tx.schema().getConstraintByName(SEQUENCE_CONSTRAINT_PREFIX + name).drop();
                } catch (IllegalArgumentException ignored) {}
                return null;
            });
        }
        return list();
    }

    @Procedure
    @Description("CALL apoc.sequence.list() - provide a list of sequences created")
    public Stream<SequenceResult> list() {

        return STORAGE.entrySet().stream().map(i -> new SequenceResult(i.getKey(), i.getValue().longValue()));
    }

    private void isSequenceExistent(@Name("name") String name, AtomicLong id) {
        if (id == null) {
            throw new RuntimeException(String.format("The sequence with name %s does not exist", name));
        }
    }

    private <T> T withSystemDbTx(Function<Transaction, T> action) {
        try (Transaction tx = apocConfig.getSystemDb().beginTx()) {
            T result = action.apply(tx);
            tx.commit();
            return result;
        }
    }
}