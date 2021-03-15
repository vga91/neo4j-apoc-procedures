package apoc.sequence;

import apoc.ApocConfig;
import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.util.Util;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.procedure.Name;
import org.neo4j.scheduler.JobHandle;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.sequence.Sequence.SEQUENCE_CONSTRAINT_PREFIX;

public class SequenceHandler extends LifecycleAdapter {

    private final ApocConfig apocConfig;

    private static final ConcurrentMap<String, AtomicLong> STORAGE = new ConcurrentHashMap<>();

    public SequenceHandler(ApocConfig apocConfig) {
        this.apocConfig = apocConfig;
    }

    private JobHandle restoreSequencencesHandle;

    @Override
    public void start() {
        refreshSequences();
    }

    private void refreshSequences() {
        withSystemDbTx(tx-> {
            tx.findNodes(SystemLabels.Sequence).forEachRemaining(item -> {

                final String name = (String) item.getProperty(SystemPropertyKeys.name.name());
                final long value = (long) item.getProperty(SystemPropertyKeys.value.name());
                STORAGE.compute(name, (s, atomicLong) -> {
                    if (atomicLong == null) {
                        return new AtomicLong(value);
                    } else {
                        long actual = atomicLong.get();
                        if (actual < value) {
                            atomicLong.compareAndSet(actual, value);
                        }
                        return atomicLong;
                    }
                });
            });
            return null;
        });
    }

    @Override
    public void stop() {
        withSystemDbTx(tx-> {
            tx.findNodes(SystemLabels.Sequence).forEachRemaining(node -> {
                final String name = (String) node.getProperty(SystemPropertyKeys.name.name());
                final long value = (long) node.getProperty(SystemPropertyKeys.value.name());
                final long actual = STORAGE.get(name).get();
                if (actual > value) {
                    node.setProperty(SystemPropertyKeys.value.name(), actual);
                }
            });
            STORAGE.clear();
            return null;
        });
    }

    public long nextValue(String name) {
        return STORAGE.compute(name, (s, atomicLong) -> {
            if (atomicLong == null) {
                throw new RuntimeException("sequence not found");
            }
            return atomicLong;
        }).incrementAndGet();
    }

    public long currentValue(String name) {
        return STORAGE.compute(name, (s, atomicLong) -> {
            if (atomicLong == null) {
                throw  new RuntimeException("sequence...");
            }
            return atomicLong;
        }).get();
    }

    private <T> T withSystemDbTx(Function<Transaction, T> action) {
        try (Transaction tx = apocConfig.getSystemDb().beginTx()) {
            T result = action.apply(tx);
            tx.commit();
            return result;
        }
    }

    private Node getSequenceNode(Transaction tx, String name) {
        return tx.findNode(SystemLabels.Sequence, SystemPropertyKeys.name.name(), name);
    }

    public Stream<Sequence.SequenceResult> create(String name, SequenceConfig conf) {

        Stream<Sequence.SequenceResult> resultStream = withSystemDbTx(tx -> {
//            List<Pair<String, Object>> list = new ArrayList<>();
//            list.add(Pair.of(SystemPropertyKeys.name.name(), name));

            boolean existsNode = tx.findNode(SystemLabels.Sequence, SystemPropertyKeys.name.name(), name) != null;

            if (existsNode) {
                throw new RuntimeException("sequence exists");
            }

            Node node = tx.createNode(SystemLabels.Sequence);
            node.setProperty(SystemPropertyKeys.name.name(), name);
            if (conf.isCreateConstraint()) {
                node.setProperty(SystemPropertyKeys.constraintPropertyName.name(), conf.getConstraintPropertyName());
            }


            long init = conf.getInitialValue();
            node.setProperty(SystemPropertyKeys.value.name(), init);
            STORAGE.put(name, new AtomicLong(init));
            return Stream.of(new Sequence.SequenceResult(name, init));
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

    public void drop(String name, SequenceConfig conf) {
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
    }


    // leader....

    private void isSequenceExistent(@Name("name") String name, AtomicLong id) {
        if (id == null) {
            throw new RuntimeException(String.format("The sequence with name %s does not exist", name));
        }
    }

    public Stream<Sequence.SequenceResult> list() {
//        return withSystemDbTx(tx -> {
//            return tx.findNodes(SystemLabels.Sequence).stream().map(item -> {
//                final String name = (String) item.getProperty(SystemPropertyKeys.name.name());
//                final long value = (long) item.getProperty(SystemPropertyKeys.value.name());
//                return new Sequence.SequenceResult(name, value);
//            });
//            return null;
//        });
        return STORAGE.entrySet().stream().map(i -> new Sequence.SequenceResult(i.getKey(), i.getValue().longValue()));
    }

}
