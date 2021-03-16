package apoc.sequence;

import apoc.ApocConfig;
import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.procedure.Name;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;

import static apoc.ApocConfig.apocConfig;
import static apoc.sequence.Sequence.SEQUENCE_CONSTRAINT_PREFIX;

public class SequenceHandler extends LifecycleAdapter implements AvailabilityListener {

    private final ApocConfig apocConfig;

    public static final String SEQUENCES_REFRESH = "apoc.sequences.refresh";

    private static final ConcurrentMap<String, AtomicLong> STORAGE = new ConcurrentHashMap<>();
    private static Group REFRESH_GROUP = Group.STORAGE_MAINTENANCE;
    private final JobScheduler jobScheduler;
    private long lastUpdate;
    private JobHandle restoreProceduresHandle;
    private final GraphDatabaseAPI api;

    public SequenceHandler(ApocConfig apocConfig, JobScheduler jobScheduler, GraphDatabaseAPI db) {
        this.apocConfig = apocConfig; // todo - fare direttamente  this.systemDb = apocConfig.getSystemDb();
        this.jobScheduler = jobScheduler;
        this.api = db;
    }

//    @Override
//    public void start() {
//        System.out.println("SequenceHandler.start");
//////        refreshSequences();
////        refreshSequences();
////        long refreshInterval = apocConfig().getInt(SEQUENCES_REFRESH, 60000);
////        System.out.println("apocConfig().getInt(SEQUENCES_REFRESH, 60000);");
////        System.out.println(apocConfig().getInt(SEQUENCES_REFRESH, 60000));
////        restoreProceduresHandle = jobScheduler.scheduleRecurring(REFRESH_GROUP, () -> {
////            if (getLastUpdate() > lastUpdate) {
////                refreshSequences();
////            }
////        }, refreshInterval, refreshInterval, TimeUnit.MILLISECONDS);
//    }

    private void refreshSequences() {

        lastUpdate = System.currentTimeMillis();// todo..... nel caso faccio System.currentTimeMillis() > lastUpdate

        withSystemDbTx(tx-> {

            tx.findNodes(SystemLabels.Sequence).forEachRemaining(node -> {

                tx.acquireWriteLock(node);
                final String name = (String) node.getProperty(SystemPropertyKeys.name.name());
                final long value = (long) node.getProperty(SystemPropertyKeys.value.name());

                STORAGE.compute(name, (s, atomicLong) -> {
                    if (atomicLong == null) {
                        return new AtomicLong(value);
                    } else {
                        long actual = atomicLong.get();
                        if (actual < value) {
                            atomicLong.compareAndSet(actual, value);
                        }
                        if (actual > value) {
                            node.setProperty(SystemPropertyKeys.value.name(), actual);
                        }
                        return atomicLong;
                    }
                });
            });
            return null;
        });
    }

//    @Override
//    public void stop() {
//        System.out.println("SequenceHandler.stop");
////        withSystemDbTx(tx-> {
////            tx.findNodes(SystemLabels.Sequence).forEachRemaining(node -> {
////                final String name = (String) node.getProperty(SystemPropertyKeys.name.name());
////                final long value = (long) node.getProperty(SystemPropertyKeys.value.name());
////                final long actual = STORAGE.get(name).get();
////                if (actual > value) {
////                    node.setProperty(SystemPropertyKeys.value.name(), actual);
////                }
////            });
////            STORAGE.clear();
////            return null;
////        });
//    }

    public long nextValue(String name) {
        return STORAGE.compute(name, (s, atomicLong) -> {
            if (atomicLong == null) {
                sequenceNotFound(name);
            }
            return atomicLong;
        }).incrementAndGet();
    }

    public long currentValue(String name) {
        return STORAGE.compute(name, (s, atomicLong) -> {
            if (atomicLong == null) {
                sequenceNotFound(name);
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
                throw new RuntimeException(String.format("The sequence with name %s exists", name));
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
            sequenceNotFound(name);
        }
    }

    private void sequenceNotFound(@Name("name") String name) {
        throw new RuntimeException(String.format("The sequence with name %s does not exist", name));
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

    @Override
    public void available() {
        System.out.println("SequenceHandler.available");
//        withSystemDbTx(tx-> {
//            tx.findNodes(SystemLabels.Sequence).forEachRemaining(item -> {
//
//                final String name = (String) item.getProperty(SystemPropertyKeys.name.name());
//                final long value = (long) item.getProperty(SystemPropertyKeys.value.name());
//                STORAGE.compute(name, (s, atomicLong) -> {
//                    if (atomicLong == null) {
//                        return new AtomicLong(value);
//                    } else {
//                        long actual = atomicLong.get();
//                        if (actual < value) {
//                            atomicLong.compareAndSet(actual, value);
//                        }
//                        return atomicLong;
//                    }
//                });
//            });
//            return null;
//        });

        refreshSequences();
        long refreshInterval = apocConfig().getInt(SEQUENCES_REFRESH, 60000);
        System.out.println("apocConfig().getInt(SEQUENCES_REFRESH, 60000);");
        System.out.println(apocConfig().getInt(SEQUENCES_REFRESH, 60000));
        restoreProceduresHandle = jobScheduler.scheduleRecurring(REFRESH_GROUP, () -> {
//            if (getLastUpdate() > lastUpdate) {
                refreshSequences();
//            }
        }, refreshInterval, refreshInterval, TimeUnit.MILLISECONDS);
    }

    @Override
    public void unavailable() {
        System.out.println("SequenceHandler.unavailable");
        if (restoreProceduresHandle != null) {
            restoreProceduresHandle.cancel();
        }

        withSystemDbTx(tx-> {
            tx.findNodes(SystemLabels.Sequence).forEachRemaining(node -> {
                tx.acquireWriteLock(node);
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
}
