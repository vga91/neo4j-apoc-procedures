package apoc.sequence;

import apoc.ApocConfig;
import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static apoc.sequence.Sequence.getSequenceNode;

public class SequenceHandler extends LifecycleAdapter {

    private final ApocConfig apocConfig;

    public static final ConcurrentMap<String, AtomicLong> STORAGE = new ConcurrentHashMap<>();

    public SequenceHandler(ApocConfig apocConfig) {
        this.apocConfig = apocConfig;
    }

    @Override
    public void start() {
        withSystemDbTx(tx-> {
            tx.findNodes(SystemLabels.Sequence).forEachRemaining(item -> STORAGE.put(
                    (String) item.getProperty(SystemPropertyKeys.name.name()),
                    new AtomicLong((long) item.getProperty(SystemPropertyKeys.value.name()))
            ));
            STORAGE.forEach((key, value) -> {
                Node node = getSequenceNode(tx, key);
                node.setProperty(SystemPropertyKeys.value.name(), value.get());
            });
            return null;
        });
    }

    @Override
    public void stop() {
        withSystemDbTx(tx-> {
            STORAGE.forEach((key, value) -> {
                Node node = getSequenceNode(tx, key);
                node.setProperty(SystemPropertyKeys.value.name(), value.get());
            });
            return null;
        });
    }

    private void withSystemDbTx(Function<Transaction, Object> action) {
        try (Transaction tx = apocConfig.getSystemDb().beginTx()) {
            action.apply(tx);
            tx.commit();
        }
    }
}
