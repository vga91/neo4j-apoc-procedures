package apoc.sequence;

import apoc.ApocConfig;
import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.util.Util;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.WRITE;

public class Sequence {

    @Context
    public ApocConfig apocConfig;

    public static class SequenceResult {
        public final String name;
        public final Long value;

        public SequenceResult(String name, Long value) {
            this.name = name;
            this.value = value;
        }
    }

    @Procedure(mode = WRITE)
    @Description("CALL apoc.sequence.create(name, initialValue [default=0]) - create a sequence, save it in a custom node in the system db and return it")
    public Stream<SequenceResult> create(@Name("name") String name, @Name(value = "initialValue", defaultValue = "0") Long initialValue) {

        return withSystemDbTx(tx -> {
            Node node = Util.mergeNodeWithOptAdditionalLabel(tx,
                    SystemLabels.Sequence,
                    null,
                    true,
                    Pair.of(SystemPropertyKeys.name.name(), name));
            node.setProperty(SystemPropertyKeys.value.name(), initialValue);
            return Stream.of(new SequenceResult(name, initialValue));
        });
    }

    @Procedure
    @Description("CALL apoc.sequence.currentValue(name) - return the targeted sequence")
    public Stream<SequenceResult> currentValue(@Name("name") String name) {

        return withSystemDbTx(tx -> Stream.of(
                new SequenceResult(name, returnSequenceValueByName(tx, name))
        ));
    }

    @Procedure(mode = WRITE)
    @Description("CALL apoc.sequence.nextValue(name) - increment the targeted sequence by one and return it")
    public Stream<SequenceResult> nextValue(@Name("name") String name) {

        return withSystemDbTx(tx -> {
            Node node = getSequenceNode(tx, name);
            Long valueIncremented = blockNodeAndReturnValue(tx, node) + 1;
            node.setProperty(SystemPropertyKeys.value.name(), valueIncremented);
            return Stream.of(new SequenceResult(name, valueIncremented));
        });
    }

    @Procedure(mode = WRITE)
    @Description("CALL apoc.sequence.drop(name) - remove the targeted sequence and return remaining sequences")
    public Stream<SequenceResult> drop(@Name("name") String name) {

        withSystemDbTx(tx -> {
            Node node = getSequenceNode(tx, name);
            node.delete();
            return null;
        });

        return list();
    }

    @Procedure
    @Description("CALL apoc.sequence.list() - provide a list of sequences created")
    public Stream<SequenceResult> list() {

        List<SequenceResult> nodes = withSystemDbTx(tx -> tx.findNodes(SystemLabels.Sequence)
                .stream()
                .map(node -> new SequenceResult(
                        node.getProperty(SystemPropertyKeys.name.name()).toString(),
                        getValueNode(node)
                )).collect(Collectors.toList())
        );

        return nodes.stream();
    }


    private long returnSequenceValueByName(Transaction tx, String name) {
        Node node = getSequenceNode(tx, name);
        return blockNodeAndReturnValue(tx, node);
    }

    private Long blockNodeAndReturnValue(Transaction tx, Node node) {
        tx.acquireWriteLock(node);
        return getValueNode(node);
    }

    private Long getValueNode(Node node) {
        return Util.toLong(node.getProperty(SystemPropertyKeys.value.name()));
    }

    private Node getSequenceNode(Transaction tx, String name) {
        Node node = tx.findNode(SystemLabels.Sequence, SystemPropertyKeys.name.name(), name);
        if (node == null) {
            throw new RuntimeException(String.format("The sequence with name %s does not exist", name));
        }
        return node;
    }

    private <T> T withSystemDbTx(Function<Transaction, T> action) {
        try (Transaction tx = apocConfig.getSystemDb().beginTx()) {
            T result = action.apply(tx);
            tx.commit();
            return result;
        }
    }
}