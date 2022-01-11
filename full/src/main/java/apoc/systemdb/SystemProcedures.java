package apoc.systemdb;

import apoc.ApocConfig;
import apoc.Extended;
import apoc.result.NodeResult;
import apoc.util.Util;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.create.Create.setProperties;
import static org.neo4j.procedure.Mode.WRITE;

@Extended
public class SystemProcedures {
    @Context
    public ApocConfig apocConfig;

    @SystemProcedure
    @Description("apoc.systemdb.create.node(labels, props) - To create a node with specified labels and props directly in system database")
    @Procedure(name = "apoc.systemdb.create.node", mode = WRITE)
    public Stream<NodeResult> node(@Name("label") List<String> labelNames, @Name("props") Map<String, Object> props) {
        Node node;
        // open a transaction in order to create node and commit
        // we cannot use @Context public Transaction tx due to error: 'Creating new node label on database 'system' is not allowed for user ..'
        try (Transaction tx = apocConfig.getSystemDb().beginTx()) {
            node = setProperties(tx.createNode(Util.labels(labelNames)), props);
            tx.commit();
        }
        return openTxAndReturnStream(node);
    }

    @SystemProcedure
    @Description("apoc.systemdb.merge.node(labels, mergeKeys, props) - To merge a node directly in system database. If exists a node with specified labels and props (mergeKeys parameter) then add props to node, otherwise create a new node ")
    @Procedure(name = "apoc.systemdb.merge.node", mode = WRITE)
    public Stream<NodeResult> merge(@Name("label") List<String> labelNames, @Name("mergeKeys") Map<String, Object> mergeKeys, @Name("props") Map<String, Object> props) {
        
        Node node;
        final List<Label> collect = labelNames.stream().map(Label::label).collect(Collectors.toList());
        final Label[] labels = Util.labels(labelNames);
        try (Transaction tx = apocConfig.getSystemDb().beginTx()) {
            node = Iterators.stream(tx.findNodes(collect.get(0), mergeKeys))
                    .filter(n -> Iterables.asSet(n.getLabels()).containsAll(Arrays.asList(labels)))
                    .findAny()
                    .orElseGet(() -> {
                        final Node nodeCreated = tx.createNode(labels);
                        props.putAll(mergeKeys);
                        return setProperties(nodeCreated, props);
                    });
                    
            tx.commit();
        }

        return openTxAndReturnStream(node);
    }

    private Stream<NodeResult> openTxAndReturnStream(Node node) {
        // open a new transaction, we can't use try-with-resources otherwise tx gets closed too early
        Transaction tx = apocConfig.getSystemDb().beginTx();
        
        // we rebind node to prevent 'The transaction of entity N has been closed.' error
        node = Util.rebind(tx, node);

        return Stream.of(new NodeResult(node));
    }
}
