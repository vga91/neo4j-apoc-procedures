package org.neo4j.cypher.export;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;

import java.util.Iterator;
import java.util.Optional;

import static org.neo4j.internal.kernel.api.TokenRead.ANY_LABEL;

public class DatabaseSubGraph implements SubGraph
{
    private final Transaction transaction;
    private final KernelTransaction kernelTransaction;

    public DatabaseSubGraph( Transaction transaction )
    {
        this.transaction = transaction;
        this.kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
    }

    public static SubGraph from( Transaction transaction )
    {
        return new DatabaseSubGraph( transaction );
    }

    @Override
    public Iterable<Node> getNodes()
    {
        return transaction.getAllNodes();
    }

    @Override
    public Iterable<Relationship> getRelationships()
    {
        return transaction.getAllRelationships();
    }

    @Override
    public boolean contains( Relationship relationship )
    {
        return transaction.getRelationshipById( relationship.getId() ) != null;
    }

    @Override
    public Iterable<IndexDefinition> getIndexes()
    {
        return transaction.schema().getIndexes();
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints()
    {
        return transaction.schema().getConstraints();
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints(Label label) {
        return transaction.schema().getConstraints(label);
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints(RelationshipType type) {
        return transaction.schema().getConstraints(type);
    }

    @Override
    public Iterable<IndexDefinition> getIndexes(Label label) {
        return transaction.schema().getIndexes(label);
    }

    @Override
    public Iterable<RelationshipType> getAllRelationshipTypesInUse() {
        return transaction.getAllRelationshipTypesInUse();
    }

    @Override
    public Iterable<Label> getAllLabelsInUse() {
        return transaction.getAllLabelsInUse();
    }

    @Override
    public long countsForRelationship(Label start, RelationshipType type, Label end) {
        final TokenRead tokenRead = kernelTransaction.tokenRead();
        final int startId = getLabelId(start, tokenRead);
        final int relId = tokenRead.relationshipType(type.name());
        final int endId = getLabelId(end, tokenRead);
        
        return kernelTransaction.dataRead()
                .countsForRelationship(startId, relId, endId);
    }

    private Integer getLabelId(Label start, TokenRead tokenRead) {
        return Optional.ofNullable(start)
                .map(Label::name)
                .map(tokenRead::nodeLabel)
                .orElse(ANY_LABEL);
    }

    @Override
    public long countsForNode(Label label) {
        final int labelId = kernelTransaction.tokenRead().nodeLabel(label.name());
        return kernelTransaction.dataRead()
                .countsForNode(labelId);
    }

    @Override
    public Iterator<Node> findNodes(Label label) {
        return transaction.findNodes(label);
    }
}
