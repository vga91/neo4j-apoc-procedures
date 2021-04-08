package apoc.export.arrow;

import apoc.Pools;
import apoc.export.cypher.ExportCypher;
import apoc.result.ByteArrayResult;
import apoc.util.QueueBasedSpliterator;
import apoc.util.Util;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.procedure.TerminationGuard;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ExportArrowService {

    // TODO - costruttore
    private final GraphDatabaseService db = null;
    private final Pools pools = null;
    private final TerminationGuard terminationGuard = null;

    public Stream<ByteArrayResult> stream(Object data, ArrowConfig config) {
//        VectorSchemaRoot root = VectorSchemaRoot.create(schema, new RootAllocator(Integer.MAX_VALUE));


        return Stream.empty();
    }

    private byte[] batchToByteArray(ArrowRecordBatch batch, Schema schema, BufferAllocator allocator) {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowFileWriter writer = new ArrowFileWriter(root, null, Channels.newChannel(out));

        try {
            VectorLoader loader = new VectorLoader(root);
            loader.load(batch);
            writer.writeBatch();
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            batch.close();
            root.close();
            writer.close();
        }
    }

    private Stream<ByteArrayResult> streamGraph(SubGraph graph, ArrowConfig config) {
        // creo Schema,
        // itero nodi e relazioni
        return Stream.empty();
    }

    private Stream<ByteArrayResult> streamResult(Result result, ArrowConfig config) {
        // creo Schema,
        // vedo lo schema dal primo batch
//        result.
        // itero result
        final BlockingQueue<ByteArrayResult> queue = new ArrayBlockingQueue<>(1000);

        Util.inTxFuture(pools.getDefaultExecutorService(), db, txInThread -> {
            ArrowRecordBatch batch = new ArrowRecordBatch(0, null, null);
            while (result.hasNext()) {
                // counter batchSize

            }
            return true;
        });
        QueueBasedSpliterator<ByteArrayResult> spliterator = new QueueBasedSpliterator<>(queue, ByteArrayResult.NULL, terminationGuard, Integer.MAX_VALUE);
        return StreamSupport.stream(spliterator, false);
    }

    private Schema createSchemaFromGraph(SubGraph subGraph, ArrowConfig config) {
        // CALL apoc.meta.nodeTypeProperties({ includeLabels: ['IncludeMe'] })
        final Iterable<Label> labels = subGraph.getAllLabelsInUse();
        db.executeTransactionally("CALL apoc.meta.nodeTypeProperties({ includeLabels: $labels })",
                Map.of("labels", labels), result -> {
                    // nodeLabels --> join(":") Map<nodeLabels.join(“:”), Map<propertyName, propertyTypes>>
                    return null;

                });
        final Iterable<RelationshipType> allRelationshipTypesInUse = subGraph.getAllRelationshipTypesInUse();
        db.executeTransactionally("CALL apoc.meta.relTypeProperties({ includeRels: $labels })",
                Map.of("labels", labels), result -> {
                    // Map<relType, Map<propertyName, propertyTypes>>
                    return null;
                });

        return new Schema(null);
    }

    private Schema createSchemaFromResult(Result result, ArrowConfig config) {
        return new Schema(null);
    }

    private FieldVector toFieldVector(List<String> propertyTypes) {
        if (propertyTypes.size() > 1) {
            // return string type
            return null;
        } else {
            // convert to RelatedType
            return null;
        }
    }
}
