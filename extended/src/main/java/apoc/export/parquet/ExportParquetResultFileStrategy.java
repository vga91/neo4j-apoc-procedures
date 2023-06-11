package apoc.export.parquet;

import apoc.Pools;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.logging.Log;
import org.neo4j.procedure.TerminationGuard;

import java.util.Iterator;
import java.util.stream.Stream;


public class ExportParquetResultFileStrategy extends ExportParquetFileStrategy<Result> /*implements ExportParquetResultStrategy*/ {
    public ExportParquetResultFileStrategy(String fileName, GraphDatabaseService db, Pools pools, TerminationGuard terminationGuard, Log logger) {
        super(fileName, db, pools, terminationGuard, logger);
    }

    @Override
    public String getSource(Result subGraph) {
        return null;
    }

//    public Iterator<Map<String, Object>> toIterator(ProgressReporter reporter, Result data) {
    @Override
    public Iterator<GenericRecord> toIterator(ProgressReporter reporter, Result data, Schema schema) {

        return null;
//        return data.stream()
//                .map(row -> {
//                    row.forEach((key, val) -> {
//                        final boolean notNodeNorRelationship = !(val instanceof Node) && !(val instanceof Relationship);
//                        reporter.update(val instanceof Node ? 1 : 0,
//                                val instanceof Relationship ? 1 : 0,
//                                notNodeNorRelationship ? 1 : 0);
//                        if (notNodeNorRelationship) {
//                            reporter.nextRow();
//                        }
//                    });
//                    return row;
//                })
//                .iterator();
    }

    @Override
    public Stream<ProgressInfo> export(Result data, ParquetConfig config) {
//        schemaFor(List.of(createConfigMap(data, config))); todo maybe nothing, serve lo schema??...
        return super.export(data, config);
    }

//    public ExportParquetResultFileStrategy(String fileName, GraphDatabaseService db, Pools pools, TerminationGuard terminationGuard, Log logger) {
//    }

//    public final String test() {
//        super.fileName;
//    }


}
