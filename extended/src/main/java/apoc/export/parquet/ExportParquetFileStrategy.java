package apoc.export.parquet;

import apoc.Pools;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
import apoc.util.QueueBasedSpliterator;
import apoc.util.QueueUtil;
import apoc.util.Util;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.TerminationGuard;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public abstract class ExportParquetFileStrategy<TYPE, IN> implements ExportParquetStrategy<IN, Stream<ProgressInfo>> {

    private final String fileName;
    private final GraphDatabaseService db;
    private final Pools pools;
    private final TerminationGuard terminationGuard;
    private final Log logger;
    private final ParquetExportType exportType;

    public ExportParquetFileStrategy(String fileName, GraphDatabaseService db, Pools pools, TerminationGuard terminationGuard, Log logger, ParquetExportType exportType) {
        this.fileName = fileName;
        this.db = db;
        this.pools = pools;
        this.terminationGuard = terminationGuard;
        this.logger = logger;
        this.exportType = exportType;
    }

    public Stream<ProgressInfo> export(IN data, ParquetConfig config) {

        ProgressInfo progressInfo = new ProgressInfo(fileName, getSource(data), "parquet");
        progressInfo.batchSize = config.getBatchSize();
        ProgressReporter reporter = new ProgressReporter(null, null, progressInfo);

        Path fileToWrite = new org.apache.hadoop.fs.Path(fileName);
        final BlockingQueue<ProgressInfo> queue = new ArrayBlockingQueue<>(10);

        Util.inTxFuture(pools.getDefaultExecutorService(), db, tx -> {
            int batchCount = 0;
            List<TYPE> rows = new ArrayList<>(config.getBatchSize());
            AvroParquetWriter.Builder<GenericRecord> builder = AvroParquetWriter
                    .builder(fileToWrite);

            try {
                Iterator<TYPE> it = toIterator(reporter, data);
                while (!Util.transactionIsTerminated(terminationGuard) && it.hasNext()) {
                    rows.add(it.next());

                    if (batchCount > 0 && batchCount % config.getBatchSize() == 0) {
                        writeBatch(builder, rows, data, config);
                    }
                    ++batchCount;
                }
                if (!rows.isEmpty()) {
                    writeBatch(builder, rows, data, config);
                }
                QueueUtil.put(queue, progressInfo, 10);
                return true;
            } catch (Exception e) {
                logger.error("Exception while extracting Parquet data:", e);
            } finally {
                reporter.done();
                QueueUtil.put(queue, ProgressInfo.EMPTY, 10);
            }
            return true;
        });

        QueueBasedSpliterator<ProgressInfo> spliterator = new QueueBasedSpliterator<>(queue, ProgressInfo.EMPTY, terminationGuard, Integer.MAX_VALUE);
        return StreamSupport.stream(spliterator, false);
    }

    private void writeBatch(AvroParquetWriter.Builder<GenericRecord> builder, List<TYPE> rows, IN data, ParquetConfig config) {

        List conf = exportType.createConfig(rows, data, config);
        Schema schema = exportType.schemaFor(db, conf);
        try (ParquetWriter<GenericRecord> writer = getBuild(schema, builder)) {
            writeRows(rows, writer, exportType, schema);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public abstract String getSource(IN data);

    public abstract Iterator<TYPE> toIterator(ProgressReporter reporter, IN data);
}
