package apoc.export.parquet;

import apoc.Pools;
import apoc.export.util.ProgressReporter;
import apoc.result.ByteArrayResult;
import apoc.result.ProgressInfo;
import apoc.util.QueueBasedSpliterator;
import apoc.util.QueueUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.TerminationGuard;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.export.parquet.ExportParquetFileStrategy.getBuild;

public abstract class ExportParquetStreamStrategy<IN> implements ExportParquetStrategy<IN, Stream<ByteArrayResult>>  {


    // todo - these 4 are common with stream one
    private final GraphDatabaseService db;
    private final Pools pools;

    // todo!!! --> test..
    private final TerminationGuard terminationGuard;


    private final Log logger;

    public ExportParquetStreamStrategy(GraphDatabaseService db, Pools pools, TerminationGuard terminationGuard, Log logger) {
        this.db = db;
        this.pools = pools;
        this.terminationGuard = terminationGuard;
        this.logger = logger;
    }


    public Stream<ByteArrayResult> export(IN data, ParquetConfig config) {
        final ParquetExportType exportType = getType(data);
        Schema schema = exportType.schemaFor(db, config, data);
        final BlockingQueue<ByteArrayResult> queue = new ArrayBlockingQueue<>(100);

        try (ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
             BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(bytesOut)) {

            ParquetBufferedWriter out = new ParquetBufferedWriter(bufferedOutputStream);

            try(ParquetWriter<GenericRecord> writer = getBuild(schema, AvroParquetWriter.builder(out)) ) {

                exportType.writeFirstBatch(writer, schema);

                for (Iterator<GenericRecord> it = toIterator(data, schema); it.hasNext(); ) {
                    GenericRecord record = it.next();
                    // todo - try catch...
                    try {
                        writer.write(record);
//                        QueueUtil.put(queue, new ByteArrayResult(bytes), 10);
                    } catch (Exception e) {
                        // create something else - or another writer??
                        System.out.println("e = " + e);
                    }
                }
            }

            ByteArrayResult item = new ByteArrayResult(bytesOut.toByteArray());
            QueueUtil.put(queue, item, 10);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            QueueUtil.put(queue, ByteArrayResult.NULL, 10);
        }


        // todo - batch??
        QueueBasedSpliterator<ByteArrayResult> spliterator = new QueueBasedSpliterator<>(queue, ByteArrayResult.NULL, terminationGuard, Integer.MAX_VALUE);
        Stream<ByteArrayResult> stream = StreamSupport.stream(spliterator, false);
        return stream;
    }

    public abstract Iterator<GenericRecord> toIterator(IN data, Schema schema);

    private static class ParquetBufferedWriter implements OutputFile {

        public final BufferedOutputStream out;

        public ParquetBufferedWriter(BufferedOutputStream out) {
            this.out = out;
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) throws IOException {
            return createPositionOutputstream();
        }

        private PositionOutputStream createPositionOutputstream() {
            return new PositionOutputStream() {

                int pos = 0;

                @Override
                public long getPos() throws IOException {
                    return pos;
                }

                @Override
                public void flush() throws IOException {
                    out.flush();
                };

                @Override
                public void close() throws IOException {
                    out.close();
                };

                @Override
                public void write(int b) throws IOException {
                    out.write(b);
                    pos++;
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    out.write(b, off, len);
                    pos += len;
                }
            };
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
            return createPositionOutputstream();
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }
    }
}
