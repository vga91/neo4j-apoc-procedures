package apoc.export.parquet;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.BufferedOutputStream;

import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;


// todo - questo funziona...
public class ParquetReaderWriterWithAvro {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetReaderWriterWithAvro.class);

    private static final Schema SCHEMA;
    private static final String SCHEMA_LOCATION = "avroToParquet.avsc";
    private static final Path OUT_PATH = new Path("sampleOne.parquet");
    private static final Path OUT_PATH1 = new Path("sampleTwo.parquet");

    static {
        // todo - schema dynamic???, or with sample??? --> vedere Arrow

        try (InputStream inStream = ParquetReaderWriterWithAvro.class.getClassLoader().getResourceAsStream(SCHEMA_LOCATION)) {
            SCHEMA = new Schema.Parser().parse(IOUtils.toString(inStream, "UTF-8"));
        } catch (IOException e) {
            LOGGER.error("Can't read SCHEMA file from {}", SCHEMA_LOCATION);
            throw new RuntimeException("Can't read SCHEMA file from" + SCHEMA_LOCATION, e);
        }
    }

    public List<GenericData.Record> sampleData = new ArrayList<>();
    public List<GenericData.Record> sampleData1 = new ArrayList<>();

    public ParquetReaderWriterWithAvro(boolean test) throws IOException {
//        List<GenericData.Record> sampleData = new ArrayList<>();

        GenericData.Record record = new GenericData.Record(SCHEMA);
        record.put("c1", 1);
        record.put("c2", "someString");
        sampleData1.add(record);

        record = new GenericData.Record(SCHEMA);
        record.put("c1", 2);
        record.put("c2", "otherString");
        sampleData1.add(record);

//        ParquetReaderWriterWithAvro writerReader = new ParquetReaderWriterWithAvro();
        writeToParquet1(sampleData1, OUT_PATH1);
        System.out.println("ParquetReaderWriterWithAvro.ParquetReaderWriterWithAvro");
        readFromParquet(OUT_PATH1);
    }


    public ParquetReaderWriterWithAvro() throws IOException {
//        List<GenericData.Record> sampleData = new ArrayList<>();

        GenericData.Record record = new GenericData.Record(SCHEMA);
        record.put("c1", 1);
        record.put("c2", "someString");
        sampleData.add(record);

        record = new GenericData.Record(SCHEMA);
        record.put("c1", 2);
        record.put("c2", "otherString");
        sampleData.add(record);

//        ParquetReaderWriterWithAvro writerReader = new ParquetReaderWriterWithAvro();
        writeToParquet(sampleData, OUT_PATH);
        System.out.println("ParquetReaderWriterWithAvro.ParquetReaderWriterWithAvro");
        readFromParquet(OUT_PATH);
    }

//    public static void main(String[] args) throws IOException {
//        List<GenericData.Record> sampleData = new ArrayList<>();
//
//        GenericData.Record record = new GenericData.Record(SCHEMA);
//        record.put("c1", 1);
//        record.put("c2", "someString");
//        sampleData.add(record);
//
//        record = new GenericData.Record(SCHEMA);
//        record.put("c1", 2);
//        record.put("c2", "otherString");
//        sampleData.add(record);
//
//        ParquetReaderWriterWithAvro writerReader = new ParquetReaderWriterWithAvro();
//        writerReader.writeToParquet(sampleData, OUT_PATH);
//        writerReader.readFromParquet(OUT_PATH);
//    }

    public void readFromParquet(Path filePathToRead) throws IOException {

        // todo - InputFile...

        try (ParquetReader<GenericData.Record> reader = AvroParquetReader
                .<GenericData.Record>builder(filePathToRead)
                .withConf(new Configuration())
                .build()) {

            GenericData.Record record;
            while ((record = reader.read()) != null) {
                System.out.println(record);
            }
        }
    }

    // todo - https://www.knpcode.com/2022/06/how-to-read-write-parquet-file-hadoop.html
    /*
      public static void main(String[] args) {
        Schema schema = parseSchema();
        List<GenericData.Record> recordList = createRecords(schema);
        writeToParquetFile(recordList, schema);
      }
     */


    // todo - streaming mode??? --> https://stackoverflow.com/questions/40089689/parquet-writer-to-buffer-or-byte-stream

    public void writeToParquet(List<GenericData.Record> recordsToWrite, Path fileToWrite) throws IOException {
        // todoooo - remove this one
        new File(fileToWrite.getName()).delete();

        buildSchema();

        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter
                .<GenericData.Record>builder(fileToWrite)
                .withSchema(SCHEMA)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build()) {

            for (GenericData.Record record : recordsToWrite) {
                writer.write(record);
            }
        }
    }

    // todo - toField(..) like ExportArrowStrategy

    private Schema buildSchema() {

        Schema graphBuilder = SchemaBuilder
                .record("GraphBuilder") // todo - name record??? needed?
                // todo - namespace name??? needed??
                .namespace("org.apache.avro.ipc")
                .fields()
                .optionalLong("c1")

                .endRecord();


        return graphBuilder;


    }

    public void writeToParquet1(List<GenericData.Record> recordsToWrite, Path fileToWrite) throws IOException {
        // todoooo - remove this one
        new File(fileToWrite.getName()).delete();

//        buildSchema();

        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter
                .<GenericData.Record>builder(fileToWrite)
                .withSchema(buildSchema())
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build()) {

            for (GenericData.Record record : recordsToWrite) {
                writer.write(record);
            }
        }
    }

    // todo - from Stream<ByteArrayResult>
    public void streamRead(byte[] fileBytes) throws IOException {
        try (ParquetReader<GenericData.Record> reader = AvroParquetReader
                .<GenericData.Record>builder(new ParquetStream("dunno", fileBytes ) )
                .withConf(new Configuration())
                .build()) {

            GenericData.Record record;
            while ((record = reader.read()) != null) {
                System.out.println(record);
            }
        }
    }


    // todo - https://stackoverflow.com/questions/58141248/read-parquet-data-from-bytearrayoutputstream-instead-of-file
    // I'm successfully using this approach. For my own implementation, I also created a ByteArraySeekableInputStream class which stores the SeekableByteArrayInputStream delegate in a field, rather than declaring an anonymous inner class. This avoids the ((SeekableByteArrayInputStream) this.getStream()) cast. –
    //M. Justin
    // Sep 9, 2020 at 22:59
    public class ParquetStream implements InputFile {
        private final String streamId;
        private final byte[] data;

        private static class SeekableByteArrayInputStream extends ByteArrayInputStream {
            public SeekableByteArrayInputStream(byte[] buf) {
                super(buf);
            }

            public void setPos(int pos) {
                this.pos = pos;
            }

            public int getPos() {
                return this.pos;
            }
        }

//        public ParquetStream(String streamId, ByteArrayOutputStream stream) {
//
//        }
        public ParquetStream(String streamId, byte[] stream) {
            this.streamId = streamId;
            this.data = stream;//.toByteArray();
        }

        @Override
        public long getLength() throws IOException {
            return this.data.length;
        }

        @Override
        public SeekableInputStream newStream() throws IOException {
            return new DelegatingSeekableInputStream(new SeekableByteArrayInputStream(this.data)) {
                @Override
                public void seek(long newPos) throws IOException {
                    ((SeekableByteArrayInputStream) this.getStream()).setPos((int) newPos);
                }

                @Override
                public long getPos() throws IOException {
                    return ((SeekableByteArrayInputStream) this.getStream()).getPos();
                }
            };
        }

        @Override
        public String toString() {
            return "ParquetStream[" + streamId + "]";
        }
    }

    // todo - in Stream<ByteArrayResult>
    public byte[] stream(List<GenericData.Record> recordsToWrite) {
//        recordsToWrite.forEach(item -> item);

        // todo- compressionAlgo??? --> c'è un'alternativa nativa???


        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(bytesOut);
        ParquetBufferedWriter out = new ParquetBufferedWriter(bufferedOutputStream);

//        try (ParquetWriter<GenericData.Record> writer = new ParquetWriter()
//                /*<GenericData.Record>*/builder()
//                .withRowGroupSize(DEFAULT_BLOCK_SIZE)
//                .withPageSize(DEFAULT_PAGE_SIZE)
//                .withSchema(SCHEMA)
//                .build()) {
//
//            for (GenericData.Record record : recordsToWrite) {
//                writer.write(record);
//            }
//        } catch (IOException e) {
//            throw new IllegalStateException(e);
//        }

        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.
                <GenericData.Record>builder(out)
                .withRowGroupSize(DEFAULT_BLOCK_SIZE)
                .withPageSize(DEFAULT_PAGE_SIZE)
                .withSchema(SCHEMA)
                // todo -- config...
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)

                // todo --> difference from compressionAlgo ??? -->.withCompressionCodec()
                .build()) {

            for (GenericData.Record record : recordsToWrite) {
                writer.write(record);
            }

//            out.

            byte[] bytes = bytesOut.toByteArray();

//            writer.
//            System.out.println("new String(bytes) = " + new String(bytes));
//            return bytes;
//            return writer.
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        byte[] bytes = bytesOut.toByteArray();
//        byte[] bytes = bytesOut.toByteArray();
        System.out.println("new String(bytes) = " + new String(bytes));
        return bytes;

        // todo - close???



        // todo oooo --> QueueUtil.put(queue, new ByteArrayResult(bytes), 10);

//        if (data instanceof Result) {
//            return new ExportResultStreamStrategy(db, pools, terminationGuard, logger).export((Result) data, config);
//        } else {
//            return new ExportGraphStreamStrategy(db, pools, terminationGuard, logger).export((SubGraph) data, config);
//        }
    }

    class ParquetBufferedWriter implements OutputFile {

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
