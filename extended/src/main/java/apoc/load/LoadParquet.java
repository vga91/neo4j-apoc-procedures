package apoc.load;

//import apoc.export.parquet.CustomTypes;
//import apoc.export.parquet.ParquetTypes;
import apoc.export.parquet.ParquetConfig;
import apoc.result.MapResult;
import apoc.util.Util;
//import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.export.parquet.ParquetReadUtil.mapFromRecord;
//import static apoc.export.parquet.ParquetReadUtil.genericDataLoad;
import static apoc.export.parquet.ParquetReadUtil.getReaderBuilder;

public class LoadParquet {

    @Context public Log log;


    private static class ParquetSpliterator extends Spliterators.AbstractSpliterator<MapResult> {

        private final ParquetReader<Group> reader;
        private final ParquetConfig conf;

        public ParquetSpliterator(ParquetReader reader, ParquetConfig conf){
            super(Long.MAX_VALUE, Spliterator.ORDERED);
            this.reader = reader;
            this.conf = conf;
        }

        @Override
        public synchronized boolean tryAdvance(Consumer<? super MapResult> action) {
            try {
                Group read = reader.read();
                if (read != null) {
                    MapResult result = new MapResult(mapFromRecord(read, conf));
                    action.accept(result);
                    return true;
                }

                return false;
            } catch (Exception e) {
                return false;
            }

        }
    }

    @Procedure(name = "apoc.load.parquet")
    @Description("Load parquet from the provided file or binary")
    public Stream<MapResult> load(
            @Name("input") Object input,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws IOException {

        ParquetConfig conf = new ParquetConfig(config);
        ParquetReader<Group> reader = getReaderBuilder(input)
//                .withDataModel(genericDataLoad)
//                .withConf(new Configuration())
                .build();

        registerCustomTypes();

        return StreamSupport.stream(new ParquetSpliterator(reader, conf), false)
                .onClose(() -> Util.close(reader));
    }

    public static void registerCustomTypes() {

//        for (ParquetTypes type: ParquetTypes.values()) {
//            CustomTypes.AbstractCustomType customType = type.getType();
//            LogicalTypes.register(customType.getLogicalTypeName(), schema -> customType);
//        }
    }

    public static class ParquetStream implements InputFile {
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

        public ParquetStream(byte[] stream) {
            this.data = stream;
        }

        @Override
        public long getLength() {
            return this.data.length;
        }

        @Override
        public SeekableInputStream newStream() {
            return new DelegatingSeekableInputStream(new SeekableByteArrayInputStream(this.data)) {
                @Override
                public void seek(long newPos) {
                    ((SeekableByteArrayInputStream) this.getStream()).setPos((int) newPos);
                }

                @Override
                public long getPos() {
                    return ((SeekableByteArrayInputStream) this.getStream()).getPos();
                }
            };
        }
    }



}
