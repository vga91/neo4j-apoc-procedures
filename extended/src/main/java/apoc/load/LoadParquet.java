package apoc.load;

import apoc.export.Hydrator;
import apoc.export.parquet.ApocParquetReader;

import apoc.export.parquet.ParquetConfig;
import apoc.result.MapResult;
import apoc.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
//import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.export.ImportParquet.getReader;
import static apoc.export.parquet.ParquetReadUtil.mapFromRecord;
//import static apoc.export.parquet.ParquetReadUtil.getReaderBuilder;
import static apoc.util.FileUtils.changeFileUrlIfImportDirectoryConstrained;

public class LoadParquet {

    @Context public Log log;


    private static class ParquetSpliterator extends Spliterators.AbstractSpliterator<MapResult> {

        private final ApocParquetReader reader;
        private final ParquetConfig conf;

        public ParquetSpliterator(ApocParquetReader reader, ParquetConfig conf){
            super(Long.MAX_VALUE, Spliterator.ORDERED);
            this.reader = reader;
            this.conf = conf;
        }

        @Override
        public synchronized boolean tryAdvance(Consumer<? super MapResult> action) {
            try {
                Object[] read = (Object[]) reader.getRecord();
                if (read != null) {
                    Map<String, Object> value = (Map<String, Object>) reader.getFinish(read);
                    MapResult result = new MapResult(value);
                    action.accept(result);
                    return true;
                }

                return false;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }



//    public static final class PropertiesHydrator<R> implements Hydrator<Object[], R> {
//
//        private final Map<String, Integer> index;
//        private final Function<Map<String, Object>, R> finisher;
//
//        public PropertiesHydrator(List<ColumnDescriptor> columns, Function<Map<String, Object>, R> finisher) {
//            this.index = new HashMap<>(columns.size());
//            this.finisher = finisher;
//            int idx = 0;
//            for (ColumnDescriptor d : columns) {
//                this.index.put(d.getPath()[d.getPath().length - 1], idx++);
//            }
//        }
//
//        @Override
//        public Object[] start() {
//            return new Object[index.size()];
//        }
//
//        @Override
//        public Object[] add(Object[] target, String heading, Object value) {
//            if (index.get(heading) == null) {
//                return target;
//            }
//            target[index.get(heading)] = value;
//            return target;
//        }
//
//        @Override
//        public R finish(Object[] target) {
//            R collect = this.index.entrySet().stream()
//                    .filter(e -> target[e.getValue()] != null)
//                    .collect(Collectors.collectingAndThen(Collectors.toMap(Map.Entry::getKey, e -> target[e.getValue()]), finisher));
//            return collect;
//        }
//    }

    public static final class PropertiesHydrator2 implements Hydrator<Object[], Map<String, Object>> {

        private final Map<String, Integer> index;
//        private final Function<Map<String, Object>, Object> finisher;

        public PropertiesHydrator2(List<ColumnDescriptor> columns/*, Function<Map<String, Object>, Object> finisher*/) {
            this.index = new HashMap<>(columns.size());
//            this.finisher = finisher;
            int idx = 0;
            for (ColumnDescriptor d : columns) {
                this.index.put(d.getPath()[d.getPath().length - 1], idx++);
            }
        }

        @Override
        public Object[] start() {
            return new Object[index.size()];
        }

        @Override
        public Object[] add(Object[] target, String heading, Object value) {
            if (index.get(heading) == null) {
                return target;
            }
            try {
                target[index.get(heading)] = value;
//                System.out.println("target = " + target);
            } catch (Exception e) {
                return target;
//                System.out.println("e = " + e);
            }
            return target;
        }

        @Override
        public Map<String, Object> finish(Object[] target) {
            Map<String, Object> collect = this.index.entrySet().stream()
                    .filter(e -> {
                        try {
                            return target[e.getValue()] != null;
                        } catch (Exception ex) {
                            return false;
                        }
                    })
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> target[e.getValue()]));
//                    .collect(Collectors.collectingAndThen(Collectors.toMap(Map.Entry::getKey, e -> target[e.getValue()]), finisher));
            return collect;
        }
    }

    @Procedure(name = "apoc.load.parquet")
    @Description("Load parquet from the provided file or binary")
    public Stream<MapResult> load(
            @Name("input") Object input,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws IOException {

        ParquetConfig conf = new ParquetConfig(config);
//        String fileName = changeFileUrlIfImportDirectoryConstrained((String) input);

        var uri = URI.create((String)input);
        var scheme = uri.getScheme();
        File inputFile;
        if (scheme == null || "file".equalsIgnoreCase(scheme)) {
            inputFile = uri.isAbsolute() ? new File(uri) : new File(uri.getPath());
        } else {
            // Probably there's a better library than the one I picked that can deal with Parquet on remote hosts
            // Making that nice is not part of this PoC
            inputFile = File.createTempFile("neo4j-", ".parquet");
            try (
                    var in = uri.toURL().openStream();
                    var out = new BufferedOutputStream(new FileOutputStream(inputFile))) {
                in.transferTo(out);
            }
        }

//        return ApocParquetReader.streamContent(inputFile, listOfColumns -> new PropertiesHydrator<>(listOfColumns, MapResult::new));

//        ParquetFileReader reader = ParquetFileReader.open(new Configuration(), new Path(fileName));
//        return reader;
//                .build();

        ApocParquetReader reader = getReader((String) input, conf);
        return StreamSupport.stream(new ParquetSpliterator(reader, conf),false)
                .onClose(() -> Util.close(reader));
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
