package apoc.export.parquet;

import apoc.export.Hydrator;
import apoc.export.HydratorSupplier;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.DummyRecordConverter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.export.parquet.ParquetReadUtil.getValue;
import static apoc.export.parquet.ParquetReadUtil.toTimeUnitJava;
import static apoc.export.parquet.ParquetReadUtil.toValidValue;
import static apoc.export.parquet.ParquetReadUtil.toValidValue1;

//import static apoc.export.parquet.ParquetReadUtil.toValidValue;

public final class ApocParquetReader implements Closeable {
    private final ParquetFileReader reader;
    private final Hydrator<Object[], Map<String, Integer>> hydrator;
    private final List<ColumnDescriptor> columns;
    private final MessageType schema;
    private final GroupRecordConverter recordConverter;
    private final String createdBy;
    private final Map<String, Integer> index;

    private boolean finished;
    private long currentRowGroupSize = -1L;
    private List<ColumnReader> currentRowGroupColumnReaders;
    private long currentRowIndex = -1L;
    private ParquetConfig config;
//
//    public static <U, S> Stream<S> streamContent(File file, HydratorSupplier<U, S> hydrator) throws IOException {
//        return streamContent(file, hydrator, null);
//    }
//
//    public static <U, S> Stream<S> streamContent(File file, HydratorSupplier<U, S> hydrator, Collection<String> columns) throws IOException {
//        return streamContent(makeInputFile(file), hydrator, columns);
//    }
//
//    public static <U, S> Stream<S> streamContent(InputFile file, HydratorSupplier<U, S> hydrator) throws IOException {
//        return streamContent(file, hydrator, null);
//    }
//
//    public static <U, S> Stream<S> streamContent(InputFile file, HydratorSupplier<U, S> hydrator, Collection<String> columns) throws IOException {
//        return stream(spliterator(file, hydrator, columns));
//    }
//
//    public static <U, S> ApocParquetReader<U, S> spliterator(File file, HydratorSupplier<U, S> hydrator) throws IOException {
//        return spliterator(file, hydrator, null);
//    }
//
//    public static <U, S> ApocParquetReader<U, S> spliterator(File file, HydratorSupplier<U, S> hydrator, Collection<String> columns) throws IOException {
//        return spliterator(makeInputFile(file), hydrator, columns);
//    }
//
//    public static <U, S> ApocParquetReader<U, S> spliterator(InputFile file, HydratorSupplier<U, S> hydrator) throws IOException {
//        return spliterator(file, hydrator, null);
//    }
//
//    public static <U, S> ApocParquetReader<U, S> spliterator(InputFile file, HydratorSupplier<U, S> hydrator, Collection<String> columns) throws IOException {
//        Set<String> columnSet = (null == columns) ? Collections.emptySet() : Set.copyOf(columns);
//        return new ApocParquetReader<>(file, columnSet, hydrator, null);
//    }
//
//    public static <U, S> Stream<S> stream(ApocParquetReader<U, S> reader) {
//        return StreamSupport
//                .stream(reader, false)
//                .onClose(() -> closeSilently(reader));
//    }
//
//    public static Stream<String[]> streamContentToStrings(File file) throws IOException {
//        return stream(spliterator(makeInputFile(file), columns -> {
//            final AtomicInteger pos = new AtomicInteger(0);
//            return new Hydrator<String[], String[]>() {
//                @Override
//                public String[] start() {
//                    return new String[columns.size()];
//                }
//
//                @Override
//                public String[] add(String[] target, String heading, Object value) {
//                    target[pos.getAndIncrement()] = heading + "=" + value.toString();
//                    return target;
//                }
//
//                @Override
//                public String[] finish(String[] target) {
//                    return target;
//                }
//            };
//        }, null));
//    }

    public static ParquetMetadata readMetadata(File file) throws IOException {
        return readMetadata(makeInputFile(file));
    }

    public static ParquetMetadata readMetadata(InputFile file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(file)) {
            return reader.getFooter();
        }
    }

//    private ApocParquetReader(InputFile file, Set<String> columnNames, HydratorSupplier<U, S> hydratorSupplier) throws IOException {
//        this.reader = ParquetFileReader.open(file);
//        FileMetaData meta = reader.getFooter().getFileMetaData();
//        this.schema = meta.getSchema();
//        this.recordConverter = new DummyRecordConverter(this.schema).getRootConverter();
//        this.createdBy = meta.getCreatedBy();
//
//        this.columns = schema.getColumns().stream()
//                .filter(c -> columnNames.isEmpty() || columnNames.contains(c.getPath()[0]))
//                .collect(Collectors.toList());
//
//        this.hydrator = hydratorSupplier.get(this.columns);
//    }

    public ApocParquetReader(InputFile file, Set<String> columnNames, ParquetConfig config) throws IOException {
        this.reader = ParquetFileReader.open(file);
        FileMetaData meta = reader.getFooter().getFileMetaData();
        this.schema = meta.getSchema();
        this.recordConverter = new GroupRecordConverter(this.schema);//.getRootConverter();
        this.createdBy = meta.getCreatedBy();

        this.columns = schema.getColumns().stream()
                .filter(c -> columnNames.isEmpty() || columnNames.contains(c.getPath()[0]))
                .collect(Collectors.toList());

        this.hydrator = null;// hydratorSupplier.get(this.columns);

        this.index = new HashMap<>(columns.size());
        int idx = 0;
        for (ColumnDescriptor d : columns) {
            this.index.put(d.getPath()[0], idx++);
//            this.index.put(d.getPath()[d.getPath().length - 1], idx++);
        }
        this.config = config;
    }

    private Object readValue(ColumnReader columnReader) {
//        this.recordConverter.getCurrentRecord()
        ColumnDescriptor column = columnReader.getDescriptor();
        PrimitiveType primitiveType = column.getPrimitiveType();
        int maxDefinitionLevel = column.getMaxDefinitionLevel();

        if (columnReader.getCurrentDefinitionLevel() == maxDefinitionLevel) {
            switch (primitiveType.getPrimitiveTypeName()) {
                case BINARY:
                case FIXED_LEN_BYTE_ARRAY:
                case INT96:
                    return columnReader.getBinary().toStringUsingUTF8();
                case BOOLEAN:
                    return columnReader.getBoolean();
                case DOUBLE:
                    return columnReader.getDouble();
                case FLOAT:
                    return columnReader.getFloat();
                case INT32:
                    return columnReader.getInteger();
                case INT64:
                    long recordLong = columnReader.getLong();
                    LogicalTypeAnnotation logicalTypeAnnotation = primitiveType.getLogicalTypeAnnotation();
                    if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
                        LogicalTypeAnnotation.TimestampLogicalTypeAnnotation logicalTypeAnnotation1 = (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalTypeAnnotation;
                        if (logicalTypeAnnotation1.isAdjustedToUTC()) {
                            return Instant.EPOCH.plus(recordLong, toTimeUnitJava(logicalTypeAnnotation1.getUnit()).toChronoUnit());
                        } else {
                            return LocalDateTime.ofInstant(Instant.EPOCH.plus(recordLong, toTimeUnitJava(logicalTypeAnnotation1.getUnit()).toChronoUnit()), ZoneId.of("UTC"));//  logicalTypeAnnotation1.getUnit()
                        }
                    }
                    return recordLong;
//                    return recordLong;
                default:
                    throw new IllegalArgumentException("Unsupported type: " + primitiveType);
            }
        } else {
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

//    @Override
//    public boolean tryAdvance(Consumer<? super S> action) {
//        try {
//            if (this.finished) {
//                return false;
//            }
//
//            U record = getRecord();
//            if (record == null) return false;
//
//            action.accept(getFinish(record));
//
//
//            return true;
//        } catch (Exception e) {
//            throw new RuntimeException("Failed to read parquet", e);
//        }
//    }

//    public S getFinish(U record) {
//        return hydrator.finish(record);
//    }
//    public S getFinish(U record) {
//        return hydrator.finish(record);
//    }
    public Map<String, Object> getFinish(Object[] target) {
        this.currentRowIndex++;

        return this.index.entrySet().stream()
                .filter(e -> {
                    try {
                        return target[e.getValue()] != null;
                    } catch (Exception ex) {
                        return false;
                    }
                })
                .collect(Collectors.toMap(Map.Entry::getKey, e -> toValidValue(target[e.getValue()], e.getKey(), config)));
    }

    private RecordReader recordReader = null;

    public Object[] getRecord() throws IOException {
        if (currentRowIndex == currentRowGroupSize) {
//            PageReadStore pages;
//            while ((pages = reader.readNextRowGroup()) != null) {
//                long rows = pages.getRowCount();
//                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
//                RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
//
//                for (int i = 0; i < rows; i++) {
//                    SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
//                    System.out.println("simpleGroup = " + simpleGroup);
////                    simpleGroup.add(simpleGroup);
//                }
//            }

            PageReadStore rowGroup = reader.readNextRowGroup();
            if (rowGroup == null) {
                this.finished = true;
                return null;
            }

//            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
//            this.recordReader = columnIO.getRecordReader(rowGroup, new GroupRecordConverter(schema));
            ColumnReadStore columnReadStore = new ColumnReadStoreImpl(rowGroup, this.recordConverter.getRootConverter(), this.schema, this.createdBy);

            this.currentRowGroupSize = rowGroup.getRowCount();
            this.currentRowGroupColumnReaders = columns.stream().map(columnReadStore::getColumnReader).collect(Collectors.toList());
            this.currentRowIndex = 0L;
        }

        Object[] record = new Object[index.size()];

//        SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
//        record = getU(record, simpleGroup);
        record = getU2(record);


        return record;
    }

    public Object[] getU(Object[] record, SimpleGroup simpleGroup/*SimpleGroup simpleGroup*/) {
        for (Type type: simpleGroup.getType().getFields()) {
            record = getAdd(record, type, simpleGroup);
//            columnReader.consume();
//            if (columnReader.getCurrentRepetitionLevel() != 0) {
//                throw new IllegalStateException("Unexpected repetition");
//            }
        }
        return record;
    }


    public Object[] getU2(Object[] record) {
        for (ColumnReader columnReader: this.currentRowGroupColumnReaders) {
            do {
                record = getAdd(record, columnReader);
                columnReader.consume();
            } while (columnReader.getCurrentRepetitionLevel() != 0);
//            while (columnReader.getTotalValueCount()) {
//                record = getAdd(record, columnReader);
//                columnReader.consume();
//            }
//            if (columnReader.getCurrentRepetitionLevel() != 0) {
//                throw new IllegalStateException("Unexpected repetition");
//            }
        }
        return record;
    }

    public Object[] getAdd(Object[] record, ColumnReader columnReader) {
        Object value = readValue(columnReader);
//        value = toValidValue(value, columnReader, config);
        if (value== null) {
            return record;
        }
        String[] path = columnReader.getDescriptor().getPath();
        String heading = path[0];
        if (index.get(heading) == null) {
            return record;
        }
        try {
//            Object curr = record[index.get(heading)];
            boolean list = path.length == 3 && path[1].equals("list");
            if (list) {
                List curr2 = (List) record[index.get(heading)];
                if (curr2 == null) {
                    ArrayList<Object> objects = new ArrayList<>();
                    objects.add(value);
                    record[index.get(heading)] = objects;
                } else {
//                    List curr2 = (List) record[index.get(heading)];
                    curr2.add(value);
                }
            } else {
                record[index.get(heading)] = value;
            }
//            if (curr != null && list) {
//                new ArrayList<>() {{ add(curr); add() }}
//            }
            // todo - if list
//                System.out.println("target = " + target);
        } catch (Exception e) {
            return record;
//                System.out.println("e = " + e);
        }
        return record;
//        return hydrator.add(record, columnReader.getDescriptor().getPath()[0], value);
    }

    public Object[] getAdd(Object[] record, Type type, Group simpleGroup) {
        Object value = getValue(type, simpleGroup);
        value = toValidValue1(value, type, config);
        String heading = type.getName();//columnReader.getDescriptor().getPath()[0];
        if (index.get(heading) == null) {
            return record;
        }
        try {
            record[index.get(heading)] = value;
//                System.out.println("target = " + target);
        } catch (Exception e) {
            return record;
//                System.out.println("e = " + e);
        }
        return record;
//        return hydrator.add(record, columnReader.getDescriptor().getPath()[0], value);
    }

    public ParquetMetadata metaData() {
        return this.reader.getFooter();
    }

    public static InputFile makeInputFile(File file) {
        return new InputFile() {
            @Override
            public long getLength() {
                return file.length();
            }

            @Override
            public SeekableInputStream newStream() throws IOException {
                FileInputStream fis = new FileInputStream(file);
                return new DelegatingSeekableInputStream(fis) {
                    private long position;

                    @Override
                    public long getPos() {
                        return position;
                    }

                    @Override
                    public void seek(long newPos) throws IOException {
                        fis.getChannel().position(newPos);
                        position = newPos;
                    }
                };
            }
        };
    }
}

