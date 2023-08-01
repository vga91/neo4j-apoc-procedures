//package apoc.export.parquet;
//
//import apoc.export.Hydrator;
//import apoc.export.HydratorSupplier;
//import org.apache.parquet.column.ColumnDescriptor;
//import org.apache.parquet.column.ColumnReadStore;
//import org.apache.parquet.column.ColumnReader;
//import org.apache.parquet.column.impl.ColumnReadStoreImpl;
//import org.apache.parquet.column.page.PageReadStore;
//import org.apache.parquet.example.DummyRecordConverter;
//import org.apache.parquet.hadoop.ParquetFileReader;
//import org.apache.parquet.hadoop.metadata.FileMetaData;
//import org.apache.parquet.hadoop.metadata.ParquetMetadata;
//import org.apache.parquet.io.DelegatingSeekableInputStream;
//import org.apache.parquet.io.InputFile;
//import org.apache.parquet.io.SeekableInputStream;
//import org.apache.parquet.io.api.GroupConverter;
//import org.apache.parquet.schema.MessageType;
//import org.apache.parquet.schema.PrimitiveType;
//
//import java.io.Closeable;
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.stream.Collectors;
//
//import static apoc.export.parquet.ParquetReadUtil.toValidValue;
//
////import static apoc.export.parquet.ParquetReadUtil.toValidValue;
//
//public final class ApocParquetReader2<U, S> implements Closeable {
//    private final ParquetFileReader reader;
//    private final Hydrator<U, S> hydrator;
//    private final List<ColumnDescriptor> columns;
//    private final MessageType schema;
//    private final GroupConverter recordConverter;
//    private final String createdBy;
//
//    private boolean finished;
//    private long currentRowGroupSize = -1L;
//    private List<ColumnReader> currentRowGroupColumnReaders;
//    private long currentRowIndex = -1L;
//    private ParquetConfig config;
////
////    public static <U, S> Stream<S> streamContent(File file, HydratorSupplier<U, S> hydrator) throws IOException {
////        return streamContent(file, hydrator, null);
////    }
////
////    public static <U, S> Stream<S> streamContent(File file, HydratorSupplier<U, S> hydrator, Collection<String> columns) throws IOException {
////        return streamContent(makeInputFile(file), hydrator, columns);
////    }
////
////    public static <U, S> Stream<S> streamContent(InputFile file, HydratorSupplier<U, S> hydrator) throws IOException {
////        return streamContent(file, hydrator, null);
////    }
////
////    public static <U, S> Stream<S> streamContent(InputFile file, HydratorSupplier<U, S> hydrator, Collection<String> columns) throws IOException {
////        return stream(spliterator(file, hydrator, columns));
////    }
////
////    public static <U, S> ApocParquetReader<U, S> spliterator(File file, HydratorSupplier<U, S> hydrator) throws IOException {
////        return spliterator(file, hydrator, null);
////    }
////
////    public static <U, S> ApocParquetReader<U, S> spliterator(File file, HydratorSupplier<U, S> hydrator, Collection<String> columns) throws IOException {
////        return spliterator(makeInputFile(file), hydrator, columns);
////    }
////
////    public static <U, S> ApocParquetReader<U, S> spliterator(InputFile file, HydratorSupplier<U, S> hydrator) throws IOException {
////        return spliterator(file, hydrator, null);
////    }
////
////    public static <U, S> ApocParquetReader<U, S> spliterator(InputFile file, HydratorSupplier<U, S> hydrator, Collection<String> columns) throws IOException {
////        Set<String> columnSet = (null == columns) ? Collections.emptySet() : Set.copyOf(columns);
////        return new ApocParquetReader<>(file, columnSet, hydrator, null);
////    }
////
////    public static <U, S> Stream<S> stream(ApocParquetReader<U, S> reader) {
////        return StreamSupport
////                .stream(reader, false)
////                .onClose(() -> closeSilently(reader));
////    }
////
////    public static Stream<String[]> streamContentToStrings(File file) throws IOException {
////        return stream(spliterator(makeInputFile(file), columns -> {
////            final AtomicInteger pos = new AtomicInteger(0);
////            return new Hydrator<String[], String[]>() {
////                @Override
////                public String[] start() {
////                    return new String[columns.size()];
////                }
////
////                @Override
////                public String[] add(String[] target, String heading, Object value) {
////                    target[pos.getAndIncrement()] = heading + "=" + value.toString();
////                    return target;
////                }
////
////                @Override
////                public String[] finish(String[] target) {
////                    return target;
////                }
////            };
////        }, null));
////    }
//
//    public static ParquetMetadata readMetadata(File file) throws IOException {
//        return readMetadata(makeInputFile(file));
//    }
//
//    public static ParquetMetadata readMetadata(InputFile file) throws IOException {
//        try (ParquetFileReader reader = ParquetFileReader.open(file)) {
//            return reader.getFooter();
//        }
//    }
//
////    private ApocParquetReader(InputFile file, Set<String> columnNames, HydratorSupplier<U, S> hydratorSupplier) throws IOException {
////        this.reader = ParquetFileReader.open(file);
////        FileMetaData meta = reader.getFooter().getFileMetaData();
////        this.schema = meta.getSchema();
////        this.recordConverter = new DummyRecordConverter(this.schema).getRootConverter();
////        this.createdBy = meta.getCreatedBy();
////
////        this.columns = schema.getColumns().stream()
////                .filter(c -> columnNames.isEmpty() || columnNames.contains(c.getPath()[0]))
////                .collect(Collectors.toList());
////
////        this.hydrator = hydratorSupplier.get(this.columns);
////    }
//
//    public ApocParquetReader2(InputFile file, Set<String> columnNames, HydratorSupplier<U, S> hydratorSupplier, ParquetConfig config) throws IOException {
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
//        this.config = config;
//    }
//
//    private static Object readValue(ColumnReader columnReader) {
//        ColumnDescriptor column = columnReader.getDescriptor();
//        PrimitiveType primitiveType = column.getPrimitiveType();
//        int maxDefinitionLevel = column.getMaxDefinitionLevel();
//
//        if (columnReader.getCurrentDefinitionLevel() == maxDefinitionLevel) {
//            switch (primitiveType.getPrimitiveTypeName()) {
//                case BINARY:
//                case FIXED_LEN_BYTE_ARRAY:
//                case INT96:
//                    return columnReader.getBinary().toStringUsingUTF8();
//                case BOOLEAN:
//                    return columnReader.getBoolean();
//                case DOUBLE:
//                    return columnReader.getDouble();
//                case FLOAT:
//                    return columnReader.getFloat();
//                case INT32:
//                    return columnReader.getInteger();
//                case INT64:
//                    return columnReader.getLong();
//                default:
//                    throw new IllegalArgumentException("Unsupported type: " + primitiveType);
//            }
//        } else {
//            return null;
//        }
//    }
//
//    @Override
//    public void close() throws IOException {
//        reader.close();
//    }
//
////    @Override
////    public boolean tryAdvance(Consumer<? super S> action) {
////        try {
////            if (this.finished) {
////                return false;
////            }
////
////            U record = getRecord();
////            if (record == null) return false;
////
////            action.accept(getFinish(record));
////
////
////            return true;
////        } catch (Exception e) {
////            throw new RuntimeException("Failed to read parquet", e);
////        }
////    }
//
//    public S getFinish(U record) {
//        return hydrator.finish(record);
////    }
//
//
//    public U getRecord() throws IOException {
//        if (currentRowIndex == currentRowGroupSize) {
//            PageReadStore rowGroup = reader.readNextRowGroup();
//            if (rowGroup == null) {
//                this.finished = true;
//                return null;
//            }
//
//            ColumnReadStore columnReadStore = new ColumnReadStoreImpl(rowGroup, this.recordConverter, this.schema, this.createdBy);
//
//            this.currentRowGroupSize = rowGroup.getRowCount();
//            this.currentRowGroupColumnReaders = columns.stream().map(columnReadStore::getColumnReader).collect(Collectors.toList());
//            this.currentRowIndex = 0L;
//        }
//
//        U record = hydrator.start();
//        record = getU(record);
//
//        this.currentRowIndex++;
//        return record;
//    }
//
//    public U getU(U record) {
//        for (ColumnReader columnReader: this.currentRowGroupColumnReaders) {
//            record = getAdd(record, columnReader);
//            columnReader.consume();
//            if (columnReader.getCurrentRepetitionLevel() != 0) {
//                throw new IllegalStateException("Unexpected repetition");
//            }
//        }
//        return record;
//    }
//
//    public U getAdd(U record, ColumnReader columnReader) {
//        Object value = readValue(columnReader);
//        value = toValidValue(value, columnReader, config);
//        return hydrator.add(record, columnReader.getDescriptor().getPath()[0], value);
//    }
//
//    public ParquetMetadata metaData() {
//        return this.reader.getFooter();
//    }
//
//    public static InputFile makeInputFile(File file) {
//        return new InputFile() {
//            @Override
//            public long getLength() {
//                return file.length();
//            }
//
//            @Override
//            public SeekableInputStream newStream() throws IOException {
//                FileInputStream fis = new FileInputStream(file);
//                return new DelegatingSeekableInputStream(fis) {
//                    private long position;
//
//                    @Override
//                    public long getPos() {
//                        return position;
//                    }
//
//                    @Override
//                    public void seek(long newPos) throws IOException {
//                        fis.getChannel().position(newPos);
//                        position = newPos;
//                    }
//                };
//            }
//        };
//    }
//}
//
