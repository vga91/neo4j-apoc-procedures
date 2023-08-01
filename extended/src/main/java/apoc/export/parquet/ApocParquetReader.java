package apoc.export.parquet;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.export.parquet.ParquetReadUtil.toTimeUnitJava;
import static apoc.export.parquet.ParquetReadUtil.toValidValue;

public final class ApocParquetReader implements Closeable {
    private final ParquetFileReader reader;
    private final List<ColumnDescriptor> columns;
    private final MessageType schema;
    private final GroupConverter recordConverter;
    private final String createdBy;
    private final Map<String, Integer> index;

    private long currentRowGroupSize = -1L;
    private List<ColumnReader> currentRowGroupColumnReaders;
    private long currentRowIndex = -1L;
    private final ParquetConfig config;

    public ApocParquetReader(InputFile file, ParquetConfig config) throws IOException {
        this.reader = ParquetFileReader.open(file);
        FileMetaData meta = reader.getFooter().getFileMetaData();
        this.schema = meta.getSchema();
        this.recordConverter = new GroupRecordConverter(this.schema).getRootConverter();
        this.createdBy = meta.getCreatedBy();

        this.columns = schema.getColumns()
                .stream()
                .collect(Collectors.toList());

        this.index = new HashMap<>(columns.size());
        int idx = 0;
        for (ColumnDescriptor d : columns) {
            this.index.put(d.getPath()[0], idx++);
        }
        this.config = config;
    }

    private Object readValue(ColumnReader columnReader) {
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
                default:
                    throw new IllegalArgumentException("Unsupported type: " + primitiveType);
            }
        } else {
            return null;
        }
    }

    public Map<String, Object> getRecord() throws IOException {
        if (currentRowIndex == currentRowGroupSize) {

            PageReadStore rowGroup = reader.readNextRowGroup();
            if (rowGroup == null) {
                return null;
            }

            ColumnReadStore columnReadStore = new ColumnReadStoreImpl(rowGroup, this.recordConverter, this.schema, this.createdBy);

            this.currentRowGroupSize = rowGroup.getRowCount();
            this.currentRowGroupColumnReaders = columns.stream()
                    .map(columnReadStore::getColumnReader)
                    .collect(Collectors.toList());
            this.currentRowIndex = 0L;
        }

        Object[] record = new Object[index.size()];
        for (ColumnReader columnReader: this.currentRowGroupColumnReaders) {
            // if it's a list we have use columnReader.consume() multiple times (until columnReader.getCurrentRepetitionLevel() == 0, i.e. totally consumed)
            // to collect the list elements
            do {
                addRecord(record, columnReader);
                columnReader.consume();
            } while (columnReader.getCurrentRepetitionLevel() != 0);
        }

        this.currentRowIndex++;

        return this.index.entrySet().stream()
                .filter(e -> {
                    try {
                        return record[e.getValue()] != null;
                    } catch (Exception ex) {
                        return false;
                    }
                })
                .collect(Collectors.toMap(Map.Entry::getKey, e -> toValidValue(record[e.getValue()], e.getKey(), config)));
    }

    public void addRecord(Object[] record, ColumnReader columnReader) {
        Object value = readValue(columnReader);
        if (value== null) {
            return;
        }
        String[] path = columnReader.getDescriptor().getPath();
        String heading = path[0];
        if (index.get(heading) == null) {
            return;
        }
        try {
            boolean isAList = path.length == 3 && path[1].equals("list");
            if (isAList) {
                List curr2 = (List) record[index.get(heading)];
                if (curr2 == null) {
                    ArrayList<Object> objects = new ArrayList<>();
                    objects.add(value);
                    record[index.get(heading)] = objects;
                } else {
                    curr2.add(value);
                }
            } else {
                record[index.get(heading)] = value;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}

