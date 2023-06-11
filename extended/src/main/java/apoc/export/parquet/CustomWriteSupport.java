package apoc.export.parquet;

import apoc.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.util.HashMap;
import java.util.List;

import static org.apache.parquet.io.api.Binary.fromConstantByteArray;

public class CustomWriteSupport extends WriteSupport<Object> {
    MessageType schema;
    RecordConsumer recordConsumer;
    List<ColumnDescriptor> cols;

    // TODO: support specifying encodings and compression
    CustomWriteSupport(MessageType schema) {
        this.schema = schema;
        this.cols = schema.getColumns();
    }

    @Override
    public WriteContext init(Configuration config) {
        return new WriteContext(schema, new HashMap<String, String>());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(Object values) {
//        if (values.size() != cols.size()) {
//            throw new ParquetEncodingException("Invalid input data. Expecting " +
//                                               cols.size() + " columns. Input had " + values.size() + " columns (" + cols + ") : " + values);
//        }

//        recordConsumer.startMessage();


        // TODO --> TRY THIS ONE AND SEE THE SPEED AND THE SPACE
        recordConsumer.addBinary(fromConstantByteArray( Util.toJson(values).getBytes() ));
//        for (int i = 0; i < cols.size(); ++i) {
//            String val = values.get(i);
//            // val.length() == 0 indicates a NULL value.
//            if (val.length() > 0) {
//                recordConsumer.startField(cols.get(i).getPath()[0], i);
//                switch (cols.get(i).getType()) {
//                    case BOOLEAN:
//                        recordConsumer.addBoolean(Boolean.parseBoolean(val));
//                        break;
//                    case FLOAT:
//                        recordConsumer.addFloat(Float.parseFloat(val));
//                        break;
//                    case DOUBLE:
//                        recordConsumer.addDouble(Double.parseDouble(val));
//                        break;
//                    case INT32:
//                        recordConsumer.addInteger(Integer.parseInt(val));
//                        break;
//                    case INT64:
//                        recordConsumer.addLong(Long.parseLong(val));
//                        break;
//                    case BINARY:
//                        recordConsumer.addBinary(stringToBinary(val));
//                        break;
//                    default:
//                        throw new ParquetEncodingException(
//                                "Unsupported column type: " + cols.get(i).getType());
//                }
//                recordConsumer.endField(cols.get(i).getPath()[0], i);
//            }
//        }
        recordConsumer.endMessage();
    }

    private Binary stringToBinary(Object value) {
        return Binary.fromString(value.toString());
    }
}