package apoc.export.parquet;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.InputFile;

import java.io.IOException;

// created custom Builder because of: https://issues.apache.org/jira/browse/PARQUET-1912,
// since ParquetFileReader is marked as "Internal implementation ..."
public class ApocParquetReaderBuilder extends ParquetReader.Builder<Group> {
    
    public static ApocParquetReaderBuilder read(InputFile file) throws IOException {
        return new ApocParquetReaderBuilder(file);
    }

    public static ApocParquetReaderBuilder builder(Path path) {
        return new ApocParquetReaderBuilder(path);
    }

    private ReadSupport<Group> readSupport;

    protected ApocParquetReaderBuilder(Path path) {
        super(path);
    }

    protected ApocParquetReaderBuilder(InputFile file) {
        super(file);
    }

    @Override
    protected ReadSupport<Group> getReadSupport() {
        if (readSupport == null) {
            return readSupport = new GroupReadSupport();
        }
        return readSupport;
    }
}
