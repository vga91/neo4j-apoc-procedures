package apoc.export.parquet;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class ExportParquetMockTest {

    private static File directory = new File("target/parquet import");
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

//    @ClassRule
//    public static DbmsRule db = new ImpermanentDbmsRule()
//            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());

    @Test
    public void testAdd() throws IOException {
        ParquetReaderWriterWithAvro parquetReaderWriterWithAvro = new ParquetReaderWriterWithAvro();

        System.out.println("ExportParquetTest -- streaming mode");

        byte[] stream = parquetReaderWriterWithAvro.stream(parquetReaderWriterWithAvro.sampleData);
        parquetReaderWriterWithAvro.streamRead(stream);
    }

    @Test
    public void testAdd1() throws IOException {
        ParquetReaderWriterWithAvro parquetReaderWriterWithAvro = new ParquetReaderWriterWithAvro();
        System.out.println("ExportParquetMockTest.testAdd1");
        ParquetReaderWriterWithAvro parquetReaderWriterWithAvro1 = new ParquetReaderWriterWithAvro(true);

    }


}
