package apoc.util;

import org.apache.commons.io.FileUtils;
import java.io.File;
import java.nio.charset.Charset;

import static java.nio.charset.StandardCharsets.UTF_8;

public class BinaryTestUtil {

    public static byte[] fileToBinary(File file, String compression) {
        try {
            final String data = FileUtils.readFileToString(file, UTF_8);
            return compression.equals(CompressionAlgo.NONE.name()) 
                    ? data.getBytes(UTF_8)
                    : CompressionAlgo.valueOf(compression).compress(data, UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String readFileToString(File file, Charset charset, String compression) throws Exception {
        return compression.equals(CompressionAlgo.NONE.name()) ?
                TestUtil.readFileToString(file, charset)
                : CompressionAlgo.valueOf(compression).decompress(FileUtils.readFileToByteArray(file), charset);
    }
}