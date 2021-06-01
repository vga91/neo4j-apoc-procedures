package apoc.util;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.nio.charset.Charset;

import static java.nio.charset.StandardCharsets.UTF_8;


public class BinaryTestUtil {

    public static String readFileToString(File file, Charset charset, CompressionAlgo compression) {
        try {
            return compression.isNone() ?
                    TestUtil.readFileToString(file, charset)
                    : compression.decompress(FileUtils.readFileToByteArray(file), charset);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String getDecompressedData(CompressionAlgo algo, Object data) {
        try {
            return algo.isNone() ? (String) data : algo.decompress((byte[]) data, UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
}
