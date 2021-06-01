package apoc.util;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.nio.charset.Charset;


public class BinaryTestUtil {

    public static String readFileToString(File file, Charset charset, CompressionAlgo compression) {
        try {
            return compression.equals(CompressionAlgo.NONE) ?
                    TestUtil.readFileToString(file, charset)
                    : compression.decompress(FileUtils.readFileToByteArray(file), charset);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
