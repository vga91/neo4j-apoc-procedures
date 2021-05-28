package apoc.util;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.nio.charset.Charset;


public class BinaryTestUtil {

    public static String readFileToString(File file, Charset charset, String compression) throws Exception {
        return compression.equals(CompressionAlgo.NONE.name()) ?
                TestUtil.readFileToString(file, charset) 
                : CompressionAlgo.valueOf(compression).decompress(FileUtils.readFileToByteArray(file), charset);
    }
    
}
