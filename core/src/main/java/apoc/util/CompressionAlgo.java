package apoc.util;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.deflate.DeflateCompressorInputStream;
import org.apache.commons.compress.compressors.deflate.DeflateCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorInputStream;
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorOutputStream;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorInputStream;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public enum CompressionAlgo {

    NONE(null, null, StringUtils.EMPTY),
    GZIP(GzipCompressorOutputStream.class, GzipCompressorInputStream.class, ".gz"),
    BZIP2(BZip2CompressorOutputStream.class, BZip2CompressorInputStream.class, ".bz2"),
    DEFLATE(DeflateCompressorOutputStream.class, DeflateCompressorInputStream.class, ".zz"),
    BLOCK_LZ4(BlockLZ4CompressorOutputStream.class, BlockLZ4CompressorInputStream.class, ".lz4"),
    FRAMED_SNAPPY(FramedSnappyCompressorOutputStream.class, FramedSnappyCompressorInputStream.class, ".sz");

    private final Class<?> compressor;
    private final Class<?> decompressor;
    private final String fileExt;

    CompressionAlgo(Class<?> compressor, Class<?> decompressor, String fileExt) {
        this.compressor = compressor;
        this.decompressor = decompressor;
        this.fileExt = fileExt;
    }

    public byte[] compress(String string, Charset charset) throws Exception {
        Constructor<?> constructor = compressor.getConstructor(OutputStream.class);
//        try (
                ByteArrayOutputStream stream = new ByteArrayOutputStream();//) {
            try (OutputStream outputStream = (OutputStream) constructor.newInstance(stream)) {
                outputStream.write(string.getBytes(charset));
            }
            return stream.toByteArray();
//        }
    }

    public String decompress(byte[] byteArray, Charset charset) throws Exception {
        Constructor<?> constructor = decompressor.getConstructor(InputStream.class);
        try (ByteArrayInputStream stream = new ByteArrayInputStream(byteArray);
                InputStream inputStream = (InputStream) constructor.newInstance((InputStream) stream)) {
            return IOUtils.toString(inputStream, charset);
        }
    }

    public OutputStream getOutputStream(OutputStream stream) throws Exception {
        return compressor == null ? stream : (OutputStream) compressor.getConstructor(OutputStream.class).newInstance(stream);
    }

    public String getFileExt() {
        return fileExt;
    }

    //    public byte[] compress(String string, Charset charset) throws Exception {
//        Constructor<?> constructor = compressor.getConstructor(OutputStream.class);
//        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
//            try (OutputStream outputStream = (OutputStream) constructor.newInstance((OutputStream) stream)) {
//                outputStream.write(string.getBytes(charset));
//            }
//            return stream.toByteArray();
//        }
//    }

}
