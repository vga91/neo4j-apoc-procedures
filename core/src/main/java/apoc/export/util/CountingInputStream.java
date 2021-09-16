package apoc.export.util;

import java.io.*;
import java.util.function.Function;

/**
 * @author mh
 * @since 22.05.16
 */
public class CountingInputStream extends FilterInputStream implements SizeCounter {
    public static final int BUFFER_SIZE = 1024 * 1024;
    private final long total;
    private Function<Character, Boolean> ignoreCondition = character -> false;
    private long count=0;
    private long newLines;

    public CountingInputStream(File file) throws FileNotFoundException {
        super(new BufferedInputStream(new FileInputStream(file), BUFFER_SIZE));
        this.total = file.length();
    }
    public CountingInputStream(InputStream stream, long total) {
        super(new BufferedInputStream(stream, BUFFER_SIZE));
        this.total = total;
    }
    public CountingInputStream(InputStream stream, long total, Function<Character, Boolean> ignoreFunction) {
        this(stream, total);
        this.ignoreCondition = ignoreFunction;
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
        int read = super.read(buf, off, len);
        count+=read;
        
        if (read == -1) {
            return -1;
        }
        int indexeEvaluated = off - 1;
        for (int currIndex = off; currIndex < off + read; currIndex++) {
            if (ignoreCondition.apply((char) buf[currIndex])) {
                continue;
            } else {
                indexeEvaluated++;
            }
            if (indexeEvaluated < currIndex) {
                buf[indexeEvaluated] = buf[currIndex];
            }
            if (buf[indexeEvaluated] == '\n') newLines++;
        }
        return indexeEvaluated - off + 1;
    }

    @Override
    public int read() throws IOException {
        count++;
        int read = super.read();
        if (read == '\n') newLines++;
        return read;
    }

    @Override
    public long skip(long n) throws IOException {
        count += n;
        return super.skip(n);
    }

    public long getCount() {
        return count;
    }

    public long getNewLines() {
        return newLines;
    }

    public long getTotal() {
        return total;
    }

    @Override
    public long getPercent() {
        if (total <= 0) return 0;
        return count*100 / total;
    }

    public InputStream getStream() {
	   return in;
    }

	public CountingReader asReader() throws IOException {
		Reader reader = new InputStreamReader(in,"UTF-8");
        return new CountingReader(reader,total);
	}
}
