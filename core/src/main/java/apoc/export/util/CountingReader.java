package apoc.export.util;

import java.io.*;
import java.util.Collection;
import java.util.Set;

/**
 * @author mh
 * @since 22.05.16
 */
public class CountingReader extends FilterReader implements SizeCounter {
    public static final int BUFFER_SIZE = 1024 * 1024;
    private final long total;
    private long count=0;
    private long newLines;
    private Set<Character> invalidChars = Set.of('\uFEFF');

    public CountingReader(File file) throws FileNotFoundException {
        super(new BufferedReader(new FileReader(file), BUFFER_SIZE));
        this.total = file.length();
    }
    public CountingReader(Reader reader, long total) throws FileNotFoundException {
        super(new BufferedReader(reader, BUFFER_SIZE));
        this.total = total;
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        int read = super.read(cbuf, off, len);
        count+=read;

        if (read == -1) {
            return -1;
        }
        int validIdx = off - 1;
        for (int i=off;i<off+read;i++) {
            if (invalidChars.contains(cbuf[i])) {
                continue;
            }
            validIdx++;
            cbuf[validIdx] = cbuf[i];
            if (cbuf[validIdx] == '\n') newLines++;
        }
        return validIdx - off + 1;
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

    public Set<Character> getInvalidChars() {
        return invalidChars;
    }

    public void addInvalidChars(Collection<Character> invalidChars) {
        this.invalidChars.addAll(invalidChars);
    }
}
