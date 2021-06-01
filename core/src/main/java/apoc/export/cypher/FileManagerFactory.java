package apoc.export.cypher;

import apoc.export.util.ExportConfig;
import apoc.util.CompressionAlgo;
import apoc.util.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author mh
 * @since 06.12.17
 */
public class FileManagerFactory {
    public static ExportFileManager createFileManager(String fileName, boolean separatedFiles, ExportConfig config) {
        if (fileName == null) {
            return new StringExportCypherFileManager(separatedFiles, config);
        }
        fileName = fileName.trim();

        final CompressionAlgo compressionAlgo = CompressionAlgo.valueOf(config.getCompressionAlgo());
        final String fileExt = compressionAlgo.getFileExt();
        // TODO - evaluate if validation is necessary
        if (!fileName.endsWith(fileExt)) {
            throw new RuntimeException("The file must have the extension " + fileExt);
        }

        int indexOfDot = StringUtils.lastOrdinalIndexOf(fileName, ".", compressionAlgo.equals(CompressionAlgo.NONE) ? 1 : 2);
        String fileType = fileName.substring(indexOfDot + 1);
        return new PhysicalExportFileManager(fileType, fileName, separatedFiles, config);
    }

    private static class PhysicalExportFileManager implements ExportFileManager {

        private final String fileName;
        private final String fileType;
        private final boolean separatedFiles;
        // todo - e questo, come ci arriva?
        private PrintWriter writer;
        private ExportConfig config;

        public PhysicalExportFileManager(String fileType, String fileName, boolean separatedFiles, ExportConfig config) {
            this.fileType = fileType;
            this.fileName = fileName;
            this.separatedFiles = separatedFiles;
            this.config = config;
        }
        
        @Override
        public PrintWriter getPrintWriter(String type) {

            if (this.separatedFiles) {
                return FileUtils.getPrintWriter(normalizeFileName(fileName, type), null, config);
            } else {
                // todo .. ma scusa, quando è diverso da null?
                if (this.writer == null) {
                    this.writer = FileUtils.getPrintWriter(normalizeFileName(fileName, null), null, config);
                }
                return this.writer;
            }
        }

        @Override
        public StringWriter getStringWriter(String type/*, String compression*/) {
            return null;
        }

        private String normalizeFileName(final String fileName, String suffix) {
            // TODO check if this should be follow the same rules of FileUtils.readerFor
            return fileName.replace("." + fileType, "." + (suffix != null ? suffix + "." +fileType : fileType));
        }

        @Override
        public String drain(String type) {
            return null;
        }

        @Override
        public String getFileName() {
            return this.fileName;
        }

        @Override
        public Boolean separatedFiles() {
            return this.separatedFiles;
        }
    }

    private static class StringExportCypherFileManager implements ExportFileManager {

        private boolean separatedFiles;
        private ExportConfig config;
        private ConcurrentMap<String, StringWriter> writers = new ConcurrentHashMap<>();

        public StringExportCypherFileManager(boolean separatedFiles, ExportConfig config) {
            this.separatedFiles = separatedFiles;
            this.config = config;
        }

//        @Override
//        public PrintWriter getPrintWriter(String type) {
//            if (this.separatedFiles) {
//                return new PrintWriter(getStringWriter(type));
//            } else {
//                switch (type) {
//                    case "csv":
//                    case "json":
//                    case "graphml":
//                        break;
//                    default:
//                        type = "cypher";
//                }
//                return new PrintWriter(getStringWriter(type));
//            }
//        }

//        @Override
//        public Writer getWriter(String type, String compression) throws IOException{
//            return null;
//            // TODO - POI VEDIAMO SE SERVE.. NEL CASO NON LO METTO NELLA INTERFACCIA
//        }

        @Override
        public StringWriter getStringWriter(String type){//, String compression) {
            // todo - e poi che fa?
            return writers.computeIfAbsent(type, (key) -> new StringWriter());
        }

        @Override
        public PrintWriter getPrintWriter(String type){//, String compression) {
            final String compression = config.getCompressionAlgo();
            if (this.separatedFiles) {
                return new PrintWriter(getStringWriter(type/*, compression*/));
            } else {
                switch (type) {
                    case "csv":
                    case "json":
                    case "graphml":
                        break;
                    default:
                        type = "cypher";
                }
                return new PrintWriter(getStringWriter(type/*, compression*/));
            }
        }

        @Override
        public synchronized Object drain(String type) {
            // todo - ma allora questo che fa? - exportCypher
            StringWriter writer = writers.get(type);
            if (writer != null) {
                try {
                    // TODO - COMMON CON L'ALTRO DRAIN...
                    final String compression = config.getCompressionAlgo();
                    final String writerString = writer.toString();
                    Object data = compression.equals(CompressionAlgo.NONE.name())
                            ? writerString
                            : CompressionAlgo.valueOf(compression).compress(writerString, config.getCharset());
                    writer.getBuffer().setLength(0);
                    return data;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            else return null;
        }

        @Override
        public String getFileName() {
            return null;
        }

        @Override
        public Boolean separatedFiles() {
            return this.separatedFiles;
        }
    }

}
