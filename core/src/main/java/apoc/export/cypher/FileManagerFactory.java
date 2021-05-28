package apoc.export.cypher;

import apoc.util.FileUtils;
import com.opencsv.CSVWriter;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author mh
 * @since 06.12.17
 */
public class FileManagerFactory {
    public static ExportFileManager createFileManager(String fileName, boolean separatedFiles) {
        if (fileName == null) {
            // questo lo fa se non c'è filename... ossia?
            // ... todo - ah ok, per lo streams
            // ... ma quindi se non è null non lo fa...
            return new StringExportCypherFileManager(separatedFiles);
        }
        // todo - per vedere se separare in più file
        
        // todo - a sto punto se separa in più file dovrei fare tipo un tar.gz

        int indexOfDot = fileName.lastIndexOf(".");
        String fileType = fileName.substring(indexOfDot + 1);
        return new PhysicalExportFileManager(fileType, fileName, separatedFiles);
    }

    private static class PhysicalExportFileManager implements ExportFileManager {

        private final String fileName;
        private final String fileType;
        private final boolean separatedFiles;
        // todo - e questo, come ci arriva?
        private PrintWriter writer;

        public PhysicalExportFileManager(String fileType, String fileName, boolean separatedFiles) {
            this.fileType = fileType;
            this.fileName = fileName;
            this.separatedFiles = separatedFiles;
        }
        
//        public Writer getWriter(String fileExtension, String compressionType) throws IOException {
//            
//            // TODO - FILENAME NDO LO PRENDE?
//            // TODO 2 -- .GZ RENDERLO DIVERSO IN BASE AL TIPO DI COMPRESSIONE
//            
//            if (compressionType.equals("GZIP")) {
//                // TODO - IMPLEMENTARE UN SUPPLIER NELLA COMPRESSION ALGO...
//
////                try (
//                FileOutputStream byteArrayOutputStream = new FileOutputStream(normalizeFileName(fileName, fileExtension + ".gz"));
//                     GzipCompressorOutputStream stream = new GzipCompressorOutputStream(byteArrayOutputStream);
////                File stream = new File("destinaton_zip_filepath.csv.gz");
////                OutputStreamWriter streamWriter = new FileWriter(stream); // --> FileWriter con fileName... forse è questo 
////                     ) { // --> FileWriter con fileName... forse è questo 
//                    return new PrintWriter(stream);
////                    CSVWriter out = new CSVWriter(streamWriter);
////                }
//            } else {
//                return getPrintWriter(fileExtension);
//            }
//        }

//        @Override
//        public PrintWriter getPrintWriter(String type) {
//            return getPrintWriter(type, "NONE");
//        }

        @Override
        public PrintWriter getPrintWriter(String type, String compressionType) {
            // TODO - POTREI PASSARE IL CONFIG E FARE COSE...

//            if ()
            return getPrintWriter1todo(type, compressionType);
        }

        private PrintWriter getPrintWriter1todo(String type, String compressionType) {
            if (this.separatedFiles) {
                return FileUtils.getPrintWriter(normalizeFileName(fileName, type), null, compressionType);
            } else {
                // todo .. ma scusa, quando è diverso da null?
                if (this.writer == null) {
                    // TODO ..
                    this.writer = FileUtils.getPrintWriter(normalizeFileName(fileName, null), null, compressionType);
                }
                return this.writer;
            }
        }

        @Override
        public StringWriter getStringWriter(String type, String compression) {
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
        private ConcurrentMap<String, StringWriter> writers = new ConcurrentHashMap<>();

        public StringExportCypherFileManager(boolean separatedFiles) {
            this.separatedFiles = separatedFiles;
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
        public StringWriter getStringWriter(String type, String compression) {
            // todo - e poi che fa?
            return writers.computeIfAbsent(type, (key) -> new StringWriter());
        }

        @Override
        public PrintWriter getPrintWriter(String type, String compression) {
            if (this.separatedFiles) {
                return new PrintWriter(getStringWriter(type, compression));
            } else {
                switch (type) {
                    case "csv":
                    case "json":
                    case "graphml":
                        break;
                    default:
                        type = "cypher";
                }
                return new PrintWriter(getStringWriter(type, compression));
            }
        }

        @Override
        public synchronized String drain(String type) {
            // todo - ma allora questo che fa? - exportCypher
            StringWriter writer = writers.get(type);
            if (writer != null) {
                String text = writer.toString();
                writer.getBuffer().setLength(0);
                return text;
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
