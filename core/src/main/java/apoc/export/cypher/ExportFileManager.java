package apoc.export.cypher;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;

public interface ExportFileManager {
    PrintWriter getPrintWriter(String type, String compression);

//    PrintWriter getPrintWriter(String type);

//    Writer getWriter(String type, String compression) throws IOException;
    StringWriter getStringWriter(String type, String compression);

    String drain(String type);

    String getFileName();

    Boolean separatedFiles();
}
