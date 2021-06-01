package apoc.export.cypher;

import apoc.export.util.ExportConfig;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;

public interface ExportFileManager {
    PrintWriter getPrintWriter(String type);

    StringWriter getStringWriter(String type);

    Object drain(String type);

    String getFileName();

    Boolean separatedFiles();
}
