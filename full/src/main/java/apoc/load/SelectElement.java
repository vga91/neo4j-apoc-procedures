package apoc.load;


import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.neo4j.logging.Log;

import java.util.List;
import java.util.Map;

import static apoc.load.LoadHtml.getElements;

public class SelectElement implements LoadHtmlFunctions.LoadElementInterface {
    private final String query;

    public SelectElement(String query) {
        this.query = query;
    }

    @Override
    public List<Map<String, Object>> get(Document document, Map<String, Object> config, List<String> errorList, Log log) {

        final Elements select = document.select(query);
        return getElements(select, config, errorList, log);
    }
}