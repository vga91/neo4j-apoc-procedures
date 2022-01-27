package apoc.load;

import apoc.generate.config.InvalidConfigException;
import org.apache.commons.lang3.BooleanUtils;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class XmlImportConfig extends LoadImportConfig {

    private boolean connectCharacters;
    private Pattern delimiter;
    private Label label = Label.label("XmlCharacters");
    private RelationshipType relType = RelationshipType.withName("NE");
    private Map<String, String> charactersForTag = new HashMap<>();
    final private boolean filterLeadingWhitespace;

    public XmlImportConfig(Map<String, Object> config) {
        super(config);
        if (config == null) {
            config = Collections.emptyMap();
        }
        connectCharacters = BooleanUtils.toBoolean((Boolean) config.get("connectCharacters"));
        filterLeadingWhitespace = BooleanUtils.toBoolean((Boolean) config.get("filterLeadingWhitespace"));

        String _delimiter = (String) config.get("delimiter");
        if (_delimiter != null) {
            connectCharacters = true;
        }
        delimiter = Pattern.compile(_delimiter == null ? "\\s" : _delimiter);

        String _label = (String) config.get("label");
        if (_label != null) {
            label = Label.label(_label);
            connectCharacters = true;
        }

        String _relType = (String) config.get("relType");
        if (_relType != null) {
            relType = RelationshipType.withName(_relType);
            connectCharacters = true;
        }

        Map<String,String> _charactersForTag = (Map<String, String>) config.get("charactersForTag");
        if (_charactersForTag !=null) {
            charactersForTag = _charactersForTag;
        }

        if (config.containsKey("createNextWordRelationships")) {
            throw new InvalidConfigException("usage of `createNextWordRelationships` is no longer allowed. Use `{relType:'NEXT_WORD', label:'XmlWord'}` instead.");
        }
    }

    @Override
    public Object createMapping(Object input) {
        return null;
    }

    public Pattern getDelimiter() {
        return delimiter;
    }

    public Label getLabel() {
        return label;
    }

    public RelationshipType getRelType() {
        return relType;
    }

    public boolean isConnectCharacters() {
        return connectCharacters;
    }

    public Map<String, String> getCharactersForTag() {
        return charactersForTag;
    }

    public boolean isFilterLeadingWhitespace() {
        return filterLeadingWhitespace;
    }

}