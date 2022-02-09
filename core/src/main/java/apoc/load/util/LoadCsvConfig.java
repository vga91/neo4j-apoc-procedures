package apoc.load.util;

import apoc.load.LoadImportConfig;
import apoc.load.CsvMapping;
import apoc.util.Util;

import java.util.*;
import java.util.stream.Collectors;

import static apoc.util.Util.parseCharFromConfig;
import static java.util.Arrays.asList;

public class LoadCsvConfig extends LoadImportConfig {

    public static final char DEFAULT_SEP = ',';
    public static final char DEFAULT_QUOTE_CHAR = '"';
    // this is the same value as ICSVParser.DEFAULT_ESCAPE_CHARACTER
    public static final char DEFAULT_ESCAPE_CHAR = '\\';

    private final boolean ignoreErrors;
    private char separator;
    private char quoteChar;
    private char escapeChar;
    private long skip;
    private boolean hasHeader;
    private long limit;

    private boolean failOnError;
    private boolean ignoreQuotations;

    private EnumSet<Results> results;

    private Map<String, CsvMapping> mappings;

    public LoadCsvConfig(Map<String, Object> config) {
        super(config);
        if (config == null) {
            config = Collections.emptyMap();
        }
        ignoreErrors = Util.toBoolean(config.getOrDefault("ignoreErrors", false));
        separator = parseCharFromConfig(config, "sep", DEFAULT_SEP);
        quoteChar = parseCharFromConfig(config,"quoteChar", DEFAULT_QUOTE_CHAR);
        escapeChar = parseCharFromConfig(config,"escapeChar", DEFAULT_ESCAPE_CHAR);
        long skip = (long) config.getOrDefault("skip", 0L);
        this.skip = skip > -1 ? skip : 0L;
        hasHeader = (boolean) config.getOrDefault("header", true);
        limit = (long) config.getOrDefault("limit", Long.MAX_VALUE);
        failOnError = (boolean) config.getOrDefault("failOnError", true);
        ignoreQuotations = (boolean) config.getOrDefault("ignoreQuotations", false);

        results = EnumSet.noneOf(Results.class);
        List<String> resultList = (List<String>) config.getOrDefault("results", asList("map","list"));
        for (String result : resultList) {
            results.add(Results.valueOf(result));
        }

        mappings = createMapping(null);
    }

    @Override
    public Map<String, CsvMapping> createMapping(Object ignored) {
        return (Map<String, CsvMapping>) this.mapping.keySet()
                .stream()
                .collect(Collectors.toMap((k) -> k, (k) -> new CsvMapping((String) k, this)));
    }

    public char getSeparator() {
        return separator;
    }

    public long getSkip() {
        return skip;
    }

    public boolean isHasHeader() {
        return hasHeader;
    }

    public long getLimit() {
        return limit;
    }

    public boolean isFailOnError() {
        return failOnError;
    }

    public EnumSet<Results> getResults() {
        return results;
    }

    public Map<String, CsvMapping> getMappings() {
        return mappings;
    }

    public char getQuoteChar() {
        return quoteChar;
    }

    public char getEscapeChar() {
        return escapeChar;
    }

    public boolean getIgnoreErrors() {
        return ignoreErrors;
    }

    public boolean isIgnoreQuotations() {
        return ignoreQuotations;
    }
}
