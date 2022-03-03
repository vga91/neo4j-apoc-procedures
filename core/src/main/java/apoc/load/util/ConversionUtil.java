package apoc.load.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.deser.std.UntypedObjectDeserializer;
import org.neo4j.logging.Log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ConversionUtil {
    public static final String KEY_ERROR = "errorList";
    public static final String ERROR_VALUE = "__ERROR";
    public enum FailSilently { FALSE, WITH_LOG, WITH_LIST }
    
    public final static class SilentDeserializer extends UntypedObjectDeserializer {
        private final Log log;
        private final ConversionUtil.FailSilently failSilently;
        private final List<String> errorList = new ArrayList<>();

        public SilentDeserializer(ConversionUtil.FailSilently failSilently, Log log, JavaType listType, JavaType mapType) {
            super(listType, mapType);
            this.log = log;
            this.failSilently = failSilently;
        }

        @Override
        public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            try {
                // fallback to standard deserialization
                return super.deserialize(p, ctxt);
            } catch (IOException e) {
                final String errMsg = "Error with key " + p.getParsingContext().getCurrentName() + " - " + e.getMessage();
                switch (failSilently) {
                    case WITH_LIST:
                        errorList.add(errMsg);
                        return ERROR_VALUE;
                    case WITH_LOG:
                        if (log != null) {
                            log.error(errMsg);
                        }
                        return ERROR_VALUE;
                    default:
                        throw new RuntimeException(e);
                }
            }
        }

        public List<String> getErrorList() {
            return errorList;
        }
    }

}
