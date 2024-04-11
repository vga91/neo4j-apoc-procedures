package apoc.data.email;

import apoc.export.util.DurationValueSerializer;
import apoc.export.util.PointSerializer;
import apoc.export.util.TemporalSerializer;
import apoc.util.Util;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.values.storable.DurationValue;

import javax.mail.internet.*;
import java.time.temporal.Temporal;
import java.util.Map;

/**
 * Separated class in order to throw MissingDependencyException if `javax.mail` is not present
 */
public class ExtractEmailHandler {
    
    public static Map<String,String> extractEmail(String value) {
        if (value == null || value.indexOf('@') == -1) {
            return null;
        }
        try {
            InternetAddress addr = new InternetAddress(value);
            String rawAddr = addr.getAddress();
            int idx = rawAddr.indexOf('@');

            return Util.map("personal", addr.getPersonal(), "user", rawAddr.substring(0, idx), "domain", rawAddr.substring(idx + 1));
        } catch(AddressException adr) {
            return null;
        }
    }
}
