package apoc.util;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static apoc.util.Util.getFormat;

public class DateParseUtil {
    private static Map<Class<? extends TemporalAccessor>, MethodHandle> parseDateMap = new ConcurrentHashMap<>();
    private static Map<Class<? extends TemporalAccessor>, MethodHandle> simpleParseDateMap = new ConcurrentHashMap<>();
    private static String METHOD_NAME = "parse";

    public static TemporalAccessor dateParse(String value, Class<? extends TemporalAccessor> date, String...formats) {
        return dateParse(value, date, null, formats);
    }

    public static TemporalAccessor dateParse(String value, Class<? extends TemporalAccessor> date, ZoneId zoneId, String...formats) {
        try {
            if (formats != null && formats.length > 0) {
                for (String form : formats) {
                    try {
                        try {
                            return getParse(date, getFormat(form), value, zoneId);
                        } catch (DateTimeParseException e) {
                            return getParse(date, value, zoneId);
                        }
                    } catch (Exception e) {
                        continue;
                    }
                }
            } else {
                return getParse(date, value, zoneId);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        throw new RuntimeException("Can't format the date with the pattern");
    }

    private static TemporalAccessor getParse(Class<? extends TemporalAccessor> date, DateTimeFormatter format, String value, ZoneId zoneId) throws Throwable {

        MethodHandle methodHandle = parseDateMap.computeIfAbsent(date, method -> {
            MethodHandles.Lookup lookup = MethodHandles.publicLookup();
            try {
                return lookup.findStatic(date, METHOD_NAME, MethodType.methodType(date, CharSequence.class, DateTimeFormatter.class));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        
        try {
            return (TemporalAccessor) methodHandle.invokeWithArguments(value, format);
        } catch (DateTimeException e) {
            if (zoneId != null) {
                if (date.equals(ZonedDateTime.class)) {
                    return LocalDateTime.parse(value, format).atZone(zoneId);
                }
                if (date.equals(OffsetTime.class)) {
                    return LocalTime.parse(value, format).atOffset(zoneId.getRules().getOffset(Instant.now()));
                }
            }
            throw e;
        }
    }

    private static TemporalAccessor getParse(Class<? extends TemporalAccessor> date, String value, ZoneId zoneId) throws Throwable {
        MethodHandle methodHandleSimple = simpleParseDateMap.computeIfAbsent(date, method -> {
            MethodHandles.Lookup lookup = MethodHandles.publicLookup();
            try {
                return lookup.findStatic(date, METHOD_NAME, MethodType.methodType(date, CharSequence.class));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        
        try {
            return (TemporalAccessor) methodHandleSimple.invokeWithArguments(value);
        } catch (DateTimeException e) {
            if (zoneId != null) {
                if (date.equals(ZonedDateTime.class)) {
                    return LocalDateTime.parse(value).atZone(zoneId);
                }
                if (date.equals(OffsetTime.class)) {
                    return LocalTime.parse(value).atOffset(zoneId.getRules().getOffset(Instant.now()));
                }
            }
            throw e;
        }
    }

}
