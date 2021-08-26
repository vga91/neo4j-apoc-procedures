package apoc.path;

import apoc.convert.Convert;
import apoc.meta.Meta;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class PropertyMatcher {

    // regex for nameLabel{propertyPartOptional}
    public static final Pattern LABEL_TYPE_PATTERN = Pattern.compile("(?<labelOrType>.[^{]*)(\\{(?<props>.+)\\})?");
    // regex for prop1 = value1 / prop1 != value1 and so on
    public static final Pattern FIELD_PATTERN = Pattern.compile("(?<prop>.[^!><]+)(?<operator>=|>=|<=|<|>|!=)(?<value>.+)");
    
    public static boolean matchesProperties(Entity entity, String propertyString) {
        // when property part or relPropFilter / nodePropFilter node is not present
        if (propertyString == null) {
            return true;
        }
        
        final String[] splitOrs = propertyString.split("\\s*\\|\\s*");

        return Arrays.stream(splitOrs).anyMatch(orItem -> {
            final String[] splitAnds = orItem.split("\\s*&\\s*");
            return Arrays.stream(splitAnds).allMatch(andItem -> matchProperty(andItem, entity));
        });
    }

    private static boolean matchProperty(String orItem, Entity entity) {
        if (orItem.startsWith("+")) {
            return entity.hasProperty(orItem.substring(1));
        } 
        if(orItem.startsWith("-")) {
            return !entity.hasProperty(orItem.substring(1));
        }
        
        final Matcher matcher = FIELD_PATTERN.matcher(orItem);
        if (matcher.matches()) {
            final String propName = matcher.group("prop");
            final String value = matcher.group("value");
            final String operator = matcher.group("operator");
            final Object nodeProperty = entity.getProperty(propName, null);
            // when property doesn't exists
            if (nodeProperty == null) {
                return false;
            }
            final boolean isComparable = nodeProperty instanceof Comparable;
            final Object valueConverted = convertValue(value, nodeProperty.getClass());
            switch (operator) {
                case ">":
                    return isComparable && ((Comparable) nodeProperty).compareTo(valueConverted) > 0;
                case ">=":
                    return isComparable && ((Comparable) nodeProperty).compareTo(valueConverted) >= 0;
                case "<":
                    return isComparable && ((Comparable) nodeProperty).compareTo(valueConverted) < 0;
                case "<=":
                    return isComparable && ((Comparable) nodeProperty).compareTo(valueConverted) <= 0;
                case "!=":
                    return !checkEquality(value, nodeProperty);
                default: // '=' case:
                    return checkEquality(value, nodeProperty);
            }
        }
        
        // todo - qua quando ci va? (nb: true significa matchato)
        return true;
    }

    private static boolean checkEquality(String value, Object nodeProperty) {
        final Class<?> propertyClass = nodeProperty.getClass();
        if (propertyClass.isArray()) {
            final List<Object> propertyList = (List<Object>) Convert.convertToList(nodeProperty);
            final List<Object> valueList = Arrays.stream(value.split(",")).map(item -> convertValue(item, propertyClass.getComponentType())).collect(Collectors.toList());
            return valueList.equals(propertyList);
        } else {
            return convertValue(value, propertyClass).equals(nodeProperty);
        }
    }

    private static Object convertValue(String value, Class<?> nodeProperty) {
        nodeProperty = Meta.Types.primitivesMapping.getOrDefault(nodeProperty, nodeProperty);
        if (nodeProperty == Long.class) {
            return Long.valueOf(value);
        }
        if (nodeProperty == Integer.class) {
            return Integer.valueOf(value);
        }
        if (nodeProperty == Double.class) {
            return Double.valueOf(value);
        }
        if (nodeProperty == Float.class) {
            return Float.valueOf(value);
        }
        if (nodeProperty == Short.class) {
            return Short.valueOf(value);
        }
        if (nodeProperty == Byte.class) {
            return Byte.valueOf(value);
        }
        if (nodeProperty == LocalDate.class) {
            return DateValue.parse(value).asObjectCopy();
        }
        if (nodeProperty == ZonedDateTime.class) {
            return DateTimeValue.parse(value, ZoneId::systemDefault).asObjectCopy();
        }
        if (nodeProperty == LocalDateTime.class) {
            return LocalDateTimeValue.parse(value).asObjectCopy();
        }
        if (nodeProperty == LocalTime.class) {
            return LocalTimeValue.parse(value).asObjectCopy();
        }
        if (nodeProperty == OffsetTime.class) {
            return TimeValue.parse(value, ZoneId::systemDefault).asObjectCopy();
        }
        if (nodeProperty == DurationValue.class) {
            return DurationValue.parse(value).asObjectCopy();
        }
        if (nodeProperty == PointValue.class) {
            return PointValue.parse(value);
        }
        return value;
    }

    public static String getPropsMatched(Matcher matcher, String props) {
        final String propsMatched = matcher.group("props");
        if (propsMatched != null) {
            props = propsMatched;
        }
        return props;
    }
}
