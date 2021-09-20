package apoc.path;

import apoc.convert.Convert;
import apoc.meta.Meta;
import apoc.util.Util;
import org.neo4j.graphdb.Entity;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class PropertyMatcher {

    // regex for nameLabel {propertyPartOptional}
    public static final Pattern LABEL_TYPE_PATTERN = Pattern.compile("(?<labelOrType>.*?(?=\\s\\{)|.*)(\\s*\\{(?<props>.+)\\})?");
    // regex for prop1 = value1 / prop1 != value1 and so on
    public static final Pattern FIELD_PATTERN = Pattern.compile("(?<prop>.[^!><]+)(?<operator>=|>=|<=|<|>|!=)(?<value>.+)");
    
    public static boolean matchesProperties(Entity entity, String propertyString, boolean regexMode) {
        // when property part or relPropFilter / nodePropFilter node is not present
        if (propertyString == null) {
            return true;
        }
        
        final String[] splitOrs = propertyString.split("\\s*\\|\\s*");

        return Arrays.stream(splitOrs).anyMatch(orItem -> {
            final String[] splitAnds = orItem.split("\\s*&\\s*");
            return Arrays.stream(splitAnds).allMatch(andItem -> matchProperty(andItem, entity, regexMode));
        });
    }

    private static boolean matchProperty(String andItem, Entity entity, boolean regexMode) {
        
        if (andItem.startsWith("+")) {
            return isPropertyExistent(andItem, entity, regexMode);
        } 
        if(andItem.startsWith("-")) {
            return !isPropertyExistent(andItem, entity, regexMode);
        }
        
        final Matcher matcher = FIELD_PATTERN.matcher(andItem);
        if (matcher.matches()) {
            final String propName = matcher.group("prop");
            final String value = matcher.group("value");
            final String operator = matcher.group("operator");
            if (regexMode) {
                return entity.getAllProperties().entrySet().stream()
                        .filter(prop -> prop.getKey().matches(propName))
                        .anyMatch(prop -> isPropertyMatched(value, operator, prop.getValue()));
            } else {
                Object nodeProperty = entity.getProperty(propName, null);
                // when property doesn't exists
                if (nodeProperty == null) {
                    return false;
                }
                return isPropertyMatched(value, operator, nodeProperty);
            }
        }
        
        return false;
    }

    private static boolean isPropertyExistent(String orItem, Entity entity, boolean regexMode) {
        final String propSubstring = orItem.substring(1);
        return regexMode
                ? entity.getAllProperties().entrySet().stream().anyMatch(prop -> prop.getKey().matches(propSubstring))
                : entity.hasProperty(propSubstring);
    }

    private static boolean isPropertyMatched(String value, String operator, Object nodeProperty) {
        final boolean isComparable = nodeProperty instanceof Comparable;
        final Object valueConverted = convertValue(value, nodeProperty.getClass());
        if (nodeProperty.getClass().isArray()) {
            nodeProperty = (List<Object>) Convert.convertToList(nodeProperty);
        }
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
                return !nodeProperty.equals(valueConverted);
            default: // '=' case:
                return nodeProperty.equals(valueConverted);
        }
    }

    private static Object convertValue(String value, Class<?> nodeProperty) {
        // todo - evaluate reuse of Mapping.java from vga91:issue-1471  
        final Meta.Types metaType = Meta.Types.of(nodeProperty);
        switch (metaType) {
            case POINT:
                return PointValue.parse(value);
            case LOCAL_DATE_TIME:
                return LocalDateTimeValue.parse(value).asObjectCopy();
            case LOCAL_TIME:
                return LocalTimeValue.parse(value).asObjectCopy();
            case DATE_TIME:
                return DateTimeValue.parse(value, ZoneId::systemDefault).asObjectCopy();
            case TIME:
                return TimeValue.parse(value, ZoneId::systemDefault).asObjectCopy();
            case DATE:
                return DateValue.parse(value).asObjectCopy();
            case DURATION:
                return DurationValue.parse(value);
            case INTEGER: 
                return Util.toLong(value);
            case FLOAT: 
                return Util.toDouble(value);
            case BOOLEAN: 
                return Util.toBoolean(value);
            case LIST:
                return Arrays.stream(value.split(","))
                        .map(item -> convertValue(item, nodeProperty.getComponentType()))
                        .collect(Collectors.toList());
            default:
                return value;
        }
    }

    public static String getPropsMatched(Matcher matcher, String props) {
        final String propsMatched = matcher.group("props");
        if (propsMatched != null) {
            props = propsMatched;
        }
        return props;
    }
}
