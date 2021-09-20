package apoc.path;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.helpers.collection.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

import static apoc.path.PathExplorer.PIPE_SEPARATOR;
import static apoc.path.PropertyMatcher.LABEL_TYPE_PATTERN;
import static apoc.path.PropertyMatcher.getPropsMatched;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

/**
 * helper class parsing relationship types and directions
 */

public abstract class RelationshipTypeAndDirections {

	public static final char BACKTICK = '`';

    public static String format(Pair<RelationshipType, Direction> typeAndDirection) {
        String type = typeAndDirection.first().name();
        switch (typeAndDirection.other()) {
            case OUTGOING:
                return type + ">";
            case INCOMING:
                return "<" + type;
            default:
                return type;
        }
    }

	public static List<Triple<String, Direction, String>> parseTriple(String pathFilter, String relPropFilter) {
		List<Triple<String, Direction, String>> relsAndDirs = new ArrayList<>();
		if (pathFilter == null) {
			relsAndDirs.add(Triple.of(null, BOTH, null));
		} else {
			String[] defs = pathFilter.split(PIPE_SEPARATOR);
			for (String def : defs) {
				final Matcher regExMatcher = LABEL_TYPE_PATTERN.matcher(def);
				String props = relPropFilter;
				if (regExMatcher.matches()) {
					def = regExMatcher.group("labelOrType");
					props = getPropsMatched(regExMatcher, props);
				}
				relsAndDirs.add(Triple.of(getRelName(def), directionFor(def), props));
			}
		}
		return relsAndDirs;
	}

	// todo - maybe is worth to use parseTriple() with props for all methods called by this parse(). Evaluate another separated issue
	public static List<Pair<RelationshipType, Direction>> parse(String pathFilter) {
		List<Pair<RelationshipType, Direction>> relsAndDirs = new ArrayList<>();
		if (pathFilter == null) {
			relsAndDirs.add(Pair.of(null, BOTH));
		} else {
			String[] defs = pathFilter.split("\\|");
			for (String def : defs) {
				relsAndDirs.add(Pair.of(relationshipTypeFor(def), directionFor(def)));
			}
		}
		return relsAndDirs;
	}
// todo - con regex da cambiare
	public static Direction directionFor(String type) {
		if (type.contains("<")) return INCOMING;
		if (type.contains(">")) return OUTGOING;
		return BOTH;
	}

	// todo - forse non serve... però replaceAll("[<>:]", ""); mi sa di sì
	public static RelationshipType relationshipTypeFor(String name) {
		name = getRelName(name);
		return getRelationshipType(name);
	}

	public static RelationshipType getRelationshipType(String name) {
		return name == null ? null : RelationshipType.withName(name); // todo - forse non serve questa funzione
	}

	private static String getRelName(String name) {
		if (name.indexOf(BACKTICK) > -1) {
			name = name.substring(name.indexOf(BACKTICK) + 1, name.lastIndexOf(BACKTICK));
		} else {
			name = name.replaceAll("[<>:]", "");
		}
		return StringUtils.isBlank(name) ? null : name;
	}
}
