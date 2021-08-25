package apoc.path;

import apoc.util.TestUtil;
import apoc.util.Util;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExpandPathTest {

    @ClassRule
	public static DbmsRule db = new ImpermanentDbmsRule();

	public ExpandPathTest() throws Exception {
	}  
	
	@BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, PathExplorer.class);
        String movies = Util.readResourceFile("movies.cypher");
		String bigbrother = "MATCH (per:Person) MERGE (bb:BigBrother {name : 'Big Brother' })  MERGE (bb)-[:FOLLOWS]->(per)";
		 try (Transaction tx = db.beginTx()) {
			tx.execute(movies);
			tx.execute(bigbrother);
			tx.commit();
		 }
    }

    @After
    public void removeOtherLabels() {
		db.executeTransactionally("OPTIONAL MATCH (c:Western) REMOVE c:Western WITH DISTINCT 1 as ignore OPTIONAL MATCH (c:Blacklist) REMOVE c:Blacklist");
	}

	@Test
	public void testExplorePathAnyRelTypeTest() throws Throwable {
		TestUtil.testCall(db,
				"MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expand(m,'>','',0,2) yield path return count(*) as c",
				(row) -> assertEquals(1L,row.get("c")));

		TestUtil.testCall(db,
				"MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expand(m,'<','',0,2) yield path return count(*) as c",
				(row) -> assertEquals(17L,row.get("c")));
		TestUtil.testCall(db,
				"MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expand(m,'','',0,2) yield path return count(*) as c",
				(row) -> assertEquals(52L,row.get("c")));
		TestUtil.testCall(db,
				"MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expand(m,null,'',0,2) yield path return count(*) as c",
				(row) -> assertEquals(52L,row.get("c")));
	}

	@Test
	public void testExplorePathRelationshipsTest() throws Throwable {
		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expand(m,'<ACTED_IN|PRODUCED>|FOLLOWS','',0,2) yield path return count(*) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(11L,row.get("c")));
	}

	@Test
	public void testExplorePathLabelWhiteListTest() throws Throwable {
		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expand(m,'ACTED_IN|PRODUCED|FOLLOWS','+Person|+Movie',0,3) yield path return count(*) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(107L,row.get("c"))); // 59 with Uniqueness.RELATIONSHIP_GLOBAL
	}

	@Test
	public void testExplorePathLabelWhiteListTestAndPropFilterMatched() throws Throwable {
		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expand(m,'ACTED_IN|PRODUCED|FOLLOWS','+Person{+name}|+Movie',0,3) yield path return count(*) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(107L,row.get("c")));
	}

	@Test
	public void testExplorePathLabelWhiteListTestAndPropFilterNotMatched() throws Throwable { 
		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expand(m,'ACTED_IN|PRODUCED|FOLLOWS','+Person{-name}|+Movie',0,3) yield path return path";
		TestUtil.testCall(db, query, (row) -> {
			final List<Node> nodes = Iterables.asList(((Path) row.get("path")).nodes());
			assertEquals(1L, nodes.size());
			assertEquals("The Matrix", nodes.get(0).getProperty("title"));
		});
	}
	
	@Test
	public void testExplorePathLabelWhiteListTestAndPropFilterNotMatched2() throws Throwable { // todo - dovrebbe spaccare forse.. però Movie non viene vista come label
		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expand(m,'ACTED_IN|PRODUCED|FOLLOWS','+Person|+Movie{-title}',0,3) yield path return count(*) as c";
		TestUtil.testCall(db, query, (row) -> {
			assertEquals(7L,row.get("c"));
//			final List<Node> nodes = Iterables.asList(((Path) row.get("path")).nodes());
//			assertEquals(1L, nodes.size());
//			assertEquals("The Matrix", nodes.get(0).getProperty("title"));
		});
	}

	// todo - questo è il test con asterisco
	@Test
	public void testExplorePathLabelBlackListTest111() {
		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expand(m,'ACTED_IN|PRODUCED|FOLLOWS','*{-title}',0,3) yield path return count(*) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(7,row.get("c")));
	}
	@Test
	public void testExplorePathLabelBlackListTest111111111() {
		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expand(m,'ACTED_IN|PRODUCED|FOLLOWS','*',0,3) yield path return count(*) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(7,row.get("c")));
	}
	@Test
	public void testExplorePathLabelBlackListTest11111111dsjnadsfadlskfskjsgunvilnliruerenuirniuvrinvsrinvinreginuriouhgriorgoiu() {
		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expand(m,'ACTED_IN|PRODUCED|FOLLOWS','*',0,2) yield path return count(*) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(32L,row.get("c")));
	}
	
	@Test
	public void testExplorePathLabelBlackListTest11111111dsjnadsfadlskfskjsgunvilnliruerenuirniuvrinvsrinvinreginuriouhgriorgoiuffffffffffffffff() {
		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expand(m,'ACTED_IN|PRODUCED|FOLLOWS','*',0,2) \n" +
				"yield path with nodes(path) as nodes\n" +
				"UNWIND nodes as node\n" +
				"return count(DISTINCT node) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(18L,row.get("c")));
	}
	
	@Test
	public void testExplorePathLabelBlackListTest11111111dsjnadsfadlskfskjsgunvilnliruerenuirnddddddddiuvrinvsrinvinreginuriouhgriorgoiuffffffffffffffff() {
		// exclude Big Brother node
		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expand(m,'ACTED_IN|PRODUCED|FOLLOWS','*{name!=Big Brother}',0,2) \n" +
				"yield path with nodes(path) as nodes unwind nodes as node with node where not node:Movie return node";
		TestUtil.testResult(db, query, (result) -> {
			Iterators.stream(result.<Node>columnAs("node")).forEach(node -> {
				assertEquals(List.of(Label.label("Person")), node.getLabels());
			});
		});
	}
	
	@Test
	public void testMultiWithWildcard() {
		// todo - decommentare e chiamare la variabile "allQuery"
//		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expand(m,null,'*',0,2) yield path return count(*) as c";
//		TestUtil.testCall(db, query, (row) -> assertEquals(52L,row.get("c")));
		
		
		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expand(m,null,'*{+name || +title}',0,2) yield path return count(*) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(52L,row.get("c")));
		
//		
//		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expand(m,null,'*',0,2) yield path return count(*) as c";
//		TestUtil.testCall(db, query, (row) -> assertEquals(52L,row.get("c")));
//		
		
//		TestUtil.testResult(db, query, (result) -> {
//			System.out.println("ExpandPathTest.testMultiWithWildcard");
//			Iterators.stream(result.<Node>columnAs("path")).forEach(node -> {
//				assertEquals(List.of(Label.label("Person")), node.getLabels());
//			});
//		});
	}
	
	// MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expand(m,'ACTED_IN|PRODUCED|FOLLOWS','*{+name | +title}',0,2) 
	//yield path return path
	// --> questa dovrebbe restituirmi tutto!

	@Test
	public void testExplorePathLabelBlackListTest() throws Throwable {
		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expand(m,null,'-BigBrother',0,2) yield path return count(*) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(44L,row.get("c")));
	}

	@Test
	public void testExplorePathWithTerminationLabel() { // todo - fare test con una prop null
		db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

		TestUtil.testResult(db,
				"MATCH (k:Person {name:'Keanu Reeves'}) " +
				"CALL apoc.path.expandConfig(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western', uniqueness: 'NODE_GLOBAL'}) yield path " +
				"return path",
				result -> {

					List<Map<String, Object>> maps = Iterators.asList(result);
					assertEquals(1, maps.size()); // since Gene blocks any path to Clint
					Path path = (Path) maps.get(0).get("path");
					assertEquals("Gene Hackman", path.endNode().getProperty("name"));
				});
	}

	@Test
	public void testExplorePathWithFilterStartNodeFalseIgnoresLabelFilter() throws Throwable {
		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expandConfig(m,{labelFilter:'+Person', maxLevel:2, filterStartNode:false}) yield path return count(*) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(9L,row.get("c")));
	}

	@Test
	public void testExplorePathWithLimitReturnsLimitedResults() {
		db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Christian Bale', 'Tom Cruise'] SET c:Western");

		TestUtil.testResult(db,
				"MATCH (k:Person {name:'Keanu Reeves'}) " +
						"CALL apoc.path.expandConfig(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western', uniqueness: 'NODE_GLOBAL', limit: 2}) yield path " +
						"RETURN nodes(path)[-1].name AS node",
				result -> {
					List<Map<String, Object>> maps = Iterators.asList(result);
					assertEquals(2, maps.size());

					MatcherAssert.assertThat(maps, Matchers.hasItem(
							MapUtil.map(
									"node", "Tom Cruise"
							)));

					MatcherAssert.assertThat(maps, Matchers.hasItem(
							MapUtil.map(
									"node", "Clint Eastwood"
							)));
				});
	}

	@Test
	public void testExplorePathWithEndNodeLabel() {
		db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

		TestUtil.testResult(db,
				"MATCH (k:Person {name:'Keanu Reeves'}) " +
						"CALL apoc.path.expandConfig(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'>Western', uniqueness: 'NODE_GLOBAL'}) yield path " +
						"return path",
				result -> {
					List<Map<String, Object>> maps = Iterators.asList(result);
					assertEquals(2, maps.size());
					Path path = (Path) maps.get(0).get("path");
					assertEquals("Gene Hackman", path.endNode().getProperty("name"));
					path = (Path) maps.get(1).get("path");
					assertEquals("Clint Eastwood", path.endNode().getProperty("name"));
				});
	}

	@Test
	public void testExplorePathWithEndNodeLabelAndLimit() {
		db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman', 'Christian Bale'] SET c:Western");

		TestUtil.testResult(db,
				"MATCH (k:Person {name:'Keanu Reeves'}) " +
						"CALL apoc.path.expandConfig(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'>Western', uniqueness: 'NODE_GLOBAL', limit:2}) yield path " +
						"return path",
				result -> {
					List<Map<String, Object>> maps = Iterators.asList(result);
					assertEquals(2, maps.size());
					Path path = (Path) maps.get(0).get("path");
					assertEquals("Gene Hackman", path.endNode().getProperty("name"));
					path = (Path) maps.get(1).get("path");
					assertEquals("Clint Eastwood", path.endNode().getProperty("name"));
				});
	}


	// label filter precedence tests

	@Test
	public void testBlacklistBeforeWhitelist() {
		db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

		TestUtil.testResult(db,
				"MATCH (k:Person {name:'Keanu Reeves'}) " +
						"CALL apoc.path.expandConfig(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'+Person|-Person', uniqueness: 'NODE_GLOBAL', filterStartNode:true}) yield path " +
						"return path",
				result -> {
					List<Map<String, Object>> maps = Iterators.asList(result);
					assertEquals(0, maps.size());
				});
	}

	@Test
	public void testBlacklistBeforeTerminationFilter() {
		db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

		TestUtil.testResult(db,
				"MATCH (k:Person {name:'Keanu Reeves'}) " +
						"CALL apoc.path.expandConfig(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western|-Western', uniqueness: 'NODE_GLOBAL', filterStartNode:false}) yield path " +
						"return path",
				result -> {
					List<Map<String, Object>> maps = Iterators.asList(result);
					assertEquals(0, maps.size());
				});
	}

	@Test
	public void testBlacklistBeforeEndNodeFilter() {
		db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

		TestUtil.testResult(db,
				"MATCH (k:Person {name:'Keanu Reeves'}) " +
						"CALL apoc.path.expandConfig(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'>Western|-Western', uniqueness: 'NODE_GLOBAL', filterStartNode:false}) yield path " +
						"return path",
				result -> {
					List<Map<String, Object>> maps = Iterators.asList(result);
					assertEquals(0, maps.size());
				});
	}

	@Test
	public void testTerminationFilterBeforeWhitelist() {
		db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman', 'Christian Bale'] SET c:Western");

		TestUtil.testResult(db,
				"MATCH (k:Person {name:'Keanu Reeves'}) " +
						"CALL apoc.path.expandConfig(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western|+Movie', uniqueness: 'NODE_GLOBAL', filterStartNode:false}) yield path " +
						"return path",
				result -> {
					List<Map<String, Object>> maps = Iterators.asList(result);
					assertEquals(1, maps.size());
					Path path = (Path) maps.get(0).get("path");
					assertEquals("Gene Hackman", path.endNode().getProperty("name"));
				});
	}

	@Test
	public void testTerminationFilterBeforeEndNodeFilter() {
		db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

		TestUtil.testResult(db,
				"MATCH (k:Person {name:'Keanu Reeves'}) " +
						"CALL apoc.path.expandConfig(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western|>Western', uniqueness: 'NODE_GLOBAL', filterStartNode:false}) yield path " +
						"return path",
				result -> {
					List<Map<String, Object>> maps = Iterators.asList(result);
					assertEquals(1, maps.size());
					Path path = (Path) maps.get(0).get("path");
					assertEquals("Gene Hackman", path.endNode().getProperty("name"));
				});
	}

	@Test
	public void testEndNodeFilterAsWhitelist() {
		db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

		TestUtil.testResult(db,
				"MATCH (k:Person {name:'Keanu Reeves'}) " +
						"CALL apoc.path.expandConfig(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'>Western|+Movie', uniqueness: 'NODE_GLOBAL', filterStartNode:false}) yield path " +
						"return path",
				result -> {
					List<Map<String, Object>> maps = Iterators.asList(result);
					assertEquals(2, maps.size());
					Path path = (Path) maps.get(0).get("path");
					assertEquals("Gene Hackman", path.endNode().getProperty("name"));
					path = (Path) maps.get(1).get("path");
					assertEquals("Clint Eastwood", path.endNode().getProperty("name"));
				});
	}

	@Test
	public void testLimitPlaysNiceWithMinLevel() {
		db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

		TestUtil.testResult(db,
				"MATCH (k:Person {name:'Keanu Reeves'}) " +
						"CALL apoc.path.expandConfig(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'>Western', uniqueness: 'NODE_GLOBAL', limit:1, minLevel:3}) yield path " +
						"return path",
				result -> {
					List<Map<String, Object>> maps = Iterators.asList(result);
					assertEquals(1, maps.size());
					Path path = (Path) maps.get(0).get("path");
					assertEquals("Clint Eastwood", path.endNode().getProperty("name"));
				});
	}

	@Test
	public void testTerminationFilterDoesNotPruneBelowMinLevel() { // todo...
		db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

		TestUtil.testResult(db,
				"MATCH (k:Person {name:'Keanu Reeves'}) " +
						"CALL apoc.path.expandConfig(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western', uniqueness: 'NODE_GLOBAL', minLevel:3}) yield path " +
						"return path",
				result -> {
					List<Map<String, Object>> maps = Iterators.asList(result);
					assertEquals(1, maps.size());
					Path path = (Path) maps.get(0).get("path");
					assertEquals("Clint Eastwood", path.endNode().getProperty("name"));
				});
	}

	@Test
	public void testTerminationFilterDoesNotPruneBelowMinLevel2() { // todo...
		db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

		TestUtil.testCallEmpty(db,
				// todo - così il test non trova niente perché tutti i nodi Western hanno name
				"MATCH (k:Person {name:'Keanu Reeves'}) " +
						"CALL apoc.path.expandConfig(k, {nodePropFilter: {Western: '-name' },  relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western', uniqueness: 'NODE_GLOBAL', minLevel:3}) yield path " +
						"return path", Collections.emptyMap()
//				result -> {
//					List<Map<String, Object>> maps = Iterators.asList(result);
//					assertEquals(1, maps.size());
//					Path path = (Path) maps.get(0).get("path");
//					assertEquals("Clint Eastwood", path.endNode().getProperty("name"));
//				}
				);
	}

	@Test
	public void testTerminationFilterDoesNotPruneBelowMinLevel23() { // todo...
		db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

		TestUtil.testResult(db,
				"MATCH (k:Person {name:'Keanu Reeves'}) " +
						"CALL apoc.path.expandConfig(k, {nodePropFilter: {Western: 'name=Clint Eastwood | name=Gene Hackman | name=Keanu Reeves' },  relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western', uniqueness: 'NODE_GLOBAL', minLevel:3}) yield path " +
						"return path",
				result -> {
					List<Map<String, Object>> maps = Iterators.asList(result);
					assertEquals(1, maps.size());
					Path path = (Path) maps.get(0).get("path");
					assertEquals("Clint Eastwood", path.endNode().getProperty("name"));
				});
	}

	@Test
	public void testTerminationFilterDoesNotPruneBelowMinLevel23WithAnd() { // todo...
		db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

		TestUtil.testResult(db,
				"MATCH (k:Person {name:'Keanu Reeves'}) " +
						"CALL apoc.path.expandConfig(k, {nodePropFilter: {Western: 'name=Clint Eastwood & born=1930 | name=Gene Hackman | name=Keanu Reeves ' },  relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western', uniqueness: 'NODE_GLOBAL', minLevel:3}) yield path " +
						"return path",
				result -> {
					List<Map<String, Object>> maps = Iterators.asList(result);
					assertEquals(1, maps.size());
					Path path = (Path) maps.get(0).get("path");
					assertEquals("Clint Eastwood", path.endNode().getProperty("name"));
				});
	}
	// todo - test con _all_ ed un altro specifico
	
	
	// TODO TODO TODOISSIMO !!! - e con gli array che cazzo succede???
	
	
	// TODO TODO TODOISSIMO !!! - e con i point che cazzo succede???


	// TODO TODO TODOISSIMO !!! - e con le date che cazzo succede???
	
	/// --> "1992-01-01T00:00:00Z"
	// todo - testare tutti i tipi di date
	@Test
	public void testTerminationFilterDoesNotPruneBelowMinLevel23WithAndAndArray() { // todo...
		db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western, c.dateCustom = localdatetime('1992-01-01')");

		TestUtil.testResult(db,
				"MATCH (k:Person {name:'Keanu Reeves'}) " +
						"CALL apoc.path.expandConfig(k, {nodePropFilter: {Western: 'name=Clint Eastwood & born=1930 & dateCustom=1992-01-01 | name=Gene Hackman | name=Keanu Reeves ' },  relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western', uniqueness: 'NODE_GLOBAL', minLevel:3}) yield path " +
						"return path",
				result -> {
					List<Map<String, Object>> maps = Iterators.asList(result);
					assertEquals(1, maps.size());
					Path path = (Path) maps.get(0).get("path");
					assertEquals("Clint Eastwood", path.endNode().getProperty("name"));
				});
	}
	
	@Test
	public void testWithPoint() { // todo...
		db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western, c.dateCustom = localdatetime('1992-01-01'), c.coord = point({latitude: 1, longitude: 2})");

		TestUtil.testResult(db,
				"MATCH (k:Person {name:'Keanu Reeves'}) " +
						"CALL apoc.path.expandConfig(k, {nodePropFilter: {Western: 'name=Clint Eastwood & born=1930 & dateCustom=1992-01-01 & coord=point({srid:4326, x:2, y:1}) | name=Gene Hackman | name=Keanu Reeves ' },  relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western', uniqueness: 'NODE_GLOBAL', minLevel:3}) yield path " +
						"return path",
				result -> {
					List<Map<String, Object>> maps = Iterators.asList(result);
					assertEquals(1, maps.size());
					Path path = (Path) maps.get(0).get("path");
					assertEquals("Clint Eastwood", path.endNode().getProperty("name"));
				});
	}
	
	@Test
	public void testWithPoint11() { // todo...
		db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western, c.dateCustom = localdatetime('1992-01-01'), c.coord = point({latitude: 1, longitude: 2})");

		TestUtil.testCallEmpty(db,
				"MATCH (k:Person {name:'Keanu Reeves'}) " +
						"CALL apoc.path.expandConfig(k, {nodePropFilter: {Western: 'name=Clint Eastwood & born=1930 & dateCustom=1992-01-01 & coord=point({srid:4326, x:2, y:1}) & notExistent=1 | name=Gene Hackman | name=Keanu Reeves ' },  relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western', uniqueness: 'NODE_GLOBAL', minLevel:3}) yield path " +
						"return path",
				Collections.emptyMap());
	}
	
	@Test
	public void testWithPoint11111() { // todo...
		db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western, c.dateCustom = localdatetime('1992-01-01'), c.coord = point({latitude: 1, longitude: 2})");

		TestUtil.testResult(db,
				"MATCH (k:Person {name:'Keanu Reeves'}) " +
						"CALL apoc.path.expandConfig(k, {nodePropFilter: {Western: 'name=Clint Eastwood & born=1930 & dateCustom=1992-01-01 & coord=point({srid:4326, x:2, y:1}) | notExistent=1 | name=Gene Hackman | name=Keanu Reeves ' },  relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western', uniqueness: 'NODE_GLOBAL', minLevel:3}) yield path " +
						"return path",
				result -> {
					List<Map<String, Object>> maps = Iterators.asList(result);
					assertEquals(1, maps.size());
					Path path = (Path) maps.get(0).get("path");
					assertEquals("Clint Eastwood", path.endNode().getProperty("name"));
				});
	}
	
	// todo -> vale la pena fare >= e <= ?
	
	@Test
	public void testWithGreaterThan() { // todo... cambiare
		db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western, c.dateCustom = localdatetime('1992-01-01'), c.coord = point({latitude: 1, longitude: 2})");

		TestUtil.testResult(db,
				"MATCH (k:Person {name:'Keanu Reeves'}) " +
						"CALL apoc.path.expandConfig(k, {nodePropFilter: {Western: 'dateCustom>1991-01-01' },  relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western', uniqueness: 'NODE_GLOBAL', minLevel:3}) yield path " +
						"return path",
				result -> {
					List<Map<String, Object>> maps = Iterators.asList(result);
					assertEquals(1, maps.size());
					Path path = (Path) maps.get(0).get("path");
					assertEquals("Clint Eastwood", path.endNode().getProperty("name"));
				});
		
		TestUtil.testCallEmpty(db,
				"MATCH (k:Person {name:'Keanu Reeves'}) " +
						"CALL apoc.path.expandConfig(k, {nodePropFilter: {Western: 'born>1940 & dateCustom=1992-01-01| name=Gene Hackman | name=Keanu Reeves ' },  relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western', uniqueness: 'NODE_GLOBAL', minLevel:3}) yield path " +
						"return path",
				Collections.emptyMap());
	}
	
	@Test
	public void testWithGreaterThan1111() { // todo...
		db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western, c.dateCustom = localdatetime('1992-01-01'), c.coord = point({latitude: 1, longitude: 2})");

		TestUtil.testResult(db,
				"MATCH (k:Person {name:'Keanu Reeves'}) " +
						"CALL apoc.path.expandConfig(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western{dateCustom>1991-01-01}', uniqueness: 'NODE_GLOBAL', minLevel:3}) yield path " +
						"return path",
				result -> {
					List<Map<String, Object>> maps = Iterators.asList(result);
					assertEquals(1, maps.size());
					Path path = (Path) maps.get(0).get("path");
					assertEquals("Clint Eastwood", path.endNode().getProperty("name"));
				});
		
//		TestUtil.testCallEmpty(db,
//				"MATCH (k:Person {name:'Keanu Reeves'}) " +
//						"CALL apoc.path.expandConfig(k, {nodePropFilter: {Western: 'born>1940 & dateCustom=1992-01-01| name=Gene Hackman | name=Keanu Reeves ' },  relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western', uniqueness: 'NODE_GLOBAL', minLevel:3}) yield path " +
//						"return path",
//				Collections.emptyMap());
	}
	
	@Test
	public void testExpandWithSequenceAndPropFilter() {
		// todo - decommentare, va bene
//		TestUtil.testCallCount(db,
//				"MATCH (m:Movie {title: 'The Matrix'}) \n" +
//						"CALL apoc.path.expandConfig(m,{sequence: 'Movie,ACTED_IN,Person,ACTED_IN,>*', maxLevel: 2})   \n" +
//						"yield path return path", 14);
		
		// todo - decommentare, va bene
//		TestUtil.testCall(db,
//				"MATCH (m:Movie {title: 'The Matrix'}) \n" +
//						"CALL apoc.path.expandConfig(m,{sequence: \"Movie,ACTED_IN,Person,ACTED_IN,>*{title=The Devil's Advocate}\", maxLevel: 2})   \n" +
//						"yield path return path", row -> {
//					final Path path = (Path) row.get("path");
//					assertEquals(2, path.length());
//					assertEquals("The Matrix", path.startNode().getProperty("title"));
//					assertEquals("The Devil's Advocate", path.endNode().getProperty("title"));
//				});

		// todo - decommentare, va bene
//		TestUtil.testCall(db,
//				"MATCH (m:Movie {title: 'The Matrix'}) \n" +
//						"CALL apoc.path.expandConfig(m,{sequence: 'Movie,ACTED_IN,Person,ACTED_IN{roles=Bill Smoke,Haskell Moore,Tadeusz Kesselring,Nurse Noakes,Boardman Mephi,Old Georgie},>*', maxLevel: 2})   \n" +
//						"yield path return path", row -> {
//					final Path path = (Path) row.get("path");
//					assertEquals(2, path.length());
//					assertEquals("The Matrix", path.startNode().getProperty("title"));
//					assertEquals("Cloud Atlas", path.endNode().getProperty("title"));
//				});
		
		// list of numbers
		db.executeTransactionally("MATCH ({name:'Hugo Weaving'})-[r]->({title: 'Cloud Atlas'}) set r.listNum = [123,456,789]");
		
		TestUtil.testCall(db,
				"MATCH (m:Movie {title: 'The Matrix'}) \n" +
						"CALL apoc.path.expandConfig(m,{sequence: 'Movie,ACTED_IN,Person,ACTED_IN{listNum=123,456,789},>*', maxLevel: 2})   \n" +
						"yield path return path", row -> {
					final Path path = (Path) row.get("path");
					assertEquals(2, path.length());
					assertEquals("The Matrix", path.startNode().getProperty("title"));
					assertEquals("Cloud Atlas", path.endNode().getProperty("title"));
				});
	}
// todo - test sequence con filter in relationship e array --> roles:Bill Smoke,Haskell Moore,Tadeusz Kesselring,Nurse Noakes,Boardman Mephi,Old Georgie


	@Test
	public void testRelPropFilter() { // todo...
		db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western, c.propNull = null"); // todo - serve sto set?

		// todo - decommentare
//		TestUtil.testResult(db,
//				// todo - deve spaccare perché c'è '-roles'
//				"MATCH (k:Person {name:'Keanu Reeves'}) " +
//						"CALL apoc.path.expandConfig(k, {relPropFilter: {_all_: '+roles'}, nodePropFilter: {Western: 'name=Clint Eastwood | name=Gene Hackman | name=Keanu Reeves' },  relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western', uniqueness: 'NODE_GLOBAL', minLevel:3}) yield path " +
//						"return path",
//				result -> {
//					List<Map<String, Object>> maps = Iterators.asList(result);
//					assertEquals(1, maps.size());
//					Path path = (Path) maps.get(0).get("path");
//					assertEquals("Clint Eastwood", path.endNode().getProperty("name"));
//				});
//		
//		TestUtil.testCallEmpty(db,
//				// todo - deve spaccare perché c'è '-roles'
//				"MATCH (k:Person {name:'Keanu Reeves'}) " +
//						"CALL apoc.path.expandConfig(k, {relPropFilter: {_all_: '-roles'}, nodePropFilter: {Western: 'name=Clint Eastwood | name=Gene Hackman | name=Keanu Reeves' },  relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western', uniqueness: 'NODE_GLOBAL', minLevel:3}) yield path " +
//						"return path",
//				Collections.emptyMap());
//
//		TestUtil.testResult(db,
//				// todo - deve spaccare perché c'è '-roles'
//				"MATCH (k:Person {name:'Keanu Reeves'}) " +
//						"CALL apoc.path.expandConfig(k, {relPropFilter: {PRODUCED: '-roles'}, nodePropFilter: {Western: 'name=Clint Eastwood | name=Gene Hackman | name=Keanu Reeves' },  relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western', uniqueness: 'NODE_GLOBAL', minLevel:3}) yield path " +
//						"return path",
//				result -> {
//					List<Map<String, Object>> maps = Iterators.asList(result);
//					assertEquals(1, maps.size());
//					Path path = (Path) maps.get(0).get("path");
//					assertEquals("Clint Eastwood", path.endNode().getProperty("name"));
//				});

//		TestUtil.testResult(db,
//				// todo - deve spaccare perché c'è '-roles'
//				"MATCH (k:Person {name:'Keanu Reeves'})\n" +
//						"CALL apoc.path.expandConfig(k, {relPropFilter: {ACTED_IN: 'roles!=Bill Munny', DIRECTED: '-anotherProp'}, nodePropFilter: {Western: 'name=Clint Eastwood | name=Gene Hackman | name=Keanu Reeves' },  relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western', uniqueness: 'NODE_GLOBAL', minLevel:3}) " +
//						"yield path return path",
//				result -> {
//					List<Map<String, Object>> maps = Iterators.asList(result);
//					assertEquals(1, maps.size());
//					Path path = (Path) maps.get(0).get("path");
//					assertEquals("Clint Eastwood", path.endNode().getProperty("name"));
//				});

		TestUtil.testResult(db,
				// todo - deve spaccare perché c'è '-roles'
				"MATCH (k:Person {name:'Keanu Reeves'})\n" +
						"CALL apoc.path.expandConfig(k, {relationshipFilter:'ACTED_IN{+roles}|PRODUCED|DIRECTED', labelFilter:'/Western', uniqueness: 'NODE_GLOBAL', minLevel:3}) " +
						"YIELD path RETURN path",
				result -> {
					List<Map<String, Object>> maps = Iterators.asList(result);
					assertEquals(1, maps.size());
					Path path = (Path) maps.get(0).get("path");
					assertEquals("Clint Eastwood", path.endNode().getProperty("name"));
				});
		
		
	}
	
	// todo - testare array di interi
	
	
	
	// todo - testare sequence config
	
	
	// todo - testare con prop null
	
	
	
	
	
	
	
	
	// todo
	// todo - test label con '*'
	

	@Test
	public void testFilterStartNodeFalseDoesNotFilterStartNodeWhenBelowMinLevel() throws Throwable {
		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expandConfig(m,{labelFilter:'+Person', minLevel:1, maxLevel:2, filterStartNode:false}) yield path return count(*) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(8L,row.get("c")));
	}

	@Test
	public void testOptionalExpandConfigWithNoResultsYieldsNull() {
		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expandConfig(m,{labelFilter:'+Agent', minLevel:1, maxLevel:2, filterStartNode:false, optional:true}) YIELD path RETURN path";
		TestUtil.testResult(db, query, (result) -> {
			assertTrue(result.hasNext());
			Map<String, Object> row = result.next();
			assertEquals(null, row.get("path"));
		});
	}

	@Test
	public void testFilterStartNodeDefaultsToFalse() throws Throwable {
		// was default true prior to 3.2.x
		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expandConfig(m,{labelFilter:'+Person'}) yield path return count(*) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(9L,row.get("c")));
	}

	@Test
	public void testCompoundLabelMatchesOnlyNodeWithBothLabels() {  // todo - vedere dove spacca!
		db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western WITH c WHERE c.name = 'Clint Eastwood' SET c:Eastwood");

		TestUtil.testResult(db,
				"MATCH (k:Person {name:'Keanu Reeves'}) " +
						"CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western:Eastwood', uniqueness: 'NODE_GLOBAL'}) yield node " +
						"return node",
				result -> {

					List<Map<String, Object>> maps = Iterators.asList(result);
					assertEquals(1, maps.size());
					Node node = (Node) maps.get(0).get("node");
					assertEquals("Clint Eastwood", node.getProperty("name")); // otherwise Gene would block path to Clint
				});
	}

//	@Test
//	public void testCompoundLabelMatchesOnlyNodeWithBothLabelsAndNodeFilter() {
//		db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western WITH c WHERE c.name = 'Clint Eastwood' SET c:Eastwood");
//
//		TestUtil.testResult(db,
//				"MATCH (k:Person {name:'Keanu Reeves'}) " +
//						"CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western:Eastwood', uniqueness: 'NODE_GLOBAL'}) yield node " +
//						"return node",
//				result -> {
//
//					List<Map<String, Object>> maps = Iterators.asList(result);
//					assertEquals(1, maps.size());
//					Node node = (Node) maps.get(0).get("node");
//					assertEquals("Clint Eastwood", node.getProperty("name")); // otherwise Gene would block path to Clint
//				});
//	}

	@Test
	public void testCompoundLabelWorksInBlacklist() {
		db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western WITH c WHERE c.name = 'Clint Eastwood' SET c:Blacklist");

		TestUtil.testResult(db,
				"MATCH (k:Person {name:'Keanu Reeves'}) " +
						"CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'>Western|-Western:Blacklist', uniqueness: 'NODE_GLOBAL'}) yield node " +
						"return node",
				result -> {

					List<Map<String, Object>> maps = Iterators.asList(result);
					assertEquals(1, maps.size());
					Node node = (Node) maps.get(0).get("node");
					assertEquals("Gene Hackman", node.getProperty("name"));
				});
	}
	
	// todo - testare anche label doppie, tipo "Western:Blacklist"
	
	@Test
	public void testCompoundLabelWorksInBlacklistAndPropFilter() {
		db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western WITH c WHERE c.name = 'Clint Eastwood' SET c:Blacklist");

		TestUtil.testResult(db,
				"MATCH (k:Person {name:'Keanu Reeves'}) " +
						"CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'>Western{+name}|-Western:Blacklist', uniqueness: 'NODE_GLOBAL'}) yield node " +
						"return node",
				result -> {

					List<Map<String, Object>> maps = Iterators.asList(result);
					assertEquals(1, maps.size());
					Node node = (Node) maps.get(0).get("node");
					assertEquals("Gene Hackman", node.getProperty("name"));
				});
	}

	@Test
	public void testRelationshipFilterWorksWithoutTypeOutgoing() {
		TestUtil.testResult(db,
				"MATCH (k:Person {name:'Keanu Reeves'}) " +
						"CALL apoc.path.subgraphNodes(k, {relationshipFilter:'>', labelFilter:'>Movie', uniqueness: 'NODE_GLOBAL'}) yield node " +
						"return collect(node.title) as titles",
				result -> {

					List<String> expectedTitles = new ArrayList<>(Arrays.asList("Something's Gotta Give", "Johnny Mnemonic", "The Replacements", "The Devil's Advocate", "The Matrix Revolutions", "The Matrix Reloaded", "The Matrix"));
					List<Map<String, Object>> maps = Iterators.asList(result);
					assertEquals(1, maps.size());
					List<String> titles = (List<String>) maps.get(0).get("titles");
					assertEquals(7, titles.size());
					assertTrue(titles.containsAll(expectedTitles));
				});
	}

	@Test
	public void testRelationshipFilterWorksWithoutTypeIncoming() {
		TestUtil.testResult(db,
				"MATCH (k:Person {name:'Keanu Reeves'}) " +
						"CALL apoc.path.subgraphNodes(k, {relationshipFilter:'<', labelFilter:'>BigBrother', uniqueness: 'NODE_GLOBAL'}) yield node " +
						"return node",
				result -> {

					List<Map<String, Object>> maps = Iterators.asList(result);
					assertEquals(1, maps.size());
				});
	}
}
