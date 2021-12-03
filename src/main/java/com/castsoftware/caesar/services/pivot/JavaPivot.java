package com.castsoftware.caesar.services.pivot;

import com.castsoftware.caesar.configuration.Language;
import com.castsoftware.caesar.database.Neo4jAL;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jBadRequestException;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jQueryException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Pivot in java / JEE
 */
public class JavaPivot extends APivot{

	private final Language language = Language.JAVA;
	private List<String> types = List.of("Java Class");

	@Override
	public List<Node> getPivots() throws Neo4jBadRequestException {
		String req = String.format("MATCH (o:Object:`%s`) WHERE o.Type IN $types " +
						"AND o.Name ENDS WITH 'Controller' " +
						"RETURN DISTINCT o as object", application);

		try {
			Result res = neo4jAL.executeQuery(req, Map.of("types", types));
			List<Node> returnList = new ArrayList<>();

			// Iterate over results
			while(res.hasNext()) {
				returnList.add((Node) res.next().get("object"));
			}

			return returnList;
		}  catch (Neo4jQueryException e) {
			String message = String.format("Failed to get Pivots in application '%s' for language '%s'",
					application, language);
			throw new Neo4jBadRequestException(message, e, "JAVPxxGETP1");
		}
	}

	/**
	 * Java pivot
	 * @param neo4jAL Neo4j Access Layer
	 * @param application Application
	 */
	public JavaPivot(Neo4jAL neo4jAL, String application) {
		super(neo4jAL, application);
	}
}
