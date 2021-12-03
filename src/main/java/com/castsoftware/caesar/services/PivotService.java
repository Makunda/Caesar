package com.castsoftware.caesar.services;

import com.castsoftware.caesar.configuration.Language;
import com.castsoftware.caesar.database.Neo4jAL;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jBadRequestException;
import com.castsoftware.caesar.services.pivot.APivot;
import com.castsoftware.caesar.services.pivot.PivotFactory;
import org.neo4j.graphdb.Node;

import java.util.List;

public class PivotService {

	protected Neo4jAL neo4jAL;
	protected String application;

	/**
	 * Get the list of Pivot pointless on the application language
	 * @return The list of pivot points in the application
	 * @throws Exception If the Pivot generator doesn't exists for thi language
	 * @throws Neo4jBadRequestException If the request to find the pivot failed
	 */
	public List<Node> getPivots() throws Exception, Neo4jBadRequestException {
		APivot pivot = PivotFactory.getPivotGen(Language.JAVA, neo4jAL, application);
		return pivot.getPivots();
	}

	/**
	 * Pilot service
	 * @param neo4jAL Neo4j Access layer
	 * @param application Application name
	 */
	public PivotService(Neo4jAL neo4jAL, String application) {
		this.neo4jAL = neo4jAL;
		this.application = application;

		// TODO : Find the language of the application
	}


}
