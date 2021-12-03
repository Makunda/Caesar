package com.castsoftware.caesar.services.pivot;

import com.castsoftware.caesar.database.Neo4jAL;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jBadRequestException;
import org.neo4j.graphdb.Node;

import java.util.List;

/**
 * Abstract class to find microservices pivots
 */
public abstract class APivot {
	protected Neo4jAL neo4jAL;
	protected String application;

	/**
	 * Get the list of pivots
	 * @return
	 */
	public abstract List<Node> getPivots() throws Neo4jBadRequestException;

	/**
	 * Abstract constructor getting common parameters
	 * @param neo4jAL Neo4j Access Layer
	 * @param application Application
	 */
	public APivot(Neo4jAL neo4jAL, String application) {
		this.neo4jAL = neo4jAL;
		this.application = application;
	}
}
