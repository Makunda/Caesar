package com.castsoftware.caesar.services.pivot;

import com.castsoftware.caesar.configuration.Language;
import com.castsoftware.caesar.database.Neo4jAL;

public class PivotFactory {

	/**
	 * Get the pivot generator
	 * @param language Language
	 * @param neo4jAL Neo4j Access Layer
	 * @param application Name of the application
	 * @return
	 */
	public static APivot getPivotGen(Language language, Neo4jAL neo4jAL, String application) throws Exception {
		switch (language) {
			case JAVA:
				return new JavaPivot(neo4jAL, application);
			case NET:
			default:
				throw new Exception(String.format("Pivot generator is not implemented for language '%s'", language));
		}
	}
}
