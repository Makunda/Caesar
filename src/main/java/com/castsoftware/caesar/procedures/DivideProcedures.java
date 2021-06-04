package com.castsoftware.caesar.procedures;

import com.castsoftware.caesar.controllers.DivideController;
import com.castsoftware.caesar.database.Neo4jAL;
import com.castsoftware.caesar.exceptions.ProcedureException;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jConnectionError;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jNoResult;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jQueryException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.List;
import java.util.stream.Stream;

public class DivideProcedures {

	@Context
	public GraphDatabaseService db;

	@Context public Transaction transaction;

	@Context public Log log;

	@Procedure(value = "caesar.divide.level.per.transaction", mode = Mode.WRITE)
	@Description("caesar.divide.level.per.transaction(String application, String levelName) - Break one level in the application")
	public void divideLevelTransaction(@Name(value = "Application") String application,
									   @Name(value = "LevelName") String levelName) throws ProcedureException {

		try {
			Neo4jAL nal = new Neo4jAL(db, transaction, log);
			DivideController dc = new DivideController(nal, application, levelName);
			dc.run();
		} catch (Exception | Neo4jConnectionError | Neo4jQueryException | Neo4jNoResult e) {
			ProcedureException ex = new ProcedureException(e);
			log.error("An error occurred while executing the procedure", e);
			throw ex;
		}
	}


}
