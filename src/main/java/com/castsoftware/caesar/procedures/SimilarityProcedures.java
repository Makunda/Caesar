package com.castsoftware.caesar.procedures;

import com.castsoftware.caesar.controllers.SimilarityController;
import com.castsoftware.caesar.database.Neo4jAL;
import com.castsoftware.caesar.exceptions.ProcedureException;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jBadRequestException;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jConnectionError;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jQueryException;
import com.castsoftware.caesar.exceptions.workspace.MissingWorkspaceException;
import com.castsoftware.caesar.results.OutputMessage;
import com.castsoftware.caesar.workspace.Workspace;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.nio.file.Path;
import java.util.stream.Stream;

public class SimilarityProcedures {

	@Context
	public GraphDatabaseService db;

	@Context public Transaction transaction;

	@Context public Log log;

	@Procedure(value = "caesar.similarity.transactions", mode = Mode.WRITE)
	@Description("CALL caesar.similarity.transactions(String source, String target, String path) - Check transaction similarity")
	public void simiCheckTransaction(@Name(value = "Source") String source, @Name(value = "Target") String target ,@Name(value = "Path") String path) throws ProcedureException {

		try {
			Neo4jAL nal = new Neo4jAL(db, transaction, log);

			SimilarityController simControl = new SimilarityController(nal, source, target, path);
			simControl.run();

		} catch (Exception | Neo4jConnectionError | Neo4jQueryException e) {
			ProcedureException ex = new ProcedureException(e);
			log.error("An error occurred while executing the procedure: caesar.similarity.transactions", e);
			throw ex;
		}
	}

}
