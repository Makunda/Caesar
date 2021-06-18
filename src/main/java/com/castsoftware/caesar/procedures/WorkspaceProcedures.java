package com.castsoftware.caesar.procedures;

import com.castsoftware.caesar.controllers.DivideController;
import com.castsoftware.caesar.database.Neo4jAL;
import com.castsoftware.caesar.exceptions.ProcedureException;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jBadRequestException;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jConnectionError;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jNoResult;
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

public class WorkspaceProcedures {

	@Context
	public GraphDatabaseService db;

	@Context public Transaction transaction;

	@Context public Log log;

	@Procedure(value = "caesar.workspace.set", mode = Mode.WRITE)
	@Description("CALL caesar.workspace.set(String workspace) - Set a workspace")
	public Stream<OutputMessage> setWorkspace(@Name(value = "Path") String path) throws ProcedureException {

		try {
			Neo4jAL nal = new Neo4jAL(db, transaction, log);
			Workspace workspace = Workspace.getInstance(nal);
			workspace.setWorkspace(Path.of(path));

			return Stream.of(new OutputMessage(path));
		} catch (Exception | Neo4jConnectionError | Neo4jQueryException | Neo4jBadRequestException | MissingWorkspaceException e) {
			ProcedureException ex = new ProcedureException(e);
			log.error("An error occurred while executing the procedure: caesar.workspace.set", e);
			throw ex;
		}
	}

	@Procedure(value = "caesar.workspace.get", mode = Mode.WRITE)
	@Description("CALL caesar.workspace.get() - Get the workspace path")
	public Stream<OutputMessage> getWorkspace() throws ProcedureException {

		try {
			Neo4jAL nal = new Neo4jAL(db, transaction, log);
			Workspace workspace = Workspace.getInstance(nal);
			String path = workspace.getWorkspace();

			return Stream.of(new OutputMessage(path));
		} catch (Exception | Neo4jConnectionError | Neo4jQueryException | Neo4jBadRequestException | MissingWorkspaceException e) {
			ProcedureException ex = new ProcedureException(e);
			log.error("An error occurred while executing the procedure: caesar.workspace.get", e);
			throw ex;
		}
	}


}
