package com.castsoftware.caesar.procedures;

import com.castsoftware.caesar.controllers.ClassifyController;
import com.castsoftware.caesar.database.Neo4jAL;
import com.castsoftware.caesar.entities.transactions.ClusterTransaction;
import com.castsoftware.caesar.exceptions.ProcedureException;
import com.castsoftware.caesar.exceptions.file.FileCorruptedException;
import com.castsoftware.caesar.exceptions.file.MissingFileException;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jBadRequestException;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jConnectionError;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jQueryException;
import com.castsoftware.caesar.exceptions.workspace.MissingWorkspaceException;
import com.castsoftware.caesar.results.ClusterTransactionResult;
import com.castsoftware.caesar.results.TransactionClassifiedResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.List;
import java.util.stream.Stream;

public class ClassifyProcedures {
	@Context
	public GraphDatabaseService db;

	@Context public Transaction transaction;

	@Context public Log log;

	@Procedure(value = "caesar.classify.transactions", mode = Mode.WRITE)
	@Description("CALL caesar.classify.transactions(String application, Long minimumSize) - Check transaction similarity")
	public Stream<TransactionClassifiedResult> classifyTransaction(@Name(value = "Application") String application,
																   @Name(value = "MinimumSize", defaultValue = "30") Long minimumSize) throws ProcedureException {

		try {
			Neo4jAL nal = new Neo4jAL(db, transaction, log);

			ClassifyController microController = new ClassifyController(nal, application);
			return microController.classifyTransaction(minimumSize).stream();

		} catch (Exception | Neo4jConnectionError | Neo4jQueryException | FileCorruptedException | Neo4jBadRequestException | MissingWorkspaceException | MissingFileException e) {
			ProcedureException ex = new ProcedureException(e);
			log.error("An error occurred while executing the procedure: caesar.classify.transactions", e);
			throw ex;
		}
	}

	@Procedure(value = "caesar.classify.transactions.weighted", mode = Mode.WRITE)
	@Description("CALL caesar.classify.transactions.weighted(String application, Long minimumSize) - Check transaction similarity")
	public Stream<ClusterTransactionResult> weightedTransaction(@Name(value = "Application") String application,
																@Name(value = "MinimumSize", defaultValue = "30") Long minimumSize) throws ProcedureException {

		try {
			Neo4jAL nal = new Neo4jAL(db, transaction, log);

			ClassifyController classifyController = new ClassifyController(nal, application);
			return classifyController.weightTransactionCategory(minimumSize).stream().map(ClusterTransactionResult::new);

		} catch (Exception | Neo4jConnectionError | Neo4jQueryException | FileCorruptedException | Neo4jBadRequestException | MissingWorkspaceException | MissingFileException e) {
			ProcedureException ex = new ProcedureException(e);
			log.error("An error occurred while executing the procedure: caesar.classify.transactions.weighted", e);
			throw ex;
		}
	}

}
