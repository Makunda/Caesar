package com.castsoftware.caesar.procedures;

import com.castsoftware.caesar.controllers.SimilarityController;
import com.castsoftware.caesar.controllers.TransactionController;
import com.castsoftware.caesar.database.Neo4jAL;
import com.castsoftware.caesar.exceptions.ProcedureException;
import com.castsoftware.caesar.exceptions.file.FileCorruptedException;
import com.castsoftware.caesar.exceptions.file.MissingFileException;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jBadRequestException;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jConnectionError;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jQueryException;
import com.castsoftware.caesar.exceptions.workspace.MissingWorkspaceException;
import com.castsoftware.caesar.results.DoubleResult;
import com.castsoftware.caesar.results.TransactionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.List;
import java.util.stream.Stream;

/**
 * Microservice procedures
 */
public class TransactionProcedures {

	@Context
	public GraphDatabaseService db;

	@Context public Transaction transaction;

	@Context public Log log;

	@Procedure(value = "caesar.transactions.list", mode = Mode.WRITE)
	@Description("CALL caesar.transactions.list(String application, Optional Long MinimumSize) - Get the list of the transaction in an application")
	public Stream<TransactionResult> getTransactionList(@Name(value = "Application") String application,
														@Name(value = "MinimumSize", defaultValue = "30") Long minimumSize) throws ProcedureException {

		try {
			Neo4jAL nal = new Neo4jAL(db, transaction, log);

			TransactionController transactionController = new TransactionController(nal, application);
			List<com.castsoftware.caesar.entities.transactions.Transaction> transactionList = transactionController.getTransactions(minimumSize.intValue());

			return transactionList.stream().map(TransactionResult::new);
		} catch (Exception | Neo4jConnectionError | Neo4jQueryException | Neo4jBadRequestException | FileCorruptedException | MissingWorkspaceException | MissingFileException e) {
			ProcedureException ex = new ProcedureException(e);
			log.error("An error occurred while executing the procedure: caesar.transactions.list", e);
			throw ex;
		}
	}

	@Procedure(value = "caesar.transactions.average.uniqueness", mode = Mode.WRITE)
	@Description("CALL caesar.transactions.average.uniqueness(String application, Optional Long MinimumSize) - Get the list of the transaction in an application")
	public Stream<DoubleResult> getAverageUniqueness(@Name(value = "Application") String application,
													 @Name(value = "MinimumSize", defaultValue = "30") Long minimumSize) throws ProcedureException {

		try {
			Neo4jAL nal = new Neo4jAL(db, transaction, log);

			TransactionController transactionController = new TransactionController(nal, application);
			Double average = transactionController.getAverageUniqueness(minimumSize.intValue());

			return Stream.of(new DoubleResult(average));
		} catch (Exception | Neo4jConnectionError | Neo4jQueryException | Neo4jBadRequestException | FileCorruptedException | MissingWorkspaceException | MissingFileException e) {
			ProcedureException ex = new ProcedureException(e);
			log.error("An error occurred while executing the procedure: caesar.transactions.average.uniqueness", e);
			throw ex;
		}
	}

}
