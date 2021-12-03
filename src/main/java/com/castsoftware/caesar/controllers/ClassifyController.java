package com.castsoftware.caesar.controllers;

import com.castsoftware.caesar.configuration.DetectionConfiguration;
import com.castsoftware.caesar.database.Neo4jAL;
import com.castsoftware.caesar.entities.transactions.ClusterTransaction;
import com.castsoftware.caesar.entities.transactions.Transaction;
import com.castsoftware.caesar.exceptions.file.FileCorruptedException;
import com.castsoftware.caesar.exceptions.file.MissingFileException;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jBadRequestException;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jQueryException;
import com.castsoftware.caesar.exceptions.workspace.MissingWorkspaceException;
import com.castsoftware.caesar.results.TransactionClassifiedResult;
import com.castsoftware.caesar.sdk.Transactions;
import com.castsoftware.caesar.services.transaction.TransactionClassifyService;
import com.castsoftware.caesar.workspace.Workspace;
import org.neo4j.graphdb.Node;

import java.io.IOException;
import java.util.*;

public class ClassifyController {

	private final static String INHERIT_LINK = "INHERIT";

	private final Neo4jAL neo4jAL;
	private final String application;
	private final List<String> linksToConsider;
	private final DetectionConfiguration configuration;

	/**
	 * Classify the transactions
	 * @param minSize Minimum size of the transaction
	 * @return Classified transaction
	 * @throws Neo4jBadRequestException
	 */
	public List<TransactionClassifiedResult> classifyTransaction(Long minSize) throws Neo4jBadRequestException {
		TransactionClassifyService tns = new TransactionClassifyService();

		Long count = Transactions.getTransactionsCount(neo4jAL, application, minSize.intValue());
		List<Node> transactions = Transactions.getTransactions(neo4jAL, application, minSize.intValue());
		List<TransactionClassifiedResult> transactionList = new ArrayList<>();

		int it = 0;
		int error = 0;
		Transaction transaction;

		for(Node tn : transactions) {
			neo4jAL.logInfo(String.format("Fetching transaction : %d on %d. [Error: %d]", it, count, error));
			it ++;

			try {
				transaction = Transactions.getTransactionFromNode(neo4jAL, tn);
				List<String> categories = tns.classifyTransaction(transaction);
				transactionList.add(new TransactionClassifiedResult(transaction, categories)); // Add to return
			} catch (Exception | Neo4jBadRequestException e) {
				error ++;
				neo4jAL.logError(String.format("Failed to get transaction [%d] insights.", tn.getId()));
			}

		}

		return transactionList;
	}


	/**
	 * Compute the frequency of each category, along with the uniqueness and the number of object
	 * @return
	 */
	public List<ClusterTransaction> weightTransactionCategory(Long minSize) throws Neo4jBadRequestException, Exception {
		TransactionClassifyService transactionService = new TransactionClassifyService();
		Map<String, ClusterTransaction> transactionMap = new HashMap<>();

		Long count = Transactions.getTransactionsCount(neo4jAL, application, minSize.intValue());
		List<Node> transactions = Transactions.getTransactions(neo4jAL, application, minSize.intValue());

		int it = 0;

		// Break transaction and sort them by categories
		for(Node transactionNode : transactions) {
			neo4jAL.logInfo(String.format("Fetching transaction : %d on %d", it, count));
			it ++;
			Transaction tn = Transactions.getTransactionFromNode(neo4jAL, transactionNode);
			// Find categories
			List<String> categories = transactionService.classifyTransaction(tn);
			for(String cat : categories) {
				// Declare new cluster if it doesn't exist
				transactionMap.putIfAbsent(cat, new ClusterTransaction(cat));
				transactionMap.get(cat).addTransaction(tn);
			}
		}

		neo4jAL.logInfo(String.format("%d clusters were identified during the process", transactionMap.size()));

		// Compute metrics on set of transaction
		List<ClusterTransaction> returnList = new ArrayList<>();
		for(ClusterTransaction ct : transactionMap.values()) {
			ct.computeSizeMetrics(neo4jAL);
			returnList.add(ct);
		}

		return returnList;
	}

	/**
	 * Constructor
	 * @param application Name of the application
	 */
	public ClassifyController(Neo4jAL neo4jAL, String application) throws FileCorruptedException, Neo4jBadRequestException, IOException, Neo4jQueryException, MissingWorkspaceException, MissingFileException {
		this.neo4jAL = neo4jAL;
		this.application = application;
		this.linksToConsider = new ArrayList<>();
		this.configuration = Workspace.getInstance(neo4jAL).getConfiguration();
	}
}
