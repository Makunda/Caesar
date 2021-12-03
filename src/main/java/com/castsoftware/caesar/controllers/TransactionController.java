package com.castsoftware.caesar.controllers;

import com.castsoftware.caesar.database.Neo4jAL;
import com.castsoftware.caesar.entities.transactions.ClusterTransaction;
import com.castsoftware.caesar.entities.transactions.Transaction;
import com.castsoftware.caesar.exceptions.file.FileCorruptedException;
import com.castsoftware.caesar.exceptions.file.MissingFileException;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jBadRequestException;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jQueryException;
import com.castsoftware.caesar.exceptions.workspace.MissingWorkspaceException;
import com.castsoftware.caesar.sdk.Transactions;
import com.castsoftware.caesar.services.transaction.TransactionClassifyService;
import org.neo4j.graphdb.Node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransactionController {
	private final Neo4jAL neo4jAL;
	private final String application;

	/**
	 * Get the list of transaction, with details
	 * @param minSize minimum size of the transaction
	 * @return
	 */
	public List<Transaction> getTransactions(int minSize) throws Neo4jBadRequestException, Exception {
		Long count = Transactions.getTransactionsCount(neo4jAL, application, minSize);
		List<Node> transactions = Transactions.getTransactions(neo4jAL, application, minSize);
		List<Transaction> transactionList = new ArrayList<>();

		int it = 0;
		int error = 0;
		Transaction transaction;

		for(Node tn : transactions) {
			neo4jAL.logInfo(String.format("Fetching transaction : %d on %d. [Error: %d]", it, count, error));
			it ++;

			try {
				transaction = Transactions.getTransactionFromNode(neo4jAL, tn);
				transactionList.add(transaction); // Add to return
			} catch (Exception e) {
				error ++;
				neo4jAL.logError(String.format("Failed to get transaction [%d] insights.", tn.getId()));
			}

		}

		return transactionList;
	}


	/**
	 * Get the list if transaction with their level of uniqueness
	 * @param minSize minimum size of the transaction
	 * @return The average of transaction uniqueness in the application
	 */
	public double getAverageUniqueness(int minSize) throws Neo4jBadRequestException, Exception {
		List<Node> transactions = Transactions.getTransactions(neo4jAL, application);
		List<Double> uniquenessList = new ArrayList<>();

		Transaction transaction;
		for(Node tn : transactions) {
			transaction = Transactions.getTransactionFromNode(neo4jAL, tn);
			uniquenessList.add(transaction.getUniqueness());
		}

		return uniquenessList.stream().mapToDouble(d -> d)
				.average()
				.orElse(0.0);
	}


	/**
	 * Constructor
	 * @param application Name of the application
	 */
	public TransactionController(Neo4jAL neo4jAL, String application) throws FileCorruptedException, Neo4jBadRequestException, IOException, Neo4jQueryException, MissingWorkspaceException, MissingFileException {
		this.neo4jAL = neo4jAL;
		this.application = application;
	}

}
