package com.castsoftware.caesar.entities.transactions;

import com.castsoftware.caesar.database.Neo4jAL;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jBadRequestException;
import com.castsoftware.caesar.sdk.Transactions;
import org.neo4j.graphdb.Node;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ClusterTransaction {

	// Mandatory
	private String name;
	private Long objectSize;
	private List<Long> transactionsId;

	private Set<Node> entrypoints;
	private Set<Node> endPoints;

	private Double uniqueness = null;

	public String getName() {
		return name;
	}

	public Long getObjectSize() {
		return objectSize;
	}

	public List<Long> getTransactionsId() {
		return transactionsId;
	}

	public Set<Node> getEntrypoints() {
		return entrypoints;
	}

	public Set<Node> getEndPoints() {
		return endPoints;
	}

	public Double getUniqueness() {
		return uniqueness;
	}

	/**
	 * Add a transaction to the cluster
	 * @param tn
	 */
	public void addTransaction(Transaction tn) {
		this.transactionsId.add(tn.getId());

		this.endPoints.addAll(tn.getEndPoints());
		this.entrypoints.add(tn.getEntrypoint());
	}

	/**
	 * Get the size of the cluster and its uniqueness
	 * @param neo4jAL Neo4j Access Layer
	 * @return The uniqueness of the cluster
	 * @throws Neo4jBadRequestException
	 */
	public Double computeSizeMetrics(Neo4jAL neo4jAL) throws Neo4jBadRequestException {
		// Get size metrics
		this.objectSize = Transactions.getTransactionsSize(neo4jAL, this.transactionsId);
		Long sharedObjects = Transactions.getNumberSharedObjects(neo4jAL, this.transactionsId);

		this.uniqueness = 1 - (double) sharedObjects / this.objectSize;
		return this.uniqueness;
	}

	/**
	 * Constructor
	 * @param name Name of the cluster
	 */
	public ClusterTransaction(String name) {
		this.name = name;
		this.objectSize = 0L;
		this.transactionsId = new ArrayList<>();

		this.entrypoints = new HashSet<>();
		this.endPoints = new HashSet<>();

		this.uniqueness = 0.0;
	}
}
