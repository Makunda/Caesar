package com.castsoftware.caesar.entities.transactions;

import com.castsoftware.caesar.database.Neo4jAL;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jBadRequestException;
import com.castsoftware.caesar.sdk.Transactions;
import org.neo4j.graphdb.Node;

import java.util.*;

public class ClusterTransaction {

	// Optional
	private String id = "";
	private String parent = "";

	// Mandatory
	private String name;
	private Long objectSize;
	private List<Long> transactionsId;

	private Set<Node> entryPoints;
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

	public Set<Node> getEntryPoints() {
		return entryPoints;
	}

	public Set<Node> getEndPoints() {
		return endPoints;
	}

	public Double getUniqueness() {
		return uniqueness;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getParent() {
		return parent;
	}

	public void setParent(String parent) {
		this.parent = parent;
	}

	/**
	 * Add a transaction to the cluster
	 * @param tn
	 */
	public void addTransaction(Transaction tn) {
		this.transactionsId.add(tn.getId());

		this.endPoints.addAll(tn.getEndPoints());
		this.entryPoints.add(tn.getEntrypoint());
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
		long sharedObjects = Transactions.getNumberSharedObjects(neo4jAL, this.transactionsId);

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

		this.entryPoints = new HashSet<>();
		this.endPoints = new HashSet<>();

		this.uniqueness = 0.0;
	}
}
