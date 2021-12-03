package com.castsoftware.caesar.entities.transactions;

import com.castsoftware.caesar.database.Neo4jAL;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jBadRequestException;
import com.castsoftware.caesar.sdk.Transactions;
import org.neo4j.graphdb.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Transaction {
	// Mandatory
	private long id;
	private String name;
	private String fullName;
	private Long size;

	// Optional
	private Node entrypoint = null;
	private List<Node> endPoints = new ArrayList<>();
	private Double uniqueness = null;

	public Long getSize() {
		return size;
	}

	public void setSize(Long size) {
		this.size = size;
	}

	public Double getUniqueness() {
		return uniqueness;
	}

	public void setUniqueness(Double uniqueness) {
		this.uniqueness = uniqueness;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getFullName() {
		return fullName;
	}

	public void setFullName(String fullName) {
		this.fullName = fullName;
	}

	public Node getEntrypoint() {
		return entrypoint;
	}

	public void setEntrypoint(Node entrypoint) {
		this.entrypoint = entrypoint;
	}

	public List<Node> getEndPoints() {
		return endPoints;
	}

	public void setEndPoints(List<Node> endPoints) {
		this.endPoints = endPoints;
	}

	// Compute

	/**
	 * Get the entry point of the transaction
	 * @param neo4jAL Neo4j Access Layer
	 * @return
	 */
	public Optional<Node> computeEntrypoint(Neo4jAL neo4jAL) throws Neo4jBadRequestException {
		Optional<Node> en = Transactions.getStartingPointObject(neo4jAL, this.id);
		en.ifPresent(node -> this.entrypoint = node);
		return en;
	}

	/**
	 * Get the end points of the transaction
	 * @param neo4jAL Neo4j Access Layer
	 * @return
	 */
	public List<Node> computeEndpoints(Neo4jAL neo4jAL) throws Neo4jBadRequestException {
		List<Node> en = Transactions.getEndPointObject(neo4jAL, this.id);
		this.endPoints = en;
		return en;
	}

	public Transaction(long id, String name, String fullName) {
		this.id = id;
		this.name = name;
		this.fullName = fullName;
	}
}
