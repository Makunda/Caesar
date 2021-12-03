package com.castsoftware.caesar.results;

import com.castsoftware.caesar.entities.transactions.ClusterTransaction;
import org.neo4j.graphdb.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ClusterTransactionResult {
	public String name;
	public Long objectSize;
	public List<Long> transactionsId;

	public List<Node> entrypoints;
	public List<Node> endPoints;

	public Double uniqueness;

	/**
	 * Cluster results
	 * @param ct Transaction Cluster
	 */
	public ClusterTransactionResult(ClusterTransaction ct) {
		this.name = ct.getName();
		this.objectSize = ct.getObjectSize();
		this.transactionsId = ct.getTransactionsId();

		this.entrypoints = new ArrayList<>(ct.getEntrypoints());
		this.endPoints = new ArrayList<>(ct.getEndPoints());

		this.uniqueness = ct.getUniqueness();

	}

}
