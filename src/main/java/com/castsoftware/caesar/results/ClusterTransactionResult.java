package com.castsoftware.caesar.results;

import com.castsoftware.caesar.entities.transactions.ClusterTransaction;
import org.neo4j.graphdb.Node;

import java.util.ArrayList;
import java.util.List;

public class ClusterTransactionResult {
	public String id;
	public String parentId;

	public String name;
	public Long objectSize;
	public List<Long> transactionsId;

	public List<Node> entryPoints;
	public List<Node> endPoints;

	public Double uniqueness;

	/**
	 * Cluster results
	 * @param ct Transaction Cluster
	 */
	public ClusterTransactionResult(ClusterTransaction ct) {
		this.id = ct.getId();
		this.parentId = ct.getParent();

		this.name = ct.getName();
		this.objectSize = ct.getObjectSize();
		this.transactionsId = ct.getTransactionsId();

		this.entryPoints = new ArrayList<>(ct.getEntryPoints());
		this.endPoints = new ArrayList<>(ct.getEndPoints());

		this.uniqueness = ct.getUniqueness();

	}

}
