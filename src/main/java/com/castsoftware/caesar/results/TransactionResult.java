package com.castsoftware.caesar.results;

import com.castsoftware.caesar.entities.transactions.Transaction;
import org.neo4j.graphdb.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TransactionResult {
	public Long id;
	public String name;
	public String fullName;
	public Long size;

	public Long entrypoint = null;
	public List<Long> endPoints = new ArrayList<>();

	public Double uniqueness;

	public TransactionResult(long id, String name, String fullName,
							 Node entrypoint, List<Node> endPoint,
							 Double uniqueness) {
		this.id = id;
		this.name = name;
		this.fullName = fullName;
		this.uniqueness = uniqueness;
	}

	public TransactionResult(Transaction tn) {
		this.id = tn.getId();
		this.name = tn.getName();
		this.fullName = tn.getFullName();
		this.size = tn.getSize();

		this.uniqueness = tn.getUniqueness();
	}
}
