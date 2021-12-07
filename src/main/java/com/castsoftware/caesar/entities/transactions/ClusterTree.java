package com.castsoftware.caesar.entities.transactions;

import org.apache.xpath.operations.Bool;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class ClusterTree {

	private Boolean isRoot;
	private String label;

	private ClusterTransaction cluster;

	private List<ClusterTree> children;

	/**
	 * Find a children with a specific label
	 * @param label Label to find
	 * @return Optional of Cluster tree, empty if not found
	 */
	public Optional<ClusterTree> findChildrenByLabel(String label) {
		for(ClusterTree child : this.children) {
			if(child.label.equals(label)) return Optional.of(child);
		}

		return  Optional.empty();
	}

	private void addChildren(ClusterTree cluster) {
		this.children.add(cluster);
	}

	/**
	 * Wrap ClusterTransaction add Transaction
	 * @param tn Transaction to add
	 */
	private void addTransaction(Transaction tn) {
		this.cluster.addTransaction(tn);
	}

	/**
	 * Insert an element in the tree
	 * @param categories Categories to add
	 * @param tn Transaction node to include
	 */
	public void insert(List<String> categories, Transaction tn) {
		this.recursiveInsert(categories, tn, this);
	}

	/**
	 * Recursive insertion
	 * @param categories Categories
	 * @param tn Transaction
	 * @param parent Parent to insert
	 * @return
	 */
	private void recursiveInsert(List<String> categories, Transaction tn, ClusterTree parent) {

		String toInsert = categories.remove(0); // Pop first element
		ClusterTree newParent = null;

		// Find or create branch
		Optional<ClusterTree> optLeaf = this.findChildrenByLabel(toInsert);
		if(optLeaf.isPresent()) {
			ClusterTree leaf = optLeaf.get();
			leaf.addTransaction(tn); // Add the transaction in the cluster
			newParent = leaf;
		} else {
			// Create a new leaf and insert under the parent
			ClusterTree newLeaf = new ClusterTree(toInsert);
			newLeaf.addTransaction(tn);
			parent.addChildren(newLeaf);
			newParent = newLeaf;
		}

		// If list isn't empty add it
		if(!categories.isEmpty()) recursiveInsert(categories, tn, newParent);
	}

	/**
	 * Flat version of the tree, where element get a uuid property and a link to their parent
	 * @return A flat list of all the children
	 */
	public List<ClusterTransaction> flatten() {
		List<ClusterTransaction> clusters = new ArrayList<>();

		String uuidIt= this.isRoot ? "" : UUID.randomUUID().toString();
		if(!this.isRoot)  {
			this.cluster.setId(uuidIt);
			clusters.add(this.cluster);
		}

		for(ClusterTree it: this.children) {
			it.cluster.setParent(uuidIt);
			clusters.addAll(it.flatten());
		}

		return clusters;
	}

	/**
	 * Assign a label to the Leaf
	 * @param label Label of the leaf to create
	 */
	public ClusterTree(String label) {
		this.label = label;
		this.children = new ArrayList<>();
		this.isRoot = false;
		this.cluster = new ClusterTransaction(label);
	}

	public ClusterTree() {
		this.children = new ArrayList<>();
		this.isRoot = true;
	}
}
