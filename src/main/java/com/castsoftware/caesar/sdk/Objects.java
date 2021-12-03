package com.castsoftware.caesar.sdk;

import org.neo4j.graphdb.*;

import java.util.*;

public class Objects {

	private static final String SUB_TO_PARENT = "BELONGSTO";

	/**
	 * Check if the node is an object
	 * @param n Node to check
	 * @return True if the node is an object
	 */
	public static boolean isObject(Node n) {
		for(Label l : n.getLabels()) {
			if(l.name().equals("Object")) return true;
		}
		return false;
	}

	/**
	 * Check if the node is a sub-object
	 * @param n Node to check
	 * @return True if the node is an sub-object
	 */
	public static boolean isSubObject(Node n) {
		for(Label l : n.getLabels()) {
			if(l.name().equals("SubObject")) return true;
		}
		return false;
	}

	/**
	 * Get the parent object
	 * @param n
	 * @return Optional returning the node, or empty if nothing has been found
	 */
	public static Optional<Node> getParentObject(Node n) {
		if( isObject(n)) return Optional.of(n); // Return if is object

		RelationshipType relation = RelationshipType.withName(SUB_TO_PARENT);
		Iterator<Relationship> relationships = n.getRelationships(Direction.INCOMING, relation).iterator();

		if (relationships.hasNext()) {
			Node otherNode = relationships.next().getOtherNode(n);
			return Optional.of(otherNode);
		} else {
			return Optional.empty();
		}
	}

	/**
	 * Get the list of all nodes with incoming relationships
	 * @param node Node to process
	 * @return The list of nodes
	 */
	public static Set<Node> getIncomingNode(Node node) {
		// Get return list
		Set<Node> returnList = new HashSet<>();

		// Get children
		Node n;
		for(Relationship rel : node.getRelationships(Direction.INCOMING)) {
			n = rel.getOtherNode(node);
			if(n.getId() != node.getId()) returnList.add(n); // Ignore self references
		}

		return returnList;
	}

	/**
	 * Get the list of all incoming nodes with a specific relationship
	 * @param node Node to treat
	 * @param incomingLinks List incoming links
	 * @return The list of incoming nodes
	 */
	public static Set<Node> getIncomingNodes(Node node, List<String> incomingLinks) {
		// Get return list
		Set<Node> returnList = new HashSet<>();

		// Get children
		Node n;
		String relType;
		for(Relationship rel : node.getRelationships(Direction.INCOMING)) {
			relType = rel.getType().name();
			if (incomingLinks.contains(relType)) {
				// Valid link discovered
				n = rel.getOtherNode(node);
				if(n.getId() != node.getId()) returnList.add(n); // Ignore self references
			}
		}

		return returnList;
	}

	/**
	 * Get the list of all nodes with incoming relationships
	 * @param node Node to process
	 * @return The list of nodes
	 */
	public static Set<Node> getOutgoingNodes(Node node) {
		// Get return list
		Set<Node> returnList = new HashSet<>();

		// Get children
		Node n;
		for(Relationship rel : node.getRelationships(Direction.OUTGOING)) {
			n = rel.getOtherNode(node);
			if(n.getId() != node.getId()) returnList.add(n); // Ignore self references
		}

		return returnList;
	}

	/**
	 * Get the list of all incoming nodes with a specific relationship
	 * @param node Node to treat
	 * @param incomingLinks List incoming links
	 * @return The list of incoming nodes
	 */
	public static Set<Node> getOutgoingNodes(Node node, List<String> incomingLinks) {
		// Get return list
		Set<Node> returnList = new HashSet<>();

		// Get children
		Node n;
		String relType;
		for(Relationship rel : node.getRelationships(Direction.OUTGOING)) {
			relType = rel.getType().name();
			if (incomingLinks.contains(relType)) {
				// Valid link discovered
				n = rel.getOtherNode(node);
				if(n.getId() != node.getId()) returnList.add(n); // Ignore self references
			}
		}

		return returnList;
	}


	/**
	 * Get the list node attached, with valid relationships
	 * @param node Node to investigate
	 * @param incomingLinks List of incoming links
	 * @param outgoingLinks List of outgoing links
	 * @return Get the set of nodes
	 */
	public static Set<Node> getAttached(Node node, List<String> incomingLinks, List<String> outgoingLinks ) {
		// Get return list
		Set<Node> returnList = new HashSet<>();
		returnList.addAll(getIncomingNodes(node, incomingLinks));
		returnList.addAll(getOutgoingNodes(node, outgoingLinks));

		// Return nodes
		return returnList;
	}
}
