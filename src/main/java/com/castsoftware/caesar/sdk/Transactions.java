package com.castsoftware.caesar.sdk;

import com.castsoftware.caesar.database.Neo4jAL;
import com.castsoftware.caesar.entities.transactions.Transaction;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jBadRequestException;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jNoResult;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jQueryException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class Transactions {

	/**
	 * Get the list of transaction in the application
	 * @param neo4jAL Neo4j Access list
	 * @param application Name of the application
	 * @return The list of transaction in an application
	 */
	public static List<Node> getTransactions(Neo4jAL neo4jAL, String application) throws Neo4jBadRequestException {
		List<Node> returnList = new ArrayList<>();
		String request = String.format("MATCH (tran:Transaction:`%s`) RETURN DISTINCT tran as transaction", application);

		try {
			Node n;
			Result results = neo4jAL.executeQuery(request);
			while(results.hasNext()) {
				n = (Node) results.next().get("transaction");
				returnList.add(n);
			}

			return returnList;
		} catch (Neo4jQueryException e) {
			throw new Neo4jBadRequestException("Failed to get the list of transaction", e, "TRANxGET1");
		}
	}

	/**
	 * Get the list of transaction in the application
	 * @param neo4jAL Neo4j Access list
	 * @param application Name of the application
	 * @param minSize Minimum number of object in the transaction to be taken in account
	 * @return The list of transaction in an application
	 */
	public static List<Node> getTransactions(Neo4jAL neo4jAL, String application, int minSize) throws Neo4jBadRequestException {
		List<Node> returnList = new ArrayList<>();
		String request =
			String.format(
				"MATCH (tran:Transaction:`%s`)-[]->(o:Object) "
					+ "WITH tran, COLLECT(DISTINCT o) as objects "
					+ "WHERE SIZE(objects) > $minSize "
					+ "RETURN DISTINCT tran as transaction ",
				application);
		Map<String, Object> params = Map.of("minSize", minSize);

		try {
			Node n;
			Result results = neo4jAL.executeQuery(request, params);
			while(results.hasNext()) {
				n = (Node) results.next().get("transaction");
				returnList.add(n);
			}

			return returnList;
		} catch (Neo4jQueryException e) {
			throw new Neo4jBadRequestException("Failed to get the list of transaction", e, "TRANxGET1");
		}
	}

	/**
	 * Get the transaction count
	 * @param neo4jAL Neo4j Access Layer
	 * @param application Name of the application
	 * @param minSize Minimum number of object in the transaction to be taken in account
	 * @return The count of transaction in the application
	 * @throws Neo4jBadRequestException
	 */
	public static Long getTransactionsCount(Neo4jAL neo4jAL, String application, int minSize) throws Neo4jBadRequestException {
		String request = String.format("MATCH (tran:Transaction:`%s`)-[]->(o:Object) "
				+ "WITH tran, COLLECT(DISTINCT o) as objects "
				+ "WHERE SIZE(objects) > $minSize "
				+ "RETURN COUNT(DISTINCT tran) as count ",
				application);
		Map<String, Object> params = Map.of("minSize", minSize);

		try {
			Result results = neo4jAL.executeQuery(request, params);
			if (results.hasNext()) return (Long) results.next().get("count");
			else throw new Neo4jNoResult("Failed to retrieve the count of transaction in the transaction",request, "TRANxGET2");
		} catch (Neo4jQueryException | Neo4jNoResult e) {
			throw new Neo4jBadRequestException("Failed to get the count of transaction", e, "TRANxGET1");
		}
	}

	/**
	 * Get the count of Transaction in the application
	 * @param neo4jAL Neo4j Access Layer
	 * @param application Application name
	 * @return The count
	 * @throws Neo4jBadRequestException
	 */
	public static Long getTransactionsCount(Neo4jAL neo4jAL, String application) throws Neo4jBadRequestException {
		String request = String.format("MATCH (tran:Transaction:`%s`) " +
				"RETURN count(DISTINCT tran) as count", application);

		try {
			Result results = neo4jAL.executeQuery(request);
			if (results.hasNext()) return (Long) results.next().get("count");
			else throw new Neo4jNoResult("Failed to retrieve the count of transaction in the transaction",request, "TRANxGET2");
		} catch (Neo4jQueryException | Neo4jNoResult e) {
			throw new Neo4jBadRequestException("Failed to get the count of transaction", e, "TRANxGET1");
		}
	}

	/**
	 * Get the start point object
	 * @param neo4jAL Neo4j Access Layer
	 * @param idTransaction Id of the transaction
	 * @return The starting object
	 * @throws Neo4jBadRequestException
	 */
	public static Optional<Node> getStartingPointObject(Neo4jAL neo4jAL, Long idTransaction) throws Neo4jBadRequestException {
		String request = "MATCH (tran:Transaction)-[:StartsWith]->(startTran:TransactionNode)-[:IN]->(startObj) " +
				"WHERE ID(tran)=$idTransaction " +
				"RETURN DISTINCT startObj as start";
		Map<String, Object> params = Map.of("idTransaction", idTransaction);
		try {
			Result results = neo4jAL.executeQuery(request, params);
      		if (!results.hasNext()) return Optional.empty();

      		Node n = (Node) results.next().get("start");

      		// If SubObject get Object, otherwise return it
			if(Objects.isObject(n)) {
				return Optional.of(n);
			} else {
				return Objects.getParentObject(n); // Get parent
			}
		} catch (Neo4jQueryException e) {
			throw new Neo4jBadRequestException("Failed to get the start point of the transaction", e, "TRANxGET1");
		}
	}

	/**
	 * Get the end point object
	 * @param neo4jAL Neo4j Access Layer
	 * @param idTransaction Id of the transaction
	 * @return The starting object
	 * @throws Neo4jBadRequestException
	 */
	public static List<Node> getEndPointObject(Neo4jAL neo4jAL, Long idTransaction) throws Neo4jBadRequestException {
		String request = "MATCH (tran:Transaction)-[:EndsWith]->(endTran:TransactionNode)-[:OUT]->(endObj) " +
				"WHERE ID(tran)=$idTransaction " +
				"RETURN DISTINCT endObj as end";
		Map<String, Object> params = Map.of("idTransaction", idTransaction);
		List<Node> returnList = new ArrayList<>();

		try {
			Result results = neo4jAL.executeQuery(request, params);
			Node n;

			while (results.hasNext()) {
				n = (Node) results.next().get("end");

				// If SubObject get Object, otherwise return it
				if(Objects.isObject(n)) {
					returnList.add(n); // Add if object
				} else {
					Optional<Node> optNode = Objects.getParentObject(n); // Get parent
					optNode.ifPresent(returnList::add);
				}
			}

			return returnList;
		} catch (Neo4jQueryException e) {
			throw new Neo4jBadRequestException("Failed to get the start point of the transaction", e, "TRANxGET1");
		}
	}

	/**
	 * Get the list of object in the transaction
	 * @param neo4jAL Neo4j Access Layer
	 * @param idTransaction Id of the transaction to compare
	 * @return
	 */
	public static List<Node> getObjects(Neo4jAL neo4jAL, Long idTransaction) throws Neo4jBadRequestException {
		List<Node> returnList = new ArrayList<>();
		String request = "MATCH (tran:Transaction)-[:Contains]->(o:Object) " +
				"WHERE ID(tran)=$idTransaction " +
				"RETURN DISTINCT o as object";
		Map<String, Object> params = Map.of("idTransaction", idTransaction);

		try {
			Node n;
			Result results = neo4jAL.executeQuery(request);
			while(results.hasNext()) {
				n = (Node) results.next().get("object");
				returnList.add(n);
			}

			return returnList;
		} catch (Neo4jQueryException e) {
			throw new Neo4jBadRequestException("Failed to get the list of objects in the transaction", e, "TRANxGET1");
		}
	}

	/**
	 * Get the list of object in the transaction
	 * @param neo4jAL Neo4j Access Layer
	 * @param idTransactions Id of the transaction to compare
	 * @return The number of Object in the transaction
	 */
	public static Long getTransactionsSize(Neo4jAL neo4jAL, List<Long> idTransactions) throws Neo4jBadRequestException {
		String request = "UNWIND $idTransactions AS id " +
				"MATCH (tran:Transaction)-[:Contains]->(o:Object) " +
				"WHERE ID(tran)=id " +
				"RETURN COUNT(DISTINCT o) as count";
		Map<String, Object> params = Map.of("idTransactions", idTransactions);

		try {
			Result results = neo4jAL.executeQuery(request, params);
			if (results.hasNext()) return (Long) results.next().get("count");
			else throw new Neo4jNoResult("Failed to retrieve the number of object in the cluster",request, "TRANxGET2");
		} catch (Neo4jQueryException | Neo4jNoResult e) {
			throw new Neo4jBadRequestException("Failed to get the list of objects in the cluster", e, "TRANxGET1");
		}
	}

	/**
	 * Get the number of object in the transaction
	 * @param neo4jAL Neo4j Access Layer
	 * @param idTransaction Id of the transaction to compare
	 * @return The number of objects in the transaction
	 */
	public static long getTransactionSize(Neo4jAL neo4jAL, Long idTransaction) throws Neo4jBadRequestException {
		String request = "MATCH (tran:Transaction)-[:Contains]->(o:Object) " +
				"WHERE ID(tran)=$idTransaction " +
				"RETURN COUNT(DISTINCT o) as count";
		Map<String, Object> params = Map.of("idTransaction", idTransaction);

		try {
			Result results = neo4jAL.executeQuery(request, params);
      		if (results.hasNext()) return (Long) results.next().get("count");
			else throw new Neo4jNoResult("Failed to retrieve the number of object in the transaction",request, "TRANxGET2");
		} catch (Neo4jQueryException | Neo4jNoResult e) {
			throw new Neo4jBadRequestException("Failed to get the transaction size", e, "TRANxGET1");
		}
	}

	/**
	 * Get the list of object in the transaction
	 * @param neo4jAL Neo4j Access Layer
	 * @param idTransaction Id of the transaction to compare
	 * @return The count of objects in the transactions
	 */
	public static long getNumberSharedObjects(Neo4jAL neo4jAL, Long idTransaction) throws Neo4jBadRequestException {
    String request =
        "MATCH (t:Transaction)-[:Contains]->(o:Object)<-[:Contains]-(tran2:Transaction) "
            + "WHERE ID(t)=$idTransaction AND ID(tran2)<>ID(t) "
            + "RETURN COUNT(DISTINCT o) as count";
		Map<String, Object> params = Map.of("idTransaction", idTransaction);

		try {
			Result results = neo4jAL.executeQuery(request, params);
			if (results.hasNext()) return (Long) results.next().get("count");
			else throw new Neo4jNoResult("Failed to retrieve the number of shared object in the transaction",request, "TRANxGET2");
		} catch (Neo4jQueryException | Neo4jNoResult e) {
			throw new Neo4jBadRequestException("Failed to get the number of shared objects in the transaction", e, "TRANxGET1");
		}
	}

	/**
	 * Get the list of specific objects in the cluster of transaction
	 * @param neo4jAL Neo4j Access Layer
	 * @param transactionsId Id of the transaction to compare
	 * @return The count of specific objects in the cluster
	 */
	public static long getNumberSharedObjects(Neo4jAL neo4jAL, List<Long> transactionsId) throws Neo4jBadRequestException {
		String request =
				"UNWIND $transactionsId as id "
				+ "MATCH (t:Transaction)-[:Contains]->(o:Object)<-[:Contains]-(tran2:Transaction) "
				+ "WHERE ID(t)=id AND NOT ID(tran2) IN $transactionsId "
				+ "RETURN COUNT(DISTINCT o) as count";
		Map<String, Object> params = Map.of("transactionsId", transactionsId);

		try {
			Result results = neo4jAL.executeQuery(request, params);
			if (results.hasNext()) return (Long) results.next().get("count");
			else throw new Neo4jNoResult("Failed to retrieve the number of shared object in the cluster",request, "TRANxGET2");
		} catch (Neo4jQueryException | Neo4jNoResult e) {
			throw new Neo4jBadRequestException("Failed to get the number of shared objects in the cluster", e, "TRANxGET1");
		}
	}

	/**
	 * Check if the node is an Transaction
	 * @param n Node to check
	 * @return True if the node is an Transaction
	 */
	public static boolean isTransaction(Node n) {
		for(Label l : n.getLabels()) {
			if(l.name().equals("Transaction")) return true;
		}
		return false;
	}

	/**
	 * Convert a node to a Transaction object
	 * @param neo4jAL Neo4j access layer
	 * @param tn Node to convert
	 * @return
	 */
	public static Transaction getTransactionFromNode(Neo4jAL neo4jAL, Node tn) throws Exception, Neo4jBadRequestException {
		if(!isTransaction(tn)) throw new Exception("Can only convert transaction nodes");

		try {
			// Get properties
			Long id = tn.getId();
			String name = (String) tn.getProperty("Name");
			String fullName = (String) tn.getProperty("FullName");

			// Compute metrics
			long size = Transactions.getTransactionSize(neo4jAL, id);
			long shared = Transactions.getNumberSharedObjects(neo4jAL, id);
			Double uniqueness = 1 - (double) shared / size;

			// Build the transaction
			Transaction transaction = new Transaction(id, name, fullName);
			transaction.setUniqueness(uniqueness);
			transaction.setSize(size);

			return transaction;
		} catch (Exception e) {
			neo4jAL.logError("Conversion of the node produced an error.", e);
			throw new Exception(String.format("Failed to convert the transaction node with id [%d]", tn.getId()));
		}
	}

}
