package com.castsoftware.caesar.controllers;

import com.castsoftware.caesar.database.Neo4jAL;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jQueryException;
import org.neo4j.graphdb.Result;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SimilarityController {

  private static final String ERROR_CODE = "DIVCx";
  private static final String PREFIX_SIMILAR = "\\SimilarTransaction_";
  private static final String PREFIX_DIFFERENT = "\\DifferentTransaction_";
  private final Neo4jAL neo4jAL;

  private final String applicationSource;
  private final String applicationTarget;
  private final String outputPath;

  /**
   * Constructor
   *
   * @param neo4jAL Neo4j Access Layer
   * @param applicationSource Application source
   * @param applicationTarget Application (Target) to compare
   * @param outputPath Output of the results
   */
  public SimilarityController(
      Neo4jAL neo4jAL, String applicationSource, String applicationTarget, String outputPath)
      throws IOException {
    this.neo4jAL = neo4jAL;
    this.applicationSource = applicationSource;
    this.applicationTarget = applicationTarget;
    this.outputPath = outputPath;

    String similarFileName = outputPath + SimilarityController.PREFIX_SIMILAR + ".csv";
    // Different
    String differentFileName =  outputPath + SimilarityController.PREFIX_DIFFERENT + ".csv";

    List<String> mapList = List.of(similarFileName, differentFileName);
    mapList.forEach(x -> {
      try (FileWriter fw = new FileWriter(x, true); BufferedWriter bw = new BufferedWriter(fw)) {
        // 			String toWrite = "Similar," + idTrans.toString() + "," + nameTrans + "," + delta + "," +
        // similarities + "," + total + "," +similarTrans;
        bw.write(
                "Type, SourceId, SourceName, SourceSize, TargetId, TargetName, TargetSize, SimilarityObject, SimilarityDatabase");
        bw.newLine();
        bw.flush();
      } catch (IOException ignored) {
      }
    });

  }

  /** Run logic */
  public void run() throws Neo4jQueryException {
    // Find transaction similarity
    runTransactions();

    // Find database similarity
    runDatabase();
  }

  /** Analyze the similarity of the transactions */
  public void runTransactions() throws Neo4jQueryException {

    // Parse the transactions
    String reqTransaction =
        String.format(
            "MATCH (t:Transaction:`%1$s`) "
                + "RETURN DISTINCT ID(t) as idTrans, t.Name as transaction",
            applicationSource);

    String reqFindByName =
        String.format(
            "MATCH (t:Transaction:`%1$s`) WHERE t.Name=$name "
                + "RETURN DISTINCT ID(t) as idTrans, t.Name as transaction LIMIT 1",
            applicationTarget);

    Result res = neo4jAL.executeQuery(reqTransaction);

    // Extract the result of the transaction query
    Result resFinding = null;
    Map<String, Object> params = null;
    Map<String, Object> results = null;

    String fullName = null;
    String fullNameTarget = null;

    Long idTrans = null;
    Long idTarget = null;

    Double deltaPercentage = null;
    long start, end, elapsedTime;

    int it = 0;

    while (res.hasNext()) {
      it++;

      start = System.currentTimeMillis();

      results = res.next();
      fullName = (String) results.get("transaction");
      idTrans = (Long) results.get("idTrans");

      try {
        // Find a transaction with the same name
        params = Map.of("name", fullName);
        resFinding = neo4jAL.executeQuery(reqFindByName, params);

        // Parse the transactions
        if (resFinding.hasNext()) {
          // Found a similar transaction
          idTarget = (Long) results.get("idTrans");
          fullNameTarget = (String) results.get("transaction");

          getSimilarTransactionDelta(idTrans, fullName, idTarget, fullNameTarget);
        } else {
          // Need to find a similar transaction
          findSimilarTransaction(idTrans, fullName);
        }

      } catch (Neo4jQueryException | IOException e) {
        // Ignored
        neo4jAL.logError("Failed to treat transaction with id " + idTrans.toString() + " :", e);
      } finally {
        end = System.currentTimeMillis();
        elapsedTime = end - start;
        neo4jAL.logInfo("Iteration : " + it + ". Took " + elapsedTime + "ms.");
      }
    }
  }

  /** Analyze the similarity of the database */
  public void runDatabase() {
    return;
  }

  /**
   * Find the average delta between two list
   * @param objectSource List of object in the source set
   * @param objectTarget List of object in the Target Set
   * @return The average percentage of similarities
   */
  public Double getDeltaList(List<String> objectSource, List<String> objectTarget) {
    int transactionAtotal = objectSource.size(); // +1 to avoid empty transactions
    int transactionBtotal = objectTarget.size();

    List<String> aInBlist = new ArrayList<>(objectSource); // Objects in A
    List<String> bInAlist = new ArrayList<>(objectTarget); // Objects in B

    aInBlist.removeAll(objectTarget); // All the object in A without the object in B ( Differences )
    bInAlist.removeAll(objectSource); // All the object in B without the object in A ( Differences )

    // If 0 difference then it's a perfect match
    double percentageA = 100.0; // By default assign 100%
    if(!aInBlist.isEmpty()) {
      if(transactionAtotal == 0) percentageA = 0.0;
      else percentageA = 100 * ( 1 - (double) aInBlist.size() / transactionAtotal );
    }

    double percentageB = 100.0;
    if(!bInAlist.isEmpty()) {
      if(transactionBtotal == 0) percentageB = 0.0;
      else percentageB = 100 * ( 1 - (double) bInAlist.size() / transactionBtotal );
    }

    // Average of this percentage
    return  percentageA;
  }

  /**
   * Find the delta based on transaction objects
   *
   * @param idTrans Id of the source transaction
   * @param idTarget Id of the target transaction
   * @throws Neo4jQueryException If the request failed to execute
   */
  private void getSimilarTransactionDelta(
      Long idTrans, String fullName, Long idTarget, String fullNameTarget)
      throws Neo4jQueryException, IOException {

    // Get Object Delta
    List<String> objectSource = getTransactionObject(applicationSource, idTrans);
    List<String> objectTarget = getTransactionObject(applicationTarget, idTarget);
    int sizeSource = objectSource.size();
    int sizeTarget = objectTarget.size();

    Double deltaObject = getDeltaList(objectSource, objectTarget);

    // Get database delta
    List<String> databaseSource = getDatabaseTables(applicationSource, idTrans);
    List<String> databaseTarget = getDatabaseTables(applicationTarget, idTarget);

    Double deltaDatabase = getDeltaList(databaseSource, databaseTarget);

    writeResult(
        true, idTrans, fullName, sizeSource,
            idTarget, fullNameTarget, sizeTarget,
            deltaObject, deltaDatabase);
  }

  /**
   * Parse target application transactions to extract the most similar ones
   *
   * @param idTrans Id of the transaction to analyze
   */
  private void findSimilarTransaction(Long idTrans, String transactionName)
      throws Neo4jQueryException, IOException {
    // Get the objects in the source transaction
    List<String> objectSource = getTransactionObject(applicationSource, idTrans);
    List<String> tablesSource = getDatabaseTables(applicationSource, idTrans);

    String reqIdTrans =
        String.format(
            "MATCH (t:Transaction:`%1$s`) RETURN DISTINCT ID(t) as tId, t.Name as transName;",
            applicationTarget);
    Result res = neo4jAL.executeQuery(reqIdTrans);

    List<String> objectTarget;
    List<String> tablesTarget;
    Map<String, Object> result;
    Long idTarget = null;
    String transName = null;

    Double deltaObject;
    Double deltaDatabase;

    Double maxDelta = 0.0;
    Long maxDeltaTransID = -1L;
    String maxDeltaName = "Empty";
    Double maxDeltaTableDiff = 0.0;
    int maxDeltaObjectCount = 0;

    while (res.hasNext()) {
      result = res.next();
      idTarget = (Long) result.get("tId");
      transName = (String) result.get("transName");

      // Get the objects in the target transaction

      objectTarget = getTransactionObject(applicationTarget, idTarget);
      tablesTarget = getDatabaseTables(applicationTarget, idTarget);

      // Skip empty transaction
      if(objectTarget.size() == 0) continue;

      deltaObject = getDeltaList(objectSource, objectTarget);
      deltaDatabase = getDeltaList(tablesSource, tablesTarget);

      if (deltaObject > maxDelta) {
        maxDelta = deltaObject;
        maxDeltaTransID = idTarget;
        maxDeltaName = transName;
        maxDeltaTableDiff = deltaDatabase;
        maxDeltaObjectCount = objectTarget.size();
      }
    }

    writeResult(false, idTrans, transactionName, objectSource.size(),
            maxDeltaTransID, maxDeltaName, maxDeltaObjectCount, maxDelta, maxDeltaTableDiff);
    // Save the results

  }

  /**
   * Get the list of object in a transaction
   *
   * @param application
   * @param idTrans
   * @throws Neo4jQueryException If the request failed to execute
   * @return The list of object fullName
   */
  private List<String> getTransactionObject(String application, Long idTrans)
      throws Neo4jQueryException {
    Map<String, Object> params = Map.of("idTrans", idTrans);
    String reqTransaction =
        String.format(
            "MATCH (t:Transaction:`%s`)-[]->(o:Object) WHERE ID(t)=$idTrans "
                + "RETURN DISTINCT o.FullName as objectName;",
            application);

    Result res = neo4jAL.executeQuery(reqTransaction, params);

    List<String> sourceObject = new ArrayList<>();
    while (res.hasNext()) {
      sourceObject.add((String) res.next().get("objectName"));
    }

    return sourceObject;
  }

  /**
   * Get the list of database table
   *
   * @param application Name of the application
   * @param idTrans Id of the transaction
   * @throws Neo4jQueryException If the request failed to execute
   * @return The list of object fullName
   */
  private List<String> getDatabaseTables(String application, Long idTrans)
      throws Neo4jQueryException {
    Map<String, Object> params = Map.of("idTrans", idTrans);
    String reqTransaction =
        String.format(
            "MATCH (t:Transaction:`%s`)-[]->(o:Object) WHERE ID(t)=$idTrans "
                + "AND o.Type CONTAINS 'Table' "
                + "RETURN DISTINCT o.FullName as objectName;",
            application);

    Result res = neo4jAL.executeQuery(reqTransaction, params);

    List<String> sourceObject = new ArrayList<>();
    while (res.hasNext()) {
      sourceObject.add((String) res.next().get("objectName"));
    }

    return sourceObject;
  }

  /**
   * Create and append the results to the File
   */
  private void writeResult(
          Boolean similar,
      Long idTrans,
      String nameTrans,
      Integer objectNumberSource,
      Long idOther,
      String otherName,
      Integer objectNumberOther,
      Double deltaObject,
      Double deltaData )
      throws IOException {

    String fileName;
    String tag;
    if(similar) {
      tag = "Similar";
      fileName = outputPath + SimilarityController.PREFIX_SIMILAR + ".csv";
    } else {
      tag = "Different";
      fileName = outputPath + SimilarityController.PREFIX_DIFFERENT + ".csv";
    }

    // "Similar, IdTransaction, TransName, DeltaObject, Similarities, DeltaDatabase, SimilarityDatabase"
    try (FileWriter fw = new FileWriter(fileName, true); BufferedWriter bw = new BufferedWriter(fw)) {
      String toWrite =
              tag
                      + ","
                      + idTrans
                      + ","
                      + nameTrans
                      + ","
                      + objectNumberSource
                      + ","
                      + idOther
                      + ","
                      + otherName
                      + ","
                      + objectNumberOther
                      + ","
                      + deltaObject
                      + ","
                      + deltaData;
      bw.write(toWrite);
      bw.newLine();
      bw.flush();
    } catch (IOException ignored) {
    }
  }


}
