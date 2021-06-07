package com.castsoftware.caesar.controllers;

import com.castsoftware.caesar.database.Neo4jAL;
import com.castsoftware.caesar.database.Neo4jTypeManager;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jNoResult;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jQueryException;
import org.neo4j.graphdb.*;

import java.awt.*;
import java.util.List;
import java.util.*;

import static java.lang.Math.exp;

public class DivideController {

  private static final String ERROR_CODE = "DIVCx";

  private static final Long MIN_CLIQUE_SIZE = 100L;
  private static final Long MAX_CLIQUE_SIZE = 3000L;

  private static final String DEMETER_LEVEL_TAG = "$l_";
  private static final Double MIN_SIMILARITY_MERGE = 0.5;
  private static final String SIMILARITY_LINK = "SIMILARITY";
  private static final String COMMUNITY = "COMMUNITY";
  private static final String TRANSACTION_COMMUNITY = "COMMUNITY_TRANSACTION";
  private static final String WEIGHT_PROPERTY = "WEIGHT";

  private static final Color[] COLOR_TABLE = {
    new Color(0x05, 0x04, 0xaa), //  royal blue
    new Color(0xe6, 0xda, 0xa6), //  beige
    new Color(0xff, 0x79, 0x6c), //  salmon
    new Color(0x6e, 0x75, 0x0e), //  olive
    new Color(0x65, 0x00, 0x21), //  maroon
    new Color(0x01, 0xff, 0x07), //  bright green
    new Color(0x35, 0x06, 0x3e), //  dark purple
    new Color(0xae, 0x71, 0x81), //  mauve
    new Color(0x06, 0x47, 0x0c), //  forest green
    new Color(0x13, 0xea, 0xc9), //  aqua
    new Color(0x00, 0xff, 0xff), //  cyan
    new Color(0xd1, 0xb2, 0x6f), //  tan
    new Color(0x00, 0x03, 0x5b), //  dark blue
    new Color(0xc7, 0x9f, 0xef), //  lavender
    new Color(0x06, 0xc2, 0xac), //  turquoise
    new Color(0x03, 0x35, 0x00), //  dark green
    new Color(0x9a, 0x0e, 0xea), //  violet
    new Color(0xbf, 0x77, 0xf6), //  light purple
    new Color(0x89, 0xfe, 0x05), //  lime green
    new Color(0x92, 0x95, 0x91), //  grey
    new Color(0x75, 0xbb, 0xfd), //  sky blue
    new Color(0xff, 0xff, 0x14), //  yellow
    new Color(0xc2, 0x00, 0x78), //  magenta
    new Color(0x96, 0xf9, 0x7b), //  light green
    new Color(0xf9, 0x73, 0x06), //  orange
    new Color(0x02, 0x93, 0x86), //  teal
    new Color(0x95, 0xd0, 0xfc), //  light blue
    new Color(0xe5, 0x00, 0x00), //  red
    new Color(0x65, 0x37, 0x00), //  brown
    new Color(0xff, 0x81, 0xc0), //  pink
    new Color(0x03, 0x43, 0xdf), //  blue
    new Color(0x15, 0xb0, 0x1a), //  green
    new Color(0x7e, 0x1e, 0x9c), //  purple
  };


  Neo4jAL neo4jAL;
  String application;
  String levelName;

  Node level;
  List<Node> candidates;
  List<Node> transactionList;


  public DivideController(Neo4jAL neo4jAL, String application, String level) {
    this.neo4jAL = neo4jAL;
    this.application = application;
    this.levelName = level;

    this.candidates = new ArrayList<>();
    this.transactionList = new ArrayList<>();
  }

  /**
   * Run the divide & conquer algorithm
   * All steps have been clearly identified
   * @throws Neo4jQueryException
   * @throws Neo4jNoResult
   * @throws Exception
   */
  public void run() throws Neo4jQueryException, Neo4jNoResult, Exception {
    try {
      long start, finish, timeElapsed;

      start = System.currentTimeMillis();
      neo4jAL.logInfo(
          String.format("Searching level '%s' in application '%s'..", levelName, application));
      this.findLevel();
      finish = System.currentTimeMillis();
      timeElapsed = finish - start;
      neo4jAL.logInfo(
          String.format("(%d ms) Level with name '%s' was found.", timeElapsed, levelName));

      neo4jAL.logInfo("Retrieving list of candidates nodes for investigation...");
      start = System.currentTimeMillis();
      this.findToInvestigateNodes();
      finish = System.currentTimeMillis();
      timeElapsed = finish - start;
      neo4jAL.logInfo(
          String.format(
              "(%d ms) %d nodes were identified as candidates.",
              timeElapsed, this.candidates.size()));

      neo4jAL.logInfo("Removing isolated nodes");
      start = System.currentTimeMillis();
      List<Node> removed = this.extractIsolated();
      finish = System.currentTimeMillis();
      timeElapsed = finish - start;
      neo4jAL.logInfo(
          String.format(
              "(%d ms) %d isolated nodes  were removed from the level.",
              timeElapsed, removed.size()));


      neo4jAL.logInfo("Performing the label propagation over transactions.");
      start = System.currentTimeMillis();
      this.transactionLabelPropagation();
      finish = System.currentTimeMillis();
      timeElapsed = finish - start;
      neo4jAL.logInfo(
              String.format(
                      "(%d ms) %d Transactions have been identified and grouped.",
                      timeElapsed, transactionList.size()));


      neo4jAL.logInfo("Extracting undecided nodes (nodes present in multiple levels).");
      start = System.currentTimeMillis();
      List<Node> undecided = this.extractUndecided();
      finish = System.currentTimeMillis();
      timeElapsed = finish - start;
      neo4jAL.logInfo(
          String.format(
              "(%d ms) %d undecided nodes were extracted.", timeElapsed, undecided.size()));

      neo4jAL.logInfo(
          String.format(
              "After these trimming operations %d nodes are remaining.", candidates.size()));

      neo4jAL.logInfo("Grouping node by transaction similarity.");
      start = System.currentTimeMillis();
      this.extractByTransactions();
      finish = System.currentTimeMillis();
      timeElapsed = finish - start;
      neo4jAL.logInfo(String.format("(%d ms) Node were grouped.", timeElapsed));

      neo4jAL.logInfo("Coloring nodes.");
      start = System.currentTimeMillis();
      int numberNode = this.colorNodes(COMMUNITY);
      finish = System.currentTimeMillis();
      timeElapsed = finish - start;
      neo4jAL.logInfo(String.format("(%d ms) %d Nodes were colored.", timeElapsed, numberNode));

      neo4jAL.logInfo("Assign DrillDown property.");
      start = System.currentTimeMillis();
      assignDrilldownNodes();
      finish = System.currentTimeMillis();
      timeElapsed = finish - start;
      neo4jAL.logInfo(String.format("(%d ms) Node's drilldown communities were reassigned..", timeElapsed));

    } catch (Exception e) {
      neo4jAL.logError("Execution of the divideLevel failed.", e);
      throw e;
    }
    // Find the level

  }

  /**
   * Find the level to divide in one specific application
   *
   * @return The level found
   * @throws Neo4jQueryException If the query produced an exception
   * @throws Neo4jNoResult If no level exists with this name in the application.
   */
  private Node findLevel() throws Neo4jQueryException, Neo4jNoResult {
    String req =
        String.format(
            "MATCH (l:Level5:`%s`) WHERE l.Name=$levelName RETURN l as node LIMIT 1", application);
    Map<String, Object> params = Map.of("levelName", levelName);

    Result res = neo4jAL.executeQuery(req, params);
    // Throw an error if no level was found
    if (!res.hasNext())
      throw new Neo4jNoResult(
          String.format(
              "Failed to retrieve node with name %s in application %s", levelName, application),
          req,
          ERROR_CODE + "FINL01");

    this.level = (Node) res.next().get("node");
    return this.level;
  }

  /**
   * Retrieve the list of candidates nodes under the selected level
   *
   * @return The list of nodes to investigate
   * @throws Neo4jQueryException
   */
  private List<Node> findToInvestigateNodes() throws Neo4jQueryException {
    this.candidates = new ArrayList<>();
    if (level == null) return this.candidates; // Level must not be null

    // Retrieve all the node under the level and reset the community property
    String matchNodes =
        String.format(
            "MATCH (l:Level5:`%1$s`)-[]->(o:Object:`%1$s`) "
                + "WHERE ID(l)=$idLevel "
                + "RETURN o as node",
            application);
    Map<String, Object> params = Map.of("idLevel", level.getId());

    Result res = neo4jAL.executeQuery(matchNodes, params);

    // Parse nodes from the query and append them to the result list
    Node n;
    while (res.hasNext()) {
      n = (Node) res.next().get("node");
      if (n.hasProperty(COMMUNITY)) n.removeProperty(COMMUNITY);
      this.candidates.add(n);
    }

    return this.candidates;
  }

  /**
   * Trim the node in the level ( Object not used by others ) Assign to them a new tag to be
   * extracted later
   *
   * @return The list of node removed
   * @throws Neo4jQueryException
   */
  private List<Node> extractIsolated() throws Neo4jQueryException {
    List<Node> nodesTrimmed = new ArrayList<>();

    // Retrieve all the node under the level
    String matchNodes =
        String.format("MATCH (l:Level5:`%1$s`)-[]->(o:Object:`%1$s`) "
                    + "WHERE ID(l)=$idLevel AND NOT (o)-[]-(:Object) "
                    + "SET o.Tags = CASE WHEN o.Tags IS NULL THEN [($levelTag+o.Level+' isolated')] "
                    + "ELSE [ x IN o.Tags WHERE NOT x CONTAINS $levelTag ] + ($levelTag+o.Level+' isolated') END "
                    + "RETURN o as node", application);
    Map<String, Object> params = Map.of("idLevel", level.getId(), "levelTag", DEMETER_LEVEL_TAG);
    Result res = neo4jAL.executeQuery(matchNodes, params);

    while (res.hasNext()) {
      Node n = (Node) res.next().get("node");

      // Remove the node from the principal list
      this.candidates.remove(n);
      nodesTrimmed.add(n);
    }

    return nodesTrimmed;
  }

  /**
   * Extract the community of undecided nodes ( Object belonging to more than 1 Level ) The first
   * step will be to build a map of the interactions / similarity between these communities. Then
   * they are regrouped together based on their size and similarity. For the remaining ones, if
   * they're too big, they will remain the same, if they're too small they will be merged in a
   * potpourri.
   *
   * Node flags as undecided will be removed from the investigation list
   *
   * @return
   * @throws Neo4jQueryException
   */
  private List<Node> extractUndecided() throws Neo4jQueryException {
    Map<Node, List<Node>> parentUndecided = new HashMap<>();

    // Retrieve all the node under the level
    String matchNodes =
        String.format(
            "MATCH (l:Level5:`%1$s`)-[]->(o:Object:`%1$s`)-[]-(:Object:`%1$s`)<-[]-(otherL:Level5:`%1$s`) "
                + "WHERE ID(l)=$idLevel AND ID(l)<>ID(otherL)"
                + "RETURN DISTINCT otherL as otherLevel, o as node",
            application);
    Map<String, Object> params = Map.of("idLevel", level.getId());

    Result res = neo4jAL.executeQuery(matchNodes, params);
    // Get a map of each neighbors levels with their interaction list
    while (res.hasNext()) {
      Map<String, Object> r = res.next();
      Node parent = (Node) r.get("otherLevel");
      Node node = (Node) r.get("node");

      // Remove node from to investigate list
      candidates.remove(node);

      if (!parentUndecided.containsKey(parent)) parentUndecided.put(parent, new ArrayList<>());
      parentUndecided.get(parent).add(node);
    }

    Set<SimClass> simList = new HashSet<>();
    // Get a map of the interactions
    for (Map.Entry<Node, List<Node>> en : parentUndecided.entrySet()) {
      for (Map.Entry<Node, List<Node>> enIt : parentUndecided.entrySet()) {
        if (en.equals(enIt)) continue; // skip the current element investigated

        // Compare element in both list
        List<Node> differences = new ArrayList<>(en.getValue());
        differences.removeAll(enIt.getValue());
        Double simPercentage =
            (double) (en.getValue().size() - differences.size()) / (double) en.getValue().size();
        Long size = (long) en.getValue().size() + (long) enIt.getValue().size();

        // Create a similairty class and add it to the set
        Set<Node> uniqueNodes = new HashSet<>();
        uniqueNodes.addAll(en.getValue());
        uniqueNodes.addAll(enIt.getValue());
        simList.add(
            new SimClass(
                en.getKey(), enIt.getKey(), simPercentage, size, new ArrayList<>(uniqueNodes)));
      }
    }

    Map<String, Set<Node>> communityNodeMap = new HashMap<>();
    Set<Node> nodeSet = new HashSet<>(); // Keep a track of nodes already happened

    Map<Set<Node>, Set<Node>> communityMap = new HashMap<>();

    // Start by merging small and similar communities
    Iterator<SimClass> i = simList.iterator();
    while (i.hasNext()) {
      SimClass s = i.next();

      // check the size and the similarity of the clique
      if (s.simPercentage > MIN_SIMILARITY_MERGE && s.size <= MAX_CLIQUE_SIZE) {
        // Search
        Set<Node> key = searchInLevelMap(communityMap, s);

        // Remove nodes already processed
        s.nodes.remove(nodeSet);

        // New community
        if (key == null) {
          communityMap.put(
              Set.of(s.level1, s.level2), new HashSet<>(s.nodes)); // Create a new community
        } else {
          communityMap.get(key).addAll(s.nodes);
        }

        // Keep a track of nodes added
        nodeSet.addAll(s.nodes);
        i.remove(); // Remove from the level investigation
      }
    }

    // Assign a Demeter Tag to create a new group after the end of the transaction
    Long comId = 0L;
    String comIdPrefix = String.format("%sExternal_%s_", DEMETER_LEVEL_TAG, levelName);
    for (Set<Node> nodes : communityMap.values()) {
      String tag = String.format("%s%d", comIdPrefix, comId);
      communityNodeMap.put(tag, nodes);
      comId++;
    }

    Long potpourri = -1L;
    String tagPotpourri = String.format("%s%d", comIdPrefix, potpourri);
    // For remaining community append them. Keep the big ones as-is, merge the smallest in a
    // potpourri
    i = simList.iterator(); // Reinitialize iterator
    while (i.hasNext()) {
      SimClass s = i.next();
      // Do something
      s.nodes.remove(nodeSet);

      // Put the small ones in the potpourri
      if (s.nodes.size() <= MIN_CLIQUE_SIZE) {
        communityNodeMap.put(tagPotpourri, new HashSet<>(s.nodes));
      } else {
        // Let the big one in their one community
        String tag = String.format("%s%d", comIdPrefix, comId);
        communityNodeMap.put(tag, new HashSet<>(s.nodes));
      }

      nodeSet.addAll(s.nodes);
    }

    neo4jAL.logInfo(String.format("%d communities will be created.", communityNodeMap.size()));

    // Group the objects by communities


    // Apply tags on objects
    List<Node> returnList = new ArrayList<>();
    for (Map.Entry<String, Set<Node>> en : communityNodeMap.entrySet()) {
      String toApplyTag = en.getKey();

      List<String> groupTags;
      for (Node n : en.getValue()) {

        // check if tags exists
        groupTags = List.of(toApplyTag);
        if (n.hasProperty("Tags")) {
          try {
            List<String> tags = (List<String>) n.getProperty("Tags");
            groupTags.addAll(tags);
          } catch (Exception ignored) {
            // Skipping this step, since the Tag property is apparently poorly formatted
          }
        }

        n.setProperty("Tags", groupTags.toArray(new String[0]));
      }
      returnList.addAll(en.getValue());
    }

    return returnList;
  }

  /**
   * Perform  a label propagation on the Transactions in on application. And flag the transactions with a community id
   * @throws Neo4jQueryException
   */
  private void transactionLabelPropagation() throws Neo4jQueryException {
    neo4jAL.logInfo("Starting the Label Propagation on transaction..");

    // Link transactions by similarity
    neo4jAL.logInfo("Grouping Transaction by similarity ( this step can take a while )...");
    this.linkTransactions();

    String reqTransaction =
            String.format(
                    "MATCH (t:Transaction:`%1$s`)-[:Contains]->(o:Object:`%1$s`) "
                            + "WITH t, COLLECT(o) as objects "
                            + "WHERE SIZE(objects) > 10 "
                            + "RETURN DISTINCT t as transaction",
                    application);
    Result res = neo4jAL.executeQuery(reqTransaction);

    // Extract the result of the transaction query
    // And assign a unique label
    Long label = 0L;
    List<Node> transactionList = new ArrayList<>();
    while (res.hasNext()) {
      Node n = (Node) res.next().get("transaction");
      // Assign label + increment to get an unique label
      n.setProperty(TRANSACTION_COMMUNITY, label);
      label++;
      this.transactionList.add(n);
    }

    neo4jAL.logInfo(
            String.format(
                    "%d Transactions have been discovered ( with filtering ) and %d labels applied.",
                    this.transactionList.size(), label));

    // Propagate the labels
    int modifications = 0;
    int maxIteration = 5;

    // Parse the nodes and compute the next label of the node.
    for (int actualIt = 0; actualIt < maxIteration; actualIt++) {

      // New results are stored in a map to avoid changing an iteration while it's running
      Map<Node, Long> nodeLabelMap = new HashMap<>();
      // Parse the node and get the new label for each node
      for (Node n : this.transactionList) {
        // Get the most present label around
        Long newLabel =
                this.getNeighborsLabel(
                        n,
                        "Transaction",
                        Direction.INCOMING,
                        SIMILARITY_LINK,
                        WEIGHT_PROPERTY,
                        TRANSACTION_COMMUNITY);
        Long lastLabel = (Long) n.getProperty(TRANSACTION_COMMUNITY);

        if (newLabel == null) continue;
        if (!newLabel.equals(lastLabel))
          modifications++; // count the modification during this iteration
        // Reassign the node

        nodeLabelMap.put(n, newLabel);
      }

      // Reassign the correct label
      for (Map.Entry<Node, Long> en : nodeLabelMap.entrySet()) {
        en.getKey().setProperty(TRANSACTION_COMMUNITY, en.getValue());
      }

      neo4jAL.logInfo(
              String.format(
                      "Iterations %d on %d (max) - Modifications : %d ",
                      actualIt, maxIteration, modifications));
    }

  }

  /**
   * Perform a label propagation on the transactions
   *
   * @throws Neo4jQueryException
   */
  private void extractByTransactions() throws Neo4jQueryException {

    // Display the results
    Map<Long, List<Node>> communityResults = new HashMap<>();
    for (Node n : this.transactionList) {
      Long lastLabel = (Long) n.getProperty(TRANSACTION_COMMUNITY);

      if (!communityResults.containsKey(lastLabel))
        communityResults.put(lastLabel, new ArrayList<>());
      communityResults.get(lastLabel).add(n);
    }

    // Display the map of community
    neo4jAL.logInfo("Community map :");
    for (Map.Entry<Long, List<Node>> en : communityResults.entrySet()) {
      neo4jAL.logInfo(
              String.format(
                      "Id of the community : %d , size of the community : %d",
                      en.getKey(), en.getValue().size()));
    }

    // Merge nodes under the community
    neo4jAL.logInfo("Assign new labels to objects");
    for (Node transaction : transactionList) {

      // Match  the objects in the transaction with the specific level
      Long transactionLabel = Neo4jTypeManager.getAsLong(transaction, TRANSACTION_COMMUNITY, null);
      if (transactionLabel == null) continue;

      String req =
          String.format(
              "MATCH (t:Transaction)-[]->(o:Object) "
                  + "WHERE ID(t)=$idTransaction AND o.Level=$levelName  "
                  + "SET o.%s=$newLabel ",
              COMMUNITY);
      Map<String, Object> params =
          Map.of(
              "idTransaction",
              transaction.getId(),
              "levelName",
              level.getProperty("Name"),
              "newLabel",
              transactionLabel);
      neo4jAL.executeQuery(req, params);
    }

  }

  /**
   * Assign to all the nodes a property to allow the drilldown.
   * Small communities will be merged in DEFAULT
   *
   * @return
   * @throws Neo4jQueryException
   */
  private void assignDrilldownNodes() throws Neo4jQueryException {
    String drillDownProperty = "DrillDown";
    String defaultGroup = "DEFAULT";
    Long minCliqueSize = 30L;

    Long numNode = 0L;

    // Initialize all nodes to DEFAULT
    String iniReq = String.format("MATCH (l:Level5:%1$s)-[]->(o:Object) " +
                "WHERE ID(l)=$idLevel " +
                "SET o.%2%s=$defaultValue", application, drillDownProperty);
    Map<String, Object> iniParams =
            Map.of(
                    "defaultValue",
                    defaultGroup,
                    "idLevel",
                    level.getId());

    neo4jAL.executeQuery(iniReq, iniParams);
    neo4jAL.logInfo(String.format("DEBUG : Exploring %d transactions ", transactionList.size()));

    // Transactions List
    int success = 0, errors = 0;
    Map<Long, Set<Node>> communityMap = new HashMap<>();
    for(Node transaction : this.transactionList) {

      String req =
              String.format("MATCH (t:Transaction)-[]->(o:Object)<-[]-(l:Level5) " +
                      "WHERE ID(t)=$idTransaction AND ID(l)=$idLevel " +
                      "RETURN t.%1$s as comId, COLLECT(DISTINCT o) as objects", TRANSACTION_COMMUNITY, drillDownProperty);


      Map<String, Object> params =
              Map.of(
                      "idTransaction",
                      transaction.getId(),
                      "idLevel",
                      level.getId());


      Result res = neo4jAL.executeQuery(req, params);
      while (res.hasNext()) {
        try {
          Map<String, Object> r = res.next();

          // Verify that the community Id is a long, otherwise skip it
          Long idCom =  (Long) r.get("comId");;

          if(!communityMap.containsKey(idCom)) communityMap.put(idCom, new HashSet<>());
          communityMap.get(idCom).addAll((List<Node>) r.get("objects"));
          success++;
        } catch (Exception ignored) {
          errors++; // Ignore but count errors
          continue;
        }
      }
    }

    neo4jAL.logInfo(String.format("Drilldown communities identified. %d successfully discovered, %d error during processing.", success, errors));

    // Treat community map, and assign drilldown
    for(Map.Entry<Long, Set<Node>> en : communityMap.entrySet()) {
      String drillDownProp = defaultGroup;

      if(en.getValue().size() > minCliqueSize) {
        // If the size of the clique is sufficient flag
        // Else apply default property
        drillDownProp = String.format("Cluster_%d", en.getKey());
      }

      for(Node n: en.getValue()) {
        // Apply the new Drilldown prop on the nodes
        n.setProperty(drillDownProperty, drillDownProp);
        numNode++;
      }

    }

    neo4jAL.logInfo(String.format("%d nodes drilldown property have been changed.", numNode));
  }

  /**
   * Color the nodes on the graph
   *
   * @param colorProperty Property used to color the node
   * @throws Neo4jQueryException
   */
  private int colorNodes(String colorProperty) throws Neo4jQueryException {
    Map<String, Object> params = Map.of("idNode", level.getId());

    // Reset the color of the current level
    String req =
        String.format(
            "MATCH (l:Level5)-[]->(o:Object) WHERE ID(l)=$idNode "
                + "SET o.Color='rgb(182, 0, 255)'");
    neo4jAL.executeQuery(req, params);

    // Get the nodes with the community property
    req =
        String.format(
            "MATCH (l:Level5)-[]->(o:Object) WHERE ID(l)=$idNode AND EXISTS(o.%1$s) "
                + "RETURN DISTINCT o as node",
            COMMUNITY);

    // Get all community
    Result res = neo4jAL.executeQuery(req, params);
    int it = 0;
    while (res.hasNext()) {
      Node n = (Node) res.next().get("node");

      Long comId = Neo4jTypeManager.getAsLong(n, COMMUNITY, null);
      if (comId == null) continue; // Ignore the node if the community property isn't valid

      Color act = COLOR_TABLE[it % COLOR_TABLE.length];
      it++;

      // Generate and apply color
      String color = String.format("rgb(%d, %d, %d)", act.getRed(), act.getGreen(), act.getBlue());
      String reqCol =
          String.format(
              "MATCH (l:Level5)-[]->(o:Object) WHERE ID(l)=$idNode AND o.%1$s=$comId "
                  + "SET o.Color=$color ",
              COMMUNITY);
      params = Map.of("idNode", level.getId(), "color", color, "comId", comId);
      neo4jAL.executeQuery(reqCol, params);
    }

    return it;
  }

  /**
   * Search in SimClass map a corresponding key
   *
   * @param map Map<Set<Node>, Set<Node>>
   * @param simClass Search a specific class
   * @return
   */
  private Set<Node> searchInLevelMap(Map<Set<Node>, Set<Node>> map, SimClass simClass) {
    // Search level 1 &  Search level 2
    for (Set<Node> en : map.keySet()) {
      if (en.contains(simClass.level1) || en.contains(simClass.level2)) {

        // verify if the size will not exceed the maximum clique size
        if (simClass.nodes.size() + map.get(en).size() < MAX_CLIQUE_SIZE) {
          return en;
        }

        // Else find the next good candidate
      }
    }

    return null;
  }

  /**
   * Links the transactions together based on their similarity
   *
   * @throws Neo4jQueryException
   */
  private void linkTransactions() throws Neo4jQueryException {
    long start = System.currentTimeMillis();
    // Remove all previous links between transactions
    String req =
        String.format(
            "MATCH (n:Transaction:`%s`)-[r:%s]-() DELETE r", application, SIMILARITY_LINK);
    neo4jAL.executeQuery(req);

    // Get a map of the transactions
    // Small transaction are ignored
    String reqLink =
        String.format(
            "MATCH (t:Transaction:`%1$s`)-[:Contains]->(o:Object:`%1$s`) "
                + "WITH t, COLLECT(o) as objects "
                + "WHERE SIZE(objects) > 10 "
                + "UNWIND objects as o "
                + "RETURN DISTINCT t as transaction, o as node",
            application);
    Result result = neo4jAL.executeQuery(reqLink);

    Map<Node, List<Node>> transactionMap = new HashMap<>();
    while (result.hasNext()) {
      Map<String, Object> r = result.next();
      Node transaction = (Node) r.get("transaction");
      Node n = (Node) r.get("node");

      if (!transactionMap.containsKey(transaction))
        transactionMap.put(transaction, new ArrayList<>());
      transactionMap.get(transaction).add(n);
    }

    // Counter to avoid waiting an eternity without logging
    int stepMax = transactionMap.size() * transactionMap.size();
    int step = 0;

    // Merge transaction by similarity
    for (Map.Entry<Node, List<Node>> en : transactionMap.entrySet()) {
      for (Map.Entry<Node, List<Node>> enIt : transactionMap.entrySet()) {

        step++;
        if (step % 10000 == 0)
          neo4jAL.logInfo(String.format("Treating transaction %d on %d.", step, stepMax));

        // check if the transaction has a link to other in the application
        Node source = en.getKey();
        Node dest = enIt.getKey();

        // If treating the same node skip this step
        if (source.getId() == dest.getId()) continue;

        //
        boolean existLink = false;
        // Check relationships
        for (Relationship rel :
            source.getRelationships(
                Direction.OUTGOING, RelationshipType.withName(SIMILARITY_LINK))) {
          Node otherNode = rel.getOtherNode(source);
          if (otherNode.getId() == dest.getId()) existLink = true;
        }

        // If a link exist skip
        if (existLink) continue;

        // Else check common nodes
        List<Node> common = new ArrayList<>(en.getValue());
        common.removeAll(enIt.getValue());

        Long commonNode = (long) en.getValue().size() - (long) common.size();
        Double percentage = exp((double) commonNode / (double) common.size());

        // If no nodes are shared, skip it
        if (percentage == 0 || percentage.isInfinite()) continue;

        Relationship rel =
            source.createRelationshipTo(dest, RelationshipType.withName(SIMILARITY_LINK));
        rel.setProperty(WEIGHT_PROPERTY, percentage);
      }
    }

    long finish = System.currentTimeMillis();
    long timeElapsed = finish - start;

    neo4jAL.logInfo(String.format("( %d ms ) Transactions were linked.", timeElapsed));
  }

  /**
   * Get the maximum label between the neighbors of one specific node
   *
   * @param n Node to explore
   * @param label Label of the nodes
   * @param direction Direction of the relationship
   * @param relationshipName Name of the relationship
   * @param weight Weight property
   * @param labelProperty Name of the property in charge of the label (Label of the label
   *     propagation not of neo4j)
   * @return The label most present in the values
   * @throws Neo4jQueryException
   */
  private Long getNeighborsLabel(
      Node n,
      String label,
      Direction direction,
      String relationshipName,
      String weight,
      String labelProperty)
      throws Neo4jQueryException {

    if (!relationshipName.isBlank()) {
      relationshipName = ":" + relationshipName;
    }

    // Change the direction of the relationship depending on the parameter
    String directionAsReq = String.format("-[r%s]->", relationshipName);
    switch (direction) {
      case BOTH:
        directionAsReq = String.format("-[r%s]-", relationshipName);
        break;
      case INCOMING:
        directionAsReq = String.format("<-[r%s]-", relationshipName);
        break;
      case OUTGOING:
        directionAsReq = String.format("-[r%s]->", relationshipName);
        break;
    }

    // Retrieve all the neighbors of one specific node
    String req =
        String.format(
            "MATCH (t:`%1$s`)%2$s(other:`%1$s`) "
                + "WHERE ID(t)=$idNode AND ID(t)<>ID(other) "
                + "RETURN DISTINCT other as node, r.%3$s as weight",
            label, directionAsReq, weight);
    Map<String, Object> params = Map.of("idNode", n.getId());
    Result res = neo4jAL.executeQuery(req, params);

    Map<Long, Long> labelMap = new HashMap<>();

    // Iterate over the results and store the weights
    while (res.hasNext()) {
      Map<String, Object> r = res.next();
      Long value;
      try {
        value = (Long) r.get("weight");
      } catch (Exception e) {
        value = ((Double) r.get("weight")).longValue();
      }

      Node node = (Node) r.get("node");
      Long labelTransaction = (Long) node.getProperty(labelProperty);

      if (!labelMap.containsKey(labelTransaction))
        labelMap.put(labelTransaction, 0L); // If not exist initialize
      labelMap.put(
          labelTransaction,
          labelMap.get(labelTransaction) + value); // if exist add the value to the label
    }

    // Get the highest value in the map and return the key
    Long maxLabel = null;
    long maxValue = 0L;
    for (Map.Entry<Long, Long> en : labelMap.entrySet()) {
      if (maxValue < en.getValue()) {
        maxValue = en.getValue();
        maxLabel = en.getKey();
      }
    }

    return maxLabel;
  }

  class SimClass {
    Node level1;
    Node level2;
    Long size;
    Double simPercentage;
    List<Node> nodes;

    public SimClass(Node level1, Node level2, Double simPercentage, Long size, List<Node> nodes) {
      this.level1 = level1;
      this.level2 = level2;
      this.simPercentage = simPercentage;
      this.size = size;
      this.nodes = nodes;
    }

    @Override
    public int hashCode() {
      return Objects.hash(level1, level2, size, simPercentage);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SimClass simClass = (SimClass) o;
      return level1.equals(simClass.level1)
          && level2.equals(simClass.level2)
          && size.equals(simClass.size)
          && simPercentage.equals(simClass.simPercentage);
    }
  }
}
