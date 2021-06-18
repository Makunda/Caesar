package com.castsoftware.caesar.configuration;

import com.castsoftware.caesar.exceptions.file.FileCorruptedException;

import java.util.Map;

public class DetectionConfiguration {
	private Long minCliqueSize;
	private Long maxCliqueSize;
	private Long minDrillDownSize;
	private Integer labelPropagationIteration;
	private Double minSimilarityMerge;
	private String demeterLevelTag;
	private String similarityLink;
	private String community;
	private String transactionCommunity;
	private String weightProperty;

	public Long getMinCliqueSize() {
		return minCliqueSize;
	}

	public Long getMaxCliqueSize() {
		return maxCliqueSize;
	}

	public Long getMinDrillDownSize() {
		return minDrillDownSize;
	}

	public String getDemeterLevelTag() {
		return demeterLevelTag;
	}

	public Double getMinSimilarityMerge() {
		return minSimilarityMerge;
	}

	public String getSimilarityLink() {
		return similarityLink;
	}

	public String getCommunity() {
		return community;
	}

	public String getTransactionCommunity() {
		return transactionCommunity;
	}

	public String getWeightProperty() {
		return weightProperty;
	}

	public Integer getLabelPropagationIteration() {
		return labelPropagationIteration;
	}

	/**
	 * Constructor
	 * @param json Json file content
	 * @throws FileCorruptedException If the Json is invalid
	 */
	public DetectionConfiguration(Map<String, Object> json) throws FileCorruptedException {
		try {
			this.minCliqueSize = ((Number) json.get("MIN_CLIQUE_SIZE")).longValue();
			this.maxCliqueSize = ((Number) json.get("MAX_CLIQUE_SIZE")).longValue();
			this.minDrillDownSize = ((Number) json.get("MIN_DRILLDOWN_CLIQUE_SIZE")).longValue();
			this.minSimilarityMerge = ((Number) json.get("MIN_SIMILARITY_MERGE")).doubleValue();
			this.labelPropagationIteration = ((Number) json.get("LABEL_PROPAGATION_ITERATIONS")).intValue();

			this.demeterLevelTag = (String) json.get("DEMETER_LEVEL_TAG");
			this.similarityLink = (String) json.get("SIMILARITY_LINK");
			this.community = (String) json.get("COMMUNITY");
			this.transactionCommunity = (String) json.get("TRANSACTION_COMMUNITY");
			this.weightProperty = (String) json.get("WEIGHT_PROPERTY");

		} catch (Exception err) {
			throw new FileCorruptedException("Failed create DetectionConfiguration due to corrupted json file", "DETCxCONS01");
		}
	}
}
