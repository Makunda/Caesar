package com.castsoftware.caesar.services.textProcessing;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class Lemmatizer {
	protected StanfordCoreNLP pipeline;
	protected static Lemmatizer INSTANCE = null;

	/**
	 * Get the instance of the Lemmatizer
	 * @return The instance
	 */
	public static Lemmatizer getInstance() {
		if (INSTANCE == null) {
			INSTANCE = new Lemmatizer();
		}

		return INSTANCE;
	}

	private Lemmatizer() {
		Properties props;
		props = new Properties();
		props.put("annotators", "tokenize, ssplit, pos, lemma");

		this.pipeline = new StanfordCoreNLP(props);
	}

	/**
	 * Lemmatize the word found
	 * @param word Word to change
	 * @return The lemma
	 */
	public String lemmatize(String word)
	{
		// create a document object
		CoreDocument document = pipeline.processToCoreDocument(word);
		// display tokens
		for (CoreLabel tok : document.tokens()) {
			return tok.lemma();
		}

		return word; // Nothing found
	}
}
