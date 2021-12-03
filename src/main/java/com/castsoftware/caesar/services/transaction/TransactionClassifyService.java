package com.castsoftware.caesar.services.transaction;

import com.castsoftware.caesar.dictionary.Dictionary;
import com.castsoftware.caesar.entities.transactions.ClusterTransaction;
import com.castsoftware.caesar.entities.transactions.Transaction;
import com.castsoftware.caesar.services.textProcessing.Lemmatizer;

import java.util.*;

public class TransactionClassifyService {
	private Dictionary dictionary;

	/**
	 * Sanitize the string
	 * @param str String to sanitize
	 * @return The sanitized string
	 */
	public String removeNonAlphanumeric(String str)
	{
		return str.replaceAll(
				"[^a-zA-Z0-9]", " ");
	}

	/**
	 * Classify the list of transaction
	 * @param tn Transaction
	 * @return List of categories
	 */
	private List<String> classifyPlainTransaction(Transaction tn) {
		List<String> categories = new ArrayList<>();
		String name = tn.getName().toLowerCase();
		name = removeNonAlphanumeric(name);

		String substring;

		String foundPrefix;
		String nextFoundPrefix;


		// Parse substring
		for(int i = 0; i < name.length(); i++) {
			foundPrefix  = null;

			substring = name.substring(i);
			foundPrefix = findLongestWord(substring);

			// If not found ?? skip
			if(foundPrefix == null) continue;
			nextFoundPrefix = findMasking(name, i);
			if(nextFoundPrefix != null) categories.add(nextFoundPrefix);

			// If found truncate the string found
			name = name.substring(foundPrefix.length() -1);
			categories.add(foundPrefix);
		}

		return categories;
	}

	/**
	 * Divide routes in different categories
	 * @param tn Transaction to classify
	 * @return The list of categories found
	 */
	private List<String> classifyRoute(Transaction tn) {
		List<String> returnItems = new ArrayList<>();

		String[] split = tn.getName().split("/");
		for(String item : split) {
			item = removeNonAlphanumeric(item);
			if(!item.isEmpty()) returnItems.add(item);
		}

		return returnItems;
	}


	/**
	 * Classify a transaction based on its name
	 * @param tn Transaction to classify
	 * @return The list of categories found
	 */
	public List<String> classifyTransaction(Transaction tn) {
		Lemmatizer lemmatizer = Lemmatizer.getInstance();
		String name = tn.getName();
		List<String> categories = new ArrayList<>();

		if(name.contains("/")) { // If route
			categories =  classifyRoute(tn);
		} else { // If plan text
			categories =  classifyPlainTransaction(tn);
		}

		categories.forEach(lemmatizer::lemmatize);
		return categories;
	}

	/**
	 * Find substring
	 * @param name Name to parse
	 * @param position Position
	 * @return
	 */
	public String findMasking(String name, int position) {
		String masked = null;

		// Go back
		String foundPrefix;
		String substring = "";
		for(int y = position; y >= name.length() -1; y --) {
			substring = name.substring(y, name.length() - 1);
			foundPrefix = findLongestWord(substring);

			if(foundPrefix != null) {
				return foundPrefix;
			}
		}

		return masked;
	}

	/**
	 * Search substring in the dictionary
	 * @param text Text to search
	 * @return The longest word found or null
	 */
	public String findLongestWord(String text) {
		// Search property
		StringBuilder longestString = new StringBuilder();
		String foundWord = null;

		// Loop on the text
		for(int i = 0; i < text.length(); i++) {
			longestString.append(text.charAt(i)); // Longest word

			// Search string in dict
			if(this.dictionary.search(longestString.toString())) {
				foundWord = longestString.toString();
			}
		}
		return foundWord;
	}




	public TransactionClassifyService() {
		this.dictionary = Dictionary.getInstance();
	}


}
