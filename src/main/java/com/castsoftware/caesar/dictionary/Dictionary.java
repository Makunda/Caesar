package com.castsoftware.caesar.dictionary;

import com.castsoftware.caesar.configuration.Configuration;
import com.castsoftware.caesar.exceptions.file.MissingFileException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

public class Dictionary {

	private static Dictionary INSTANCE = null;
	private Set<String> items;

	/**
	 * Get the dictionary instance
	 * @return
	 */
	public static Dictionary getInstance() {
		if(INSTANCE == null) {
			INSTANCE = new Dictionary();
		}

		return INSTANCE;
	}

	/**
	 * Search an item in the dictionnary
	 * @param item Item
	 * @return
	 */
	public Boolean search(String item) {
		return this.items.contains(item);
	}

	private Dictionary() {
		// Initialize
		this.items = new HashSet<>();

		// Load file
		try (InputStream input =
					 Configuration.class.getClassLoader().getResourceAsStream("dictionary.txt")) {

			if (input == null) {
				throw new MissingFileException(
						"No file 'dictionary.txt' was found.",
						"resources/dictionary.txt",
						"DICTxLOAD1");
			}

			// Convert to buffer reader
			BufferedReader reader = new BufferedReader(new InputStreamReader(input));

			// Load line by line
			String item;
			while(reader.ready()) {
				item = reader.readLine();
				item = item.trim();
				this.items.add(item);
			}

		} catch (IOException | MissingFileException ex) {
			System.err.println(ex.getMessage());
		}
	}
}
