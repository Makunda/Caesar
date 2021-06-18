package com.castsoftware.caesar.workspace;

import com.castsoftware.caesar.configuration.Configuration;
import com.castsoftware.caesar.configuration.DetectionConfiguration;
import com.castsoftware.caesar.configuration.NodeConfiguration;
import com.castsoftware.caesar.database.Neo4jAL;
import com.castsoftware.caesar.exceptions.file.FileCorruptedException;
import com.castsoftware.caesar.exceptions.file.MissingFileException;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jBadRequestException;
import com.castsoftware.caesar.exceptions.neo4j.Neo4jQueryException;
import com.castsoftware.caesar.exceptions.workspace.MissingWorkspaceException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;


public class Workspace {

	private static Workspace INSTANCE = null;

	private Neo4jAL neo4jAL;
	private NodeConfiguration nodeConfiguration;
	private Map<String, Object> configuration = new HashMap<>();
	private Path configurationFilePath;
	private String workspace;

	/**
	 * Singleton
	 */
	private Workspace(Neo4jAL neo4jAL) throws Neo4jQueryException, Neo4jBadRequestException, MissingWorkspaceException, IOException {
		this.neo4jAL = neo4jAL;

		try {
			this.nodeConfiguration = NodeConfiguration.getInstance(neo4jAL);
			this.workspace = this.nodeConfiguration.getWorkspace();
		} catch (Neo4jBadRequestException | Neo4jQueryException e) {
			neo4jAL.logError("Failed to instantiate the Workspace class. Failed to retrieve workspace from Configuration node", e);
			throw e;
		}
	}

	/**
	 * Get the configuration of the extensions
	 * @return
	 * @throws IOException
	 * @throws MissingWorkspaceException
	 */
	public DetectionConfiguration getConfiguration() throws IOException, MissingWorkspaceException, FileCorruptedException, MissingFileException {
		// Load the configuration file from the workspace
		// Get the path to the configuration File and make sure the internal configuration is valid.
		Path pathConfig;
		try {
			pathConfig = Paths.get(workspace, Configuration.get("workspace.configuration.file"));
		} catch (Exception err) {
			throw new MissingWorkspaceException("Failed to get the path of the configuration file", "WORKSxCONS02");
		}

		// Configuration  from workspace
		Map<String, Object> configurationAsJson;
		try {
			configurationAsJson = this.readConfigurationFile(pathConfig);
			return new DetectionConfiguration(configurationAsJson);
		} catch (Exception | FileCorruptedException | MissingWorkspaceException err) {
			neo4jAL.logError("Failed to load configuration from the workspace. Will use the default configuration.", err);
		}


		// Load the default configuration
		try {
			Map<String, Object> mapConfig = this.readPropertyFile();
			return new DetectionConfiguration(mapConfig);
		} catch (Exception | MissingFileException err) {
			neo4jAL.logError("Failed to load default configuration. Aborting.", err);
			throw err;
		}
	}

	/**
	 * Change the path of the workspace
	 * @param newPath New path to be set
	 * @throws MissingWorkspaceException If the path is not pointing to a valid workspace
	 */
	public void setWorkspace(Path newPath) throws MissingWorkspaceException, Exception {
		// Verify the presence of configuration file
		Path pathToconfig = newPath.resolve(Configuration.get("workspace.configuration.file"));
		if(!Files.exists(pathToconfig)) {
			throw new MissingWorkspaceException(String.format("No configuration file exists at the specified path '%s'", this.configurationFilePath), "WORKSxSETP01");
		}

		// Valid path
		String absolutePath = newPath.toAbsolutePath().toString();
		this.nodeConfiguration.setWorkspace(absolutePath);
		this.workspace = absolutePath;
	}

	/**
	 * Get the workspace
	 * @return Workspace of Caesar
	 */
	public String getWorkspace() {
		return workspace;
	}

	/**
	 * Get the instance of the workspace
	 * @return  Workspace Object
	 */
	public static Workspace getInstance(Neo4jAL neo4jAL) throws Neo4jBadRequestException, Neo4jQueryException, MissingWorkspaceException, IOException {
		if (INSTANCE == null) {
			INSTANCE = new Workspace(neo4jAL);
		}

		return INSTANCE;
	}

	/**
	 * Read the configuration of the file if the file exists and can be read
	 * @throws IOException
	 */
	private Map<String, Object> readConfigurationFile(Path pathToFile) throws IOException, MissingWorkspaceException {

		// Check if the file exist in the folder
		if (!Files.exists(pathToFile))
			throw new MissingWorkspaceException(String.format("The configuration file doesn't exist at the specified path '%s'", this.configurationFilePath), "WORKSxCONS02");

		// Read the content
		File initialFile = pathToFile.toFile();

		try (InputStream targetStream = new FileInputStream(initialFile)) {
			return new ObjectMapper().readValue(targetStream, HashMap.class);
		} catch (Exception err) {
			this.neo4jAL.logError("Failed to read the configuration. Make sure the configuration is a valid JSON file");
			throw err;
		}
	}

	/**
	 * Read the configuration from the property file
	 * @return the Configuration as a Hashmap
	 * @throws IOException
	 * @throws MissingFileException
	 */
	private Map<String, Object> readPropertyFile() throws IOException, MissingFileException {
		try (InputStream input =
					 Workspace.class.getClassLoader().getResourceAsStream("default_configuration.json")) {

			if (input == null) {
				throw new MissingFileException(
						"No file 'default_configuration.json' was found.",
						"resources/default_configuration.json",
						"WORKxREAP1");
			}

			return new ObjectMapper().readValue(input, HashMap.class);
		} catch (IOException | MissingFileException  ex) {
			throw ex;
		}
	}


}
