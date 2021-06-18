package com.castsoftware.caesar.exceptions.workspace;

import com.castsoftware.caesar.exceptions.ExtensionException;

public class MissingWorkspaceException  extends ExtensionException {

	private static final long serialVersionUID = -729600315555876926L;
	private static final String MESSAGE_PREFIX = "Error, reaching the workspace : ";
	private static final String CODE_PREFIX = "MISS_WS_";

	/**
	 * Thrown the workspace isn't set properly
	 * @param message Message to be displayed
	 * @param code Error code
	 */
	public MissingWorkspaceException(String message, String code) {
		super(MESSAGE_PREFIX.concat(message), CODE_PREFIX.concat(code));
	}

	/**
	 * Thrown if a File is missing in the Workspace
	 * @param message Message to be displayed
	 * @param path Missing file
	 * @param code Error Code
	 */
	public MissingWorkspaceException(String message, String path, String code) {
		super(
				MESSAGE_PREFIX.concat(message).concat(". Path : ").concat(path),
				CODE_PREFIX.concat(code));
	}
}
