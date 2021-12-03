package com.castsoftware.caesar.configuration;

public enum Language {
	JAVA("Java"),
	NET("Net");

	public String language;

	@Override
	public String toString() {
		return this.language;
	}

	Language(String lang) {
		this.language = lang;
	}
}
