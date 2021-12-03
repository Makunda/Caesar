package com.castsoftware.caesar.entities.microservice;

import org.neo4j.graphdb.Node;

import java.util.List;
import java.util.Objects;

/**
 * Bind between an entry point and an endpoint
 */
public class FlowBind {
	public Node entry;
	public List<Node> endpoints;

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		FlowBind flowBind = (FlowBind) o;
		return entry.equals(flowBind.entry) &&
				endpoints.equals(flowBind.endpoints);
	}

	@Override
	public int hashCode() {
		return Objects.hash(entry, endpoints);
	}

	public FlowBind(Node entry, List<Node> endpoints) {
		this.entry = entry;
		this.endpoints = endpoints;
	}
}
