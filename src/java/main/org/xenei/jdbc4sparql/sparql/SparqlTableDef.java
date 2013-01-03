package org.xenei.jdbc4sparql.sparql;

import java.util.ArrayList;
import java.util.List;

import org.xenei.jdbc4sparql.impl.TableDefImpl;

public class SparqlTableDef extends TableDefImpl
{

	/**
	 * Query segments are string format strings where
	 * %1$s = table variable name
	 * %2$s = table FQ name.
	 * Multiple lines may be added. They will be added to the sparql query when
	 * the table is used.
	 * The string must have the form of a triple: S, P, O
	 * the components of the triple other than %1$s and %2$s must be fully
	 * qualified.
	 */
	private final List<String> querySegments;

	public SparqlTableDef( final String namespace, final String name,
			final String querySegment )
	{
		super(namespace, name);
		this.querySegments = new ArrayList<String>();
		this.querySegments.add(querySegment);
	}

	public void addQuerySegment( final String querySegment )
	{
		querySegments.add(querySegment);
	}

	public List<String> getQuerySegments()
	{
		return querySegments;
	}
}
