package org.xenei.jdbc4sparql.impl;

import com.hp.hpl.jena.sparql.core.Var;

import org.apache.commons.lang3.StringUtils;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Table;

public class NameUtils
{
	public static final String DB_DOT = ".";

	public static final String SPARQL_DOT = "\u00B7";

	public static String convertDB2SPARQL( final String dbName )
	{
		return dbName.replace(NameUtils.DB_DOT, NameUtils.SPARQL_DOT);
	}

	private static String createName( final String schema, final String table,
			final String col, final String separator )
	{
		final StringBuilder sb = new StringBuilder();

		if (StringUtils.isNotEmpty(schema))
		{
			sb.append(schema).append(separator);
		}

		if (StringUtils.isNotEmpty(table) || (sb.length() > 0))
		{
			sb.append(table);
		}

		if (StringUtils.isNotEmpty(col))
		{
			if (sb.length() > 0)
			{
				sb.append(separator);
			}
			sb.append(col);
		}

		return sb.toString();
	}

	public static String getDBName( final Column column )
	{
		return NameUtils.getDBName(column.getSchema().getName(), column
				.getTable().getName(), column.getName());
	}

	public static String getDBName( final String schema, final String table,
			final String col )
	{
		return NameUtils.createName(schema, table, col, NameUtils.DB_DOT);
	}

	public static String getDBName( final Table table )
	{
		return NameUtils.getDBName(table.getSchema().getName(),
				table.getName(), null);
	}

	public static String getDBName( final Var var )
	{
		return var.getName().replace(NameUtils.SPARQL_DOT, NameUtils.DB_DOT);

	}

	public static String getSPARQLName( final Column column )
	{
		return NameUtils.getSPARQLName(column.getSchema().getName(), column
				.getTable().getName(), column.getName());
	}

	public static String getSPARQLName( final String schema,
			final String table, final String col )
	{
		return NameUtils.createName(schema, table, col, NameUtils.SPARQL_DOT);
	}

	public static String getSPARQLName( final Table table )
	{
		return NameUtils.getSPARQLName(table.getSchema().getName(),
				table.getName(), null);
	}

}
