package org.xenei.jdbc4sparql.impl;

import com.hp.hpl.jena.sparql.core.Var;

import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ItemName;
import org.xenei.jdbc4sparql.iface.NamedObject;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableName;

public class NameUtils
{
	public static final String DB_DOT = ".";

	public static final String SPARQL_DOT = "\u00B7";

	public static String convertDB2SPARQL( final String dbName )
	{
		return dbName.replace(NameUtils.DB_DOT, NameUtils.SPARQL_DOT);
	}

	public static String convertSPARQL2DB( final String dbName )
	{
		return dbName.replace(NameUtils.SPARQL_DOT, NameUtils.DB_DOT);
	}

	public static String createUUIDName()
	{
		return ("v_" + UUID.randomUUID().toString()).replace("-", "_");
	}

	public static String getCursorName( TableName name )
	{
		return "CURSOR_" + name.createName( "_" );
	}

	public static String getCursorName( final Table<?> t )
	{
		return NameUtils.getCursorName( t.getName());
	}

	public static String getDBName( final NamedObject<?> namedObject )
	{
		return namedObject.getName().getDBName();
	}

	public static String getDBName( final Var var )
	{
		return var.getName().replace(NameUtils.SPARQL_DOT, NameUtils.DB_DOT);
	}

	public static String getSPARQLName( final NamedObject<?> namedObject )
	{
		return namedObject.getName().getSPARQLName();
	}

	

}
