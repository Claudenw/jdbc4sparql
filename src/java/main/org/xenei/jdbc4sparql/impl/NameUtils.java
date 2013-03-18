package org.xenei.jdbc4sparql.impl;

import com.hp.hpl.jena.sparql.core.Var;

import org.apache.commons.lang3.StringUtils;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Table;

public class NameUtils
{
	public static final String DB_DOT=".";

	public static final String SPARQL_DOT = "\u00B7";
	
	private static String createName( String schema, String table, String col, String separator )
	{
		StringBuilder sb = new StringBuilder();
		
		if (StringUtils.isNotEmpty( schema ))
		{
			sb.append( schema ).append( separator);
		}
		
		if (StringUtils.isNotEmpty( table ) || sb.length()>0)
		{
			sb.append( table );
		}
		
		if (StringUtils.isNotEmpty( col ))
		{
			if (sb.length()>0)
			{
				sb.append( separator );
			}
			sb.append( col );
		}
		
		return sb.toString();
	}
	
	
	public static String getDBName(  String schema, String table, String col )
	{
		return createName( schema, table, col, DB_DOT );
	}
	
	public static String getDBName( Var var )
	{
		return var.getName().replace( SPARQL_DOT, DB_DOT );	
		
	}
	
	public static String getDBName( Column column )
	{
		return getDBName( column.getSchema().getLocalName(), 
				column.getTable().getLocalName(), 
				column.getLocalName());
	}
	
	public static String getDBName( Table table )
	{
		return getDBName( table.getSchema().getLocalName(), 
				table.getLocalName(), null );
	}
	
	public static String getSPARQLName( String schema, String table, String col )
	{
		return createName( schema, table, col,  SPARQL_DOT);
	}
	
	public static String getSPARQLName( Column column )
	{
		return getSPARQLName( column.getSchema().getLocalName(), 
				column.getTable().getLocalName(), 
				column.getLocalName());
	}
	
	public static String getSPARQLName( Table table )
	{
		return getSPARQLName( table.getSchema().getLocalName(), 
				table.getLocalName(), null );
	}
	
	public static String convertDB2SPARQL( String dbName )
	{
		return dbName.replace( DB_DOT, SPARQL_DOT );	
	}

	
}
