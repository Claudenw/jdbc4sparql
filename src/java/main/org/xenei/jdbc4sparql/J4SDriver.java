package org.xenei.jdbc4sparql;

import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.logging.Logger;

import org.apache.commons.discovery.resource.ClassLoaders;
import org.apache.commons.discovery.tools.DiscoverClass;
import org.apache.commons.discovery.tools.DiscoverSingleton;
import org.xenei.jdbc4sparql.sparql.builders.SchemaBuilder;
import org.xenei.jdbc4sparql.sparql.builders.SimpleBuilder;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;

public class J4SDriver implements Driver
{

	public J4SDriver()
	{
	}

	@Override
	public boolean acceptsURL( final String url ) throws SQLException
	{
		try {
			new J4SURL( url );
			return true;
		}
		catch (IllegalArgumentException e)
		{
			return false;
		}
	}

	@Override
	public Connection connect( final String urlStr, final Properties props )
			throws SQLException
	{
		J4SURL url = null;
		
		try {
			url = new J4SURL( urlStr );
		}
		catch (IllegalArgumentException e )
		{
			return null; // return null if the URL is not valid for us.
		}
		try {
		return new J4SConnection(this, url, props);
		}
		catch (MalformedURLException e)
		{
			throw new SQLException( e );
		}
	}

	@Override
	public int getMajorVersion()
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMinorVersion()
	{
		// TODO Auto-generated method stub
		return 1;
	}

	public String getName()
	{
		return "Jdbc 4 Sparql Driver";
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo( final String url,
			final Properties info ) throws SQLException
	{
		try {
			new J4SURL( url );
			return new DriverPropertyInfo[0];
		}
		catch (IllegalArgumentException e )
		{
			throw new SQLException( e );
		}
	}

	@Override
	public boolean jdbcCompliant()
	{
		return false;
	}
	
	public static void main( String[] args )
	{
	
		J4SDriver driver = new J4SDriver();
		System.out.println( String.format( "%s Version %s.%s", driver.getName(), driver.getMajorVersion(), driver.getMinorVersion()));
		
		System.out.println( "Registered Schema Builders:");
		System.out.print( "(Default) ");
		List<Class<? extends SchemaBuilder>> builderList = SchemaBuilder.Util.getBuilders();
		for (Class<? extends SchemaBuilder> c : builderList)
		{
			System.out.println( SchemaBuilder.Util.getName(c)+": "+SchemaBuilder.Util.getDescription(c));
		}
		System.out.println();
		System.out.println("Registered SPARQL parsers:");
		System.out.println("(Default)");
		List<Class<? extends SparqlParser>> parserList = SparqlParser.Util.getParsers();
		for (Class<? extends SparqlParser> c : parserList)
		{
			System.out.println( SparqlParser.Util.getName(c)+": "+SparqlParser.Util.getDescription(c));
		}
	}

}
