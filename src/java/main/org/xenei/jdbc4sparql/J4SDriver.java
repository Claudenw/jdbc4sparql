package org.xenei.jdbc4sparql;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class J4SDriver implements Driver
{

	
	public J4SDriver()
	{
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean acceptsURL( String arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Connection connect( String url, Properties props )
			throws SQLException
	{
		return new J4SConnection(this, url, props);
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
		return "JDBC4SPARQL Driver";
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo( String arg0, Properties arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean jdbcCompliant()
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException
	{
		// TODO Auto-generated method stub
		return null;
	}

}
