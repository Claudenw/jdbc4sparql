package org.xenei.jdbc4sparql.mock;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import org.xenei.jdbc4sparql.J4SDriver;

public class MockDriver extends J4SDriver
{

	public MockDriver()
	{
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean acceptsURL( final String arg0 ) throws SQLException
	{
		return true;
	}

	@Override
	public Connection connect( final String arg0, final Properties arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getMajorVersion()
	{
		return 1;
	}

	@Override
	public int getMinorVersion()
	{
		return 2;
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo( final String arg0,
			final Properties arg1 ) throws SQLException
	{
		return new DriverPropertyInfo[0];
	}

	@Override
	public boolean jdbcCompliant()
	{
		// TODO Auto-generated method stub
		return false;
	}

}
