package org.xenei.jdbc4sparql.sparql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;

public class SparqlStatement implements Statement
{
	private final SparqlParser parser;

	SparqlStatement( final SparqlCatalog catalog, final SparqlParser parser )
	{
		this.parser = parser;
	}

	@Override
	public void addBatch( final String arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void cancel() throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void clearBatch() throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void clearWarnings() throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void closeOnCompletion() throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public boolean execute( final String arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean execute( final String arg0, final int arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean execute( final String arg0, final int[] arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean execute( final String arg0, final String[] arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int[] executeBatch() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet executeQuery( final String query ) throws SQLException
	{
		final SparqlQueryBuilder builder = parser.parse(query);
		return new SparqlView(builder).getResultSet();
	}

	@Override
	public int executeUpdate( final String arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int executeUpdate( final String arg0, final int arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int executeUpdate( final String arg0, final int[] arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int executeUpdate( final String arg0, final String[] arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Connection getConnection() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getFetchDirection() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getFetchSize() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getMaxFieldSize() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxRows() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean getMoreResults() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean getMoreResults( final int arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int getQueryTimeout() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ResultSet getResultSet() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getResultSetConcurrency() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getResultSetHoldability() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getResultSetType() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getUpdateCount() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isClosed() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isPoolable() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isWrapperFor( final Class<?> arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setCursorName( final String arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void setEscapeProcessing( final boolean arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void setFetchDirection( final int arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void setFetchSize( final int arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void setMaxFieldSize( final int arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void setMaxRows( final int arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void setPoolable( final boolean arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void setQueryTimeout( final int arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public <T> T unwrap( final Class<T> arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

}
