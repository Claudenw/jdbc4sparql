package org.xenei.jdbc4sparql;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class J4SConnection implements Connection
{
	private String catalog;
	private final J4SDriver driver;

	public J4SConnection( final J4SDriver driver, final String url,
			final Properties props )
	{
		this.driver = driver;
		// TODO Auto-generated constructor stub
	}

	@Override
	public void abort( final Executor arg0 ) throws SQLException
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
	public void commit() throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public Array createArrayOf( final String arg0, final Object[] arg1 )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Blob createBlob() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Clob createClob() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public NClob createNClob() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Statement createStatement() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Statement createStatement( final int arg0, final int arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Statement createStatement( final int arg0, final int arg1,
			final int arg2 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Struct createStruct( final String arg0, final Object[] arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean getAutoCommit() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getCatalog() throws SQLException
	{
		return catalog;
	}

	@Override
	public Properties getClientInfo() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getClientInfo( final String arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getHoldability() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException
	{
		return new J4SDatabaseMetaData(this, driver);
	}

	@Override
	public int getNetworkTimeout() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getSchema() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getTransactionIsolation() throws SQLException
	{
		return Connection.TRANSACTION_NONE;
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
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
		return false;
	}

	@Override
	public boolean isReadOnly() throws SQLException
	{
		return true;
	}

	@Override
	public boolean isValid( final int arg0 ) throws SQLException
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
	public String nativeSQL( final String arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CallableStatement prepareCall( final String arg0 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CallableStatement prepareCall( final String arg0, final int arg1,
			final int arg2 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CallableStatement prepareCall( final String arg0, final int arg1,
			final int arg2, final int arg3 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PreparedStatement prepareStatement( final String arg0 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PreparedStatement prepareStatement( final String arg0, final int arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PreparedStatement prepareStatement( final String arg0,
			final int arg1, final int arg2 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PreparedStatement prepareStatement( final String arg0,
			final int arg1, final int arg2, final int arg3 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PreparedStatement prepareStatement( final String arg0,
			final int[] arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PreparedStatement prepareStatement( final String arg0,
			final String[] arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void releaseSavepoint( final Savepoint arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void rollback() throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void rollback( final Savepoint arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void setAutoCommit( final boolean arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void setCatalog( final String catalog ) throws SQLException
	{
		this.catalog = catalog;
	}

	@Override
	public void setClientInfo( final Properties arg0 )
			throws SQLClientInfoException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void setClientInfo( final String arg0, final String arg1 )
			throws SQLClientInfoException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void setHoldability( final int arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void setNetworkTimeout( final Executor arg0, final int arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void setReadOnly( final boolean arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public Savepoint setSavepoint() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Savepoint setSavepoint( final String arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setSchema( final String arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void setTransactionIsolation( final int arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void setTypeMap( final Map<String, Class<?>> arg0 )
			throws SQLException
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
