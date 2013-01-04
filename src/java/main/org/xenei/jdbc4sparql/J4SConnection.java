package org.xenei.jdbc4sparql;

import java.net.MalformedURLException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.meta.MetaCatalog;
import org.xenei.jdbc4sparql.sparql.SparqlCatalog;
import org.xenei.jdbc4sparql.sparql.SparqlSchema;
import org.xenei.jdbc4sparql.sparql.builders.SchemaBuilder;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlParserImpl;

public class J4SConnection implements Connection
{
	private Properties clientInfo;
	private J4SURL url;
	private Map<String,Catalog> catalogMap;
	private String currentCatalog;
	private String currentSchema;
	private final J4SDriver driver;
	private int networkTimeout;
	private boolean closed = false;
	private boolean autoCommit = true;
	private SparqlParser sparqlParser;
	private SQLWarning sqlWarnings;
	private int holdability;

	public J4SConnection( final J4SDriver driver, J4SURL url,
			final Properties properties ) throws MalformedURLException
	{
		this.catalogMap = new HashMap<String,Catalog>();
		
		this.sqlWarnings = null;
		this.driver = driver;
		this.url = url;
		this.currentCatalog = url.getCatalog();
		this.currentSchema = null;
		
		this.clientInfo = null;
		
		this.holdability = ResultSet.HOLD_CURSORS_OVER_COMMIT;
		
		configureCatalogMap( properties );
		
		Catalog c = new MetaCatalog();
		catalogMap.put( c.getLocalName(), c);
		if (currentCatalog != null && catalogMap.get(currentCatalog)==null)
		{
			throw new IllegalArgumentException( "Catalog "+currentCatalog+" not found in catalog map");
		}
		this.sparqlParser = SparqlParser.Util.getDefaultParser();
	}
	
	private void configureCatalogMap( Properties properties ) throws MalformedURLException
	{
		if (url.getBuilder() != null)
		{
			SparqlCatalog catalog = new SparqlCatalog( url.getEndpoint().toURL(), currentCatalog );
			SchemaBuilder builder = SchemaBuilder.Util.getBuilder( url.getBuilder() );
			SparqlSchema schema = new SparqlSchema(catalog, SparqlSchema.DEFAULT_NAMESPACE, "");
			catalog.addSchema(schema);
			schema.addTableDefs(builder.getTableDefs(catalog));
			currentSchema = schema.getLocalName();
			catalogMap.put( catalog.getLocalName(), catalog);
		}
		else
		{
			// TODO future read from configuration
		}
	}
	
	public String getConfiguration()
	{
		// TODO create config serialization
		return "";
	}
	
	SparqlParser getSparqlParser()
	{
		return sparqlParser;
	}

	@Override
	public void abort( final Executor arg0 ) throws SQLException
	{
		close();
	}

	@Override
	public void clearWarnings() throws SQLException
	{
		sqlWarnings = null;
	}

	@Override
	public void close() throws SQLException
	{
		closed = true;
	}

	private void checkClosed() throws SQLException
	{
		if (closed)
		{
			throw new SQLException( "Connection closed"); 
		}
	}
	
	@Override
	public void commit() throws SQLException
	{
		checkClosed();
		if (autoCommit)
		{
			throw new SQLException( "commit called on autoCommit connection");
		}
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
		return createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
	}

	@Override
	public Statement createStatement( final int resultSetType, final int resultSetConcurrency )
			throws SQLException
	{
		return  createStatement( resultSetType, resultSetConcurrency, this.getHoldability());
	}

	@Override
	public Statement createStatement( final int resultSetType, final int resultSetConcurrency,
			final int resultSetHoldability ) throws SQLException
	{
		Catalog catalog = catalogMap.get(currentCatalog);
		if (catalog instanceof SparqlCatalog)
		{
			return new J4SStatement(this, (SparqlCatalog)catalog, resultSetType, resultSetConcurrency, resultSetHoldability);
		}
		else
		{
			throw new SQLException( "Catalog "+currentCatalog+" does not support statements");
		}
	}

	@Override
	public Struct createStruct( final String arg0, final Object[] arg1 )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean getAutoCommit() throws SQLException
	{
		return autoCommit;
	}

	@Override
	public String getCatalog() throws SQLException
	{
		return currentCatalog;
	}

	@Override
	public Properties getClientInfo() throws SQLException
	{
		return clientInfo;
	}

	@Override
	public String getClientInfo( final String key ) throws SQLException
	{
		return clientInfo.getProperty(key);
	}

	@Override
	public int getHoldability() throws SQLException
	{
		return holdability;
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException
	{
		return new J4SDatabaseMetaData(this, driver);
	}

	@Override
	public int getNetworkTimeout() throws SQLException
	{
		return networkTimeout;
	}

	@Override
	public String getSchema() throws SQLException
	{
		return currentSchema;
	}

	@Override
	public int getTransactionIsolation() throws SQLException
	{
		return Connection.TRANSACTION_NONE;
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException
	{
		return sqlWarnings;
	}

	@Override
	public boolean isClosed() throws SQLException
	{
		return closed;
	}

	@Override
	public boolean isReadOnly() throws SQLException
	{
		return true;
	}

	@Override
	public boolean isValid( final int timeout ) throws SQLException
	{
		if (timeout < 0)
		{
			throw new SQLException( "Timeout must not be less than zero");
		}
		// TODO figure out how to do thos
		return true;
	}

	@Override
	public boolean isWrapperFor( final Class<?> arg0 ) throws SQLException
	{
		return false;
	}

	@Override
	public String nativeSQL( final String sql ) throws SQLException
	{
		return sparqlParser.nativeSQL(sql);
	}

	@Override
	public CallableStatement prepareCall( final String arg0 )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public CallableStatement prepareCall( final String arg0, final int arg1,
			final int arg2 ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public CallableStatement prepareCall( final String arg0, final int arg1,
			final int arg2, final int arg3 ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement( final String arg0 )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement( final String arg0, final int arg1 )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement( final String arg0,
			final int arg1, final int arg2 ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement( final String arg0,
			final int arg1, final int arg2, final int arg3 )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement( final String arg0,
			final int[] arg1 ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement( final String arg0,
			final String[] arg1 ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void releaseSavepoint( final Savepoint arg0 ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void rollback() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void rollback( final Savepoint arg0 ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setAutoCommit( final boolean state ) throws SQLException
	{
		autoCommit = state;
	}

	@Override
	public void setCatalog( final String catalog ) throws SQLException
	{
		if (catalogMap.get(catalog) == null)
		{
			throw new SQLException( "Catalog "+catalog+" was not found");
		}
		this.currentCatalog = catalog;
	}

	@Override
	public void setClientInfo( final Properties clientInfo )
			throws SQLClientInfoException
	{
		this.clientInfo = clientInfo;
	}

	@Override
	public void setClientInfo( final String param, final String value )
			throws SQLClientInfoException
	{
		this.clientInfo.setProperty( param, value);
	}

	@Override
	public void setHoldability( final int holdability ) throws SQLException
	{
		if (holdability !=  ResultSet.HOLD_CURSORS_OVER_COMMIT && 
				holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT)
		{
			throw new SQLException( "Invalid holdability value");
		}
		this.holdability = holdability;
	}

	@Override
	public void setNetworkTimeout( final Executor arg0, final int timeout )
			throws SQLException
	{
		this.networkTimeout = timeout;
	}

	@Override
	public void setReadOnly( final boolean state ) throws SQLException
	{
		// do nothing as it is all read only.
	}

	@Override
	public Savepoint setSavepoint() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Savepoint setSavepoint( final String arg0 ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setSchema( final String schema ) throws SQLException
	{
		this.currentSchema = schema;
	}

	@Override
	public void setTransactionIsolation( final int arg0 ) throws SQLException
	{
		// do nothing as we don't do transactions
	}

	@Override
	public void setTypeMap( final Map<String, Class<?>> arg0 )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public <T> T unwrap( final Class<T> arg0 ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

}
