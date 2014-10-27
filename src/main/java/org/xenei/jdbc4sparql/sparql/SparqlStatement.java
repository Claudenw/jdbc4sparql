/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xenei.jdbc4sparql.sparql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Map;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfSchema;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;

public class SparqlStatement implements Statement
{
	private final SparqlParser parser;
	private final RdfCatalog catalog;
	private final RdfSchema schema;
	private final Map<String, Catalog> catalogs;

	SparqlStatement( final Map<String, Catalog> catalogs,
			final RdfCatalog catalog, final RdfSchema schema,
			final SparqlParser parser )
	{
		this.parser = parser;
		this.catalog = catalog;
		this.schema = schema;
		this.catalogs = catalogs;
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
		final SparqlQueryBuilder builder = parser.parse(catalogs, catalog,
				schema, query);
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
