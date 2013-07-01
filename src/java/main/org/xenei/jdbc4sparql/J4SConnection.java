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
package org.xenei.jdbc4sparql;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.util.iterator.WrappedIterator;
import com.hp.hpl.jena.vocabulary.RDF;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.lang3.StringUtils;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.ModelFactory;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfSchema;
import org.xenei.jdbc4sparql.impl.rdf.RdfTable;
import org.xenei.jdbc4sparql.impl.rdf.ResourceBuilder;
import org.xenei.jdbc4sparql.meta.MetaCatalogBuilder;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.MissingAnnotation;

public class J4SConnection implements Connection
{
	private class NoCloseZipInputStream extends InputStream
	{
		ZipInputStream wrapped;

		public NoCloseZipInputStream( final ZipInputStream is )
		{
			wrapped = is;
		}

		@Override
		public int available() throws IOException
		{
			return wrapped.available();
		}

		@Override
		public void close() throws IOException
		{
			wrapped.closeEntry();
		}

		@Override
		public boolean equals( final Object obj )
		{
			return wrapped.equals(obj);
		}

		@Override
		public int hashCode()
		{
			return wrapped.hashCode();
		}

		@Override
		public void mark( final int readlimit )
		{
			wrapped.mark(readlimit);
		}

		@Override
		public boolean markSupported()
		{
			return wrapped.markSupported();
		}

		@Override
		public int read() throws IOException
		{
			return wrapped.read();
		}

		@Override
		public int read( final byte[] b ) throws IOException
		{
			return wrapped.read(b);
		}

		@Override
		public int read( final byte[] b, final int off, final int len )
				throws IOException
		{
			return wrapped.read(b, off, len);
		}

		@Override
		public void reset() throws IOException
		{
			wrapped.reset();
		}

		@Override
		public long skip( final long n ) throws IOException
		{
			return wrapped.skip(n);
		}

		@Override
		public String toString()
		{
			return wrapped.toString();
		}

	}

	public static final String META_MODEL_FACTORY = "MetaModelFactory";
	private Properties clientInfo;
	private final J4SUrl url;
	private final Map<String, Catalog> catalogMap;
	private String currentCatalog;
	private String currentSchema;
	private final J4SDriver driver;
	private int networkTimeout;
	private boolean closed = false;
	private boolean autoCommit = true;
	private final SparqlParser sparqlParser;
	private SQLWarning sqlWarnings;
	private int holdability;
	private File tmpDir;

	private final Properties properties;

	/**
	 * The dataset of catalogs
	 */
	private Dataset dataset;

	/**
	 * The dataset of local data
	 */
	private Dataset localData;

	public J4SConnection( final J4SDriver driver, final J4SUrl url,
			final Properties properties ) throws IOException,
			InstantiationException, IllegalAccessException,
			ClassNotFoundException, MissingAnnotation
	{
		if (properties == null)
		{
			throw new IllegalArgumentException("Properties may not be null");
		}
		this.properties = properties;
		this.catalogMap = new HashMap<String, Catalog>();
		this.sqlWarnings = null;
		this.driver = driver;
		this.url = url;
		this.currentCatalog = url.getCatalog();
		this.currentSchema = null;
		this.tmpDir = null;
		this.clientInfo = new Properties();

		this.holdability = ResultSet.HOLD_CURSORS_OVER_COMMIT;

		configureCatalogMap();
		final Model metadataModel = getDataset().getNamedModel(
				MetaCatalogBuilder.LOCAL_NAME);
		final Catalog c = MetaCatalogBuilder.getInstance(metadataModel);
		catalogMap.put(c.getName(), c);
		if (StringUtils.isNotEmpty(currentCatalog)
				&& (catalogMap.get(currentCatalog) == null))
		{
			throw new IllegalArgumentException("Catalog '" + currentCatalog
					+ "' not found in catalog map");
		}

		this.sparqlParser = url.getParser() != null ? url.getParser()
				: SparqlParser.Util.getDefaultParser();
	}

	@Override
	public void abort( final Executor arg0 ) throws SQLException
	{
		close();
	}

	public RdfCatalog addCatalog( final RdfCatalog.Builder catalogBuilder )
	{
		final Model model = getDataset()
				.getNamedModel(catalogBuilder.getName());
		Model dataModel = catalogBuilder.getLocalModel();
		if (dataModel != null)
		{
			getLocalData().addNamedModel(catalogBuilder.getName(), dataModel);
		}
		else
		{
			dataModel = getLocalData().getNamedModel(catalogBuilder.getName());
			if (dataModel != null)
			{
				catalogBuilder.setLocalModel(dataModel);
			}
		}
		final RdfCatalog cat = catalogBuilder.build(model);
		catalogMap.put(cat.getName(), cat);
		return cat;
	}

	private void checkClosed() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("Connection closed");
		}
	}

	@Override
	public void clearWarnings() throws SQLException
	{
		sqlWarnings = null;
	}

	@Override
	public void close() throws SQLException
	{
		for (final Catalog cat : catalogMap.values())
		{
			cat.close();
		}
		if (dataset != null)
		{
			dataset.close();
		}
		if (localData != null)
		{
			localData.close();
		}
		closed = true;
	}

	@Override
	public void commit() throws SQLException
	{
		checkClosed();
		if (autoCommit)
		{
			throw new SQLException("commit called on autoCommit connection");
		}
	}

	private void configureCatalogMap() throws IOException,
			InstantiationException, IllegalAccessException,
			ClassNotFoundException, MissingAnnotation
	{

		if (url.getType().equals(J4SUrl.TYPE_CONFIG))
		{
			loadConfig(url.getEndpoint().toURL().openStream());
		}
		else
		{
			RdfCatalog catalog = null;
			Model model = null;
			if (StringUtils.isEmpty(currentCatalog))
			{
				model = getDataset().getDefaultModel();
			}
			else
			{
				model = getDataset().getNamedModel(currentCatalog);
			}

			if (url.getType().equals(J4SUrl.TYPE_SPARQL))
			{
				catalog = new RdfCatalog.Builder()
						.setSparqlEndpoint(url.getEndpoint().toURL())
						.setName(currentCatalog).build(model);
			}
			else
			{
				final Model dataModel = createModel();
				dataModel.read(url.getEndpoint().toString(), url.getType());
				catalog = new RdfCatalog.Builder().setLocalModel(dataModel)
						.setName(currentCatalog).build(model);
			}

			final RdfSchema schema = new RdfSchema.Builder()
					.setCatalog(catalog).setName("").build(model);
			// schema.addTableDefs(builder.getTableDefs(catalog));
			currentSchema = schema.getName();
			catalogMap.put(catalog.getName(), catalog);
			if (url.getBuilder() != null)
			{
				for (final RdfTable table : url.getBuilder().getTables(catalog))
				{
					schema.addTables(table);
				}
			}
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

	/**
	 * Create a model using the configured model builder.
	 * 
	 * @return a fresh model
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	private Model createModel() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException
	{
		if (properties.containsKey(J4SConnection.META_MODEL_FACTORY))
		{
			final Class<? extends ModelFactory> clazz = (Class<? extends ModelFactory>) J4SConnection.class
					.getClassLoader()
					.loadClass(
							properties
									.getProperty(J4SConnection.META_MODEL_FACTORY));
			return clazz.newInstance().createModel(properties);
		}
		return com.hp.hpl.jena.rdf.model.ModelFactory.createDefaultModel();

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
		return createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
				ResultSet.CONCUR_READ_ONLY);
	}

	@Override
	public Statement createStatement( final int resultSetType,
			final int resultSetConcurrency ) throws SQLException
	{
		return createStatement(resultSetType, resultSetConcurrency,
				this.getHoldability());
	}

	@Override
	public Statement createStatement( final int resultSetType,
			final int resultSetConcurrency, final int resultSetHoldability )
			throws SQLException
	{
		final Catalog catalog = catalogMap.get(currentCatalog);
		if (catalog instanceof RdfCatalog)
		{
			return new J4SStatement(this, (RdfCatalog) catalog, resultSetType,
					resultSetConcurrency, resultSetHoldability);
		}
		else
		{
			throw new SQLException("Catalog '" + currentCatalog
					+ "' does not support statements");
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

	public Map<String, Catalog> getCatalogs()
	{
		return catalogMap;
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

	private Dataset getDataset()
	{
		if (dataset == null)
		{
			// must be a local file
			tmpDir = new File(System.getProperty("java.io.tmpdir"));
			tmpDir = new File(tmpDir, UUID.randomUUID().toString());
			dataset = TDBFactory.createDataset(tmpDir.getAbsolutePath());
		}
		return dataset;
	}

	@Override
	public int getHoldability() throws SQLException
	{
		return holdability;
	}

	private Dataset getLocalData()
	{
		if (localData == null)
		{
			// must be a local file
			tmpDir = new File(System.getProperty("java.io.tmpdir"));
			tmpDir = new File(tmpDir, UUID.randomUUID().toString());
			localData = TDBFactory.createDataset(tmpDir.getAbsolutePath());
		}
		return localData;
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

	public SparqlParser getSparqlParser()
	{
		return sparqlParser;
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
			throw new SQLException("Timeout must not be less than zero");
		}
		// TODO figure out how to do this
		return true;
	}

	@Override
	public boolean isWrapperFor( final Class<?> arg0 ) throws SQLException
	{
		return false;
	}

	private void loadConfig( final InputStream in ) throws IOException,
			MissingAnnotation
	{
		Dataset ds;
		final ZipInputStream zis = new ZipInputStream(in);
		for (ZipEntry e = zis.getNextEntry(); e != null; e = zis.getNextEntry())
		{
			Model m = null;
			final String name = e.getName();
			String graphName = name;
			if (name.startsWith("/default/"))
			{
				graphName = name.substring("/default/".length());
				if (graphName.startsWith("local"))
				{
					ds = getLocalData();
				}
				else if (graphName.startsWith("meta"))
				{
					ds = getDataset();
				}
				else
				{
					throw new IllegalArgumentException("Unknown type " + name);

				}
				m = ds.getDefaultModel();
			}
			else
			{
				if (name.startsWith("/local/"))
				{
					ds = getLocalData();
					graphName = name.substring("/local/".length());
				}
				else if (name.startsWith("/meta/"))
				{
					ds = getDataset();
					graphName = name.substring("/meta/".length());
				}
				else
				{
					throw new IllegalArgumentException("Unknown type " + name);
				}
				graphName = graphName.substring(0,
						graphName.length() - ".ttl".length());
				m = ds.getNamedModel(graphName);
			}

			final NoCloseZipInputStream ncis = new NoCloseZipInputStream(zis);
			m.read(ncis, graphName, "TURTLE");
		}
		zis.close();
		// create the catalogs
		ds = getDataset();
		final Resource catType = ResourceFactory.createResource(ResourceBuilder
				.getFQName(RdfCatalog.class));
		final EntityManager entityManager = EntityManagerFactory
				.getEntityManager();
		final List<String> names = WrappedIterator.create(ds.listNames())
				.toList();
		names.add("");
		for (final String name : names)
		{
			final Model m = name.equals("") ? ds.getDefaultModel() : ds
					.getNamedModel(name);
			final List<RdfCatalog> cats = new ArrayList<RdfCatalog>();
			final ResIterator ri = m
					.listSubjectsWithProperty(RDF.type, catType);
			while (ri.hasNext())
			{
				cats.add(entityManager.read(ri.next(), RdfCatalog.class));
			}
			// add local data if necessary
			for (RdfCatalog cat : cats)
			{
				if (getLocalData().containsNamedModel(name))
				{
					cat = new RdfCatalog.Builder(cat).setLocalModel(
							getLocalData().getNamedModel(name)).build(m);
				}
				catalogMap.put(cat.getName(), cat);
			}

		}
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

	/**
	 * Save all the current RdfCatalogs to a configuration file.
	 * 
	 * Reloading this file may be used in the URL as the configuration location.
	 * 
	 * @param The
	 *            modelWriter to write the config to.
	 * @throws IOException
	 */
	public void saveConfig( final OutputStream os ) throws IOException
	{
		final ZipOutputStream out = new ZipOutputStream(os);
		Dataset ds = getDataset();
		String entryName;
		ZipEntry e;
		for (final Iterator<String> nIter = ds.listNames(); nIter.hasNext();)
		{
			final String name = nIter.next();
			entryName = String.format("/meta/%s.ttl", name);
			e = new ZipEntry(entryName);
			out.putNextEntry(e);
			ds.getNamedModel(name).write(out, "TURTLE");
			out.closeEntry();
		}
		entryName = "/default/meta.ttl";
		e = new ZipEntry(entryName);
		out.putNextEntry(e);
		ds.getDefaultModel().write(out, "TURTLE");
		out.closeEntry();

		ds = getLocalData();
		for (final Iterator<String> nIter = ds.listNames(); nIter.hasNext();)
		{
			final String name = nIter.next();
			entryName = String.format("/local/%s.ttl", name);
			e = new ZipEntry(entryName);
			out.putNextEntry(e);
			ds.getNamedModel(name).write(out, "TURTLE");
			out.closeEntry();
		}
		entryName = "/default/local.ttl";
		e = new ZipEntry(entryName);
		out.putNextEntry(e);
		ds.getDefaultModel().write(out, "TURTLE");
		out.closeEntry();
		out.close();
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
			throw new SQLException("Catalog " + catalog + " was not found");
		}
		this.currentCatalog = catalog;
	}

	@Override
	public void setClientInfo( final Properties clientInfo )
	{
		this.clientInfo = clientInfo;
	}

	@Override
	public void setClientInfo( final String param, final String value )
	{
		if( value != null)
		{
			this.clientInfo.setProperty(param, value);
		} else {
			this.clientInfo.remove(param);
		}
	}

	@Override
	public void setHoldability( final int holdability ) throws SQLException
	{
		// don't support ResultSet.CLOSE_CURSORS_AT_COMMIT
		if (holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT)
		{
			throw new SQLFeatureNotSupportedException("Invalid holdability value");
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
		if (!state)
		{
			throw new SQLFeatureNotSupportedException( "Can not set ReadOnly=false");
		}
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
		throw new SQLFeatureNotSupportedException();
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
