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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.arq.querybuilder.SelectBuilder;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFWriter;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.util.iterator.WrappedIterator;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.config.MemDatasetProducer;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.DatasetProducer;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.impl.AbstractDatasetProducer;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfSchema;
import org.xenei.jdbc4sparql.impl.rdf.RdfTable;
import org.xenei.jdbc4sparql.impl.rdf.ResourceBuilder;
import org.xenei.jdbc4sparql.impl.virtual.VirtualCatalog;
import org.xenei.jdbc4sparql.meta.MetaCatalogBuilder;
import org.xenei.jdbc4sparql.sparql.builders.SchemaBuilder;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.MissingAnnotation;

/**
 * The JDBC 4 Sparql SQLConnection implementation.
 *
 */
public class J4SConnection implements Connection {
    private Properties clientInfo;
    private final J4SUrl url;
    private final Map<String, Catalog> catalogMap;
    private final J4SDriver driver;
    private int networkTimeout;
    private boolean closed = false;
    private boolean autoCommit = true;
    private final SparqlParser sparqlParser;
    private SQLWarning sqlWarnings;
    private int holdability;
    private static final Logger LOG = LoggerFactory.getLogger( J4SConnection.class );
    private DatasetProducer dsProducer = null;
    private final Properties properties;

    /**
     * Constructor.
     * @param driver The J$SDriver this connection uses
     * @param url the J4SURL that this connection is attaching to.
     * @param properties The properties for this connectno.
     * @throws IOException on IO error
     * @throws InstantiationException If a class can not be instantiated.
     * @throws IllegalAccessException If a class method can not be accessed.
     * @throws ClassNotFoundException If a class can not be found.
     * @throws MissingAnnotation If there is a missing annotation on an internal class.
     * @throws SQLException On SQL error.
     */
    public J4SConnection(final J4SDriver driver, final J4SUrl url, final Properties properties) throws IOException,
            InstantiationException, IllegalAccessException, ClassNotFoundException, MissingAnnotation, SQLException {
        if (properties == null) {
            throw new IllegalArgumentException( "Properties may not be null" );
        }
        this.properties = properties;

        this.catalogMap = new HashMap<String, Catalog>();
        this.sqlWarnings = null;
        this.driver = driver;
        this.url = url;
        this.clientInfo = new Properties();

        this.holdability = ResultSet.HOLD_CURSORS_OVER_COMMIT;

        // create the SPARQLParser
        this.sparqlParser = url.getParser() != null ? url.getParser() : SparqlParser.Util.getDefaultParser();

        // default catalog name is set here and catalog schema is set here
        mergeProperties( url.getProperties() );

        // make sure the dataset producer class is set.
        if (!properties.containsKey( J4SPropertyNames.DATASET_PRODUCER )) {
            properties.setProperty( J4SPropertyNames.DATASET_PRODUCER, MemDatasetProducer.class.getCanonicalName() );
        }

        configureCatalogMap();

       if (StringUtils.isNotEmpty( getCatalog() ) && (catalogMap.get( getCatalog() ) == null)) {
            throw new IllegalArgumentException(
                    String.format( "Catalog '%s' not found in catalog map", getCatalog() ) );
        }

        catalogMap.put( VirtualCatalog.NAME, new VirtualCatalog() );

    }

    @Override
    public void abort(final Executor arg0) throws SQLException {
        close();
    }

    public RdfCatalog addCatalog(final RdfCatalog.Builder catalogBuilder) {
        String catName = catalogBuilder.getName().getShortName();
        final EntityManager metaEntityManager = dsProducer.getMetaDataEntityManager( catName );
        final RdfCatalog cat = catalogBuilder.build( metaEntityManager );
        catalogMap.put( cat.getName().getShortName(), cat );
        return cat;
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException( "Connection closed" );
        }
    }

    @Override
    public void clearWarnings() throws SQLException {
        sqlWarnings = null;
    }

    @Override
    public void close() throws SQLException {
        for (final Catalog cat : catalogMap.values()) {
            cat.close();
        }
        dsProducer.close();
        closed = true;
    }

    @Override
    public void commit() throws SQLException {
        checkClosed();
        if (autoCommit) {
            throw new SQLException( "commit called on autoCommit connection" );
        }
    }

    /**
     * Examine the J4SUrl and read the catalog(s).
     * 
     * @throws IOException on IO error
     * @throws InstantiationException If a class can not be instantiated.
     * @throws IllegalAccessException If a class method can not be accessed.
     * @throws ClassNotFoundException If a class can not be found.
     * @throws MissingAnnotation If there is a missing annotation on an internal class.
     * @throws SQLException On SQL error.
     */
    private void configureCatalogMap() throws IOException, InstantiationException, IllegalAccessException,
            ClassNotFoundException, MissingAnnotation, SQLException {
        // if this is a config file just read the file.
        if (url.getType().equals( J4SUrl.TYPE_CONFIG )) {
            loadConfig( url.getEndpoint().toURL() );           
        } else {
            // otherwise we have to read the data and parse the input.
            dsProducer = DatasetProducer.Loader.load( properties );
            
            if (catalogMap.get( MetaCatalogBuilder.LOCAL_NAME ) == null) {
                final Catalog c = MetaCatalogBuilder.getInstance( dsProducer );
                catalogMap.put( c.getName().getShortName(), c );
            }
                   
            /* if the catalog name is not set the set to "" (empty string). 
             Can not use setCatalog here as the catalog map is not yet set. */
            properties.setProperty( J4SPropertyNames.CATALOG_PROPERTY, StringUtils.defaultString( getCatalog(), "" ) );

            /* the schema name is the produced by the builder name. Create the Schema builder */
            SchemaBuilder builder = url.getBuilder();
            if (builder == null) {
                builder = SchemaBuilder.Util.getBuilder( null );
            }

            String schemaName = SchemaBuilder.Util.getName( builder.getClass() );
            // make schema builder valid for schema name user
            schemaName = schemaName.replace( "^[A-Z0-9a-z]", "_" );

            /* get the EntityManager for the catalog name. */
            final EntityManager metaDataMgr = dsProducer.getMetaDataEntityManager( getCatalog() );

            /*
             * Build the catalog.
             */
            RdfCatalog catalog = null;
            // if a SPARQL endpoint the driver URL has the endpoint URL.
            if (url.getType().equals( J4SUrl.TYPE_SPARQL )) {
                catalog = new RdfCatalog.Builder().setSparqlEndpoint( url.getEndpoint().toURL() )
                        .setName( getCatalog() ).build( metaDataMgr );
            } else {                
                Dataset ds = DatasetFactory.create();
                RDFDataMgr.read( ds, url.getEndpoint().toURL().openStream(), url.getLang() );
                dsProducer.getLocalConnection().putDataset( ds );                
                catalog = new RdfCatalog.Builder().setLocalConnection( dsProducer.getLocalConnection() ).setName( getCatalog() )
                        .build( metaDataMgr );
            }

            // builder adds schema to catalog           
            final RdfSchema schema = new RdfSchema.Builder().setCatalog( catalog ).setName( schemaName ).build();

            catalogMap.put( catalog.getName().getShortName(), catalog );

            // builder adds tables to schma
            if (builder != null) {
                for (final RdfTable table : builder.getTables( schema )) {
                    schema.addTables( table );
                }
            }

            metaDataMgr.sync();

        }

    }

    @Override
    public Array createArrayOf(final String arg0, final Object[] arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Statement createStatement() throws SQLException {
        return createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY );
    }

    @Override
    public Statement createStatement(final int resultSetType, final int resultSetConcurrency) throws SQLException {
        return createStatement( resultSetType, resultSetConcurrency, this.getHoldability() );
    }

    @Override
    public Statement createStatement(final int resultSetType, final int resultSetConcurrency,
            final int resultSetHoldability) throws SQLException {
        final Catalog catalog = catalogMap.get( getCatalog() );

        if (catalog instanceof RdfCatalog) {
            return new J4SStatement( this, (RdfCatalog) catalog, resultSetType, resultSetConcurrency,
                    resultSetHoldability );
        } else {
            throw new SQLException( "Catalog '" + getCatalog() + "' does not support statements" );
        }
    }

    @Override
    public Struct createStruct(final String arg0, final Object[] arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return autoCommit;
    }

    @Override
    public String getCatalog() {
        return properties.getProperty( J4SPropertyNames.CATALOG_PROPERTY );
    }

    public Map<String, Catalog> getCatalogs() {
        return catalogMap;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return clientInfo;
    }

    @Override
    public String getClientInfo(final String key) throws SQLException {
        return clientInfo.getProperty( key );
    }

    public DatasetProducer getDatasetProducer() {
        return dsProducer;
    }

    @Override
    public int getHoldability() throws SQLException {
        return holdability;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return new J4SDatabaseMetaData( this, driver );
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return networkTimeout;
    }

    @Override
    public String getSchema() {
        return properties.getProperty( J4SPropertyNames.SCHEMA_PROPERTY );
    }

    public SparqlParser getSparqlParser() {
        return sparqlParser;
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return sqlWarnings;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return true;
    }

    @Override
    public boolean isValid(final int timeout) throws SQLException {
        if (timeout < 0) {
            throw new SQLException( "Timeout must not be less than zero" );
        }
        // TODO figure out how to do this
        return true;
    }

    @Override
    public boolean isWrapperFor(final Class<?> arg0) throws SQLException {
        return false;
    }

    private void loadConfig(final URL url) throws IOException, MissingAnnotation {
        // config specifies producer
        dsProducer = DatasetProducer.Loader.load( properties, url );
        mergeProperties( dsProducer.getProperties() );

        // create the catalogs
        final Resource catType = ResourceFactory
                .createResource( ResourceBuilder.getFQName( dsProducer.getMetaDataEntityManager(), RdfCatalog.class ) );
        Query q = new SelectBuilder().setDistinct( true ).addVar( "?s" ).addWhere( "?s", RDF.type, catType ).build();
        final List<String> names = WrappedIterator.create( dsProducer.listMetaDataNames() ).toList();

        for (final String name : names) {
            final EntityManager entityManager = dsProducer.getMetaDataEntityManager( name );

            final Iterator<Resource> ri = WrappedIterator.create( entityManager.execute( q ).execSelect() )
                    .mapWith( qs -> qs.getResource( "s" ) );
            while (ri.hasNext()) {
                RdfCatalog cat = entityManager.read( ri.next(), RdfCatalog.class );
                final RdfCatalog.Builder builder = new RdfCatalog.Builder( cat );
                EntityManager em = null;
                if (AbstractDatasetProducer
                        .getModelURI( dsProducer.getMetaDataEntityManager(), MetaCatalogBuilder.LOCAL_NAME )
                        .equals( name )) {
                    em = dsProducer.getMetaDataEntityManager();
                } else {
                    em = dsProducer.getMetaDataEntityManager( name );

                }
                builder.setLocalConnection( dsProducer.getLocalConnection() );
                builder.setWriteGraph( em.getModelName().getURI() );
                cat = builder.build( entityManager );

                catalogMap.put( cat.getName().getShortName(), cat );
            }
        }
        if (catalogMap.get( MetaCatalogBuilder.LOCAL_NAME ) == null) {
            final Catalog c = MetaCatalogBuilder.getInstance( dsProducer );
            catalogMap.put( c.getName().getShortName(), c );
        }
        
    }

    /**
     * Adds any new properties form the argument into our internal properties.
     *
     * @param properties
     */
    private void mergeProperties(final Properties properties) {
        for (final String s : properties.stringPropertyNames()) {
            if (!this.properties.containsKey( s )) {
                this.properties.put( s, properties.getProperty( s ) );
            }
        }
    }

    @Override
    public String nativeSQL(final String sql) throws SQLException {
        return sparqlParser.nativeSQL( sql );
    }

    @Override
    public CallableStatement prepareCall(final String arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public CallableStatement prepareCall(final String arg0, final int arg1, final int arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public CallableStatement prepareCall(final String arg0, final int arg1, final int arg2, final int arg3)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(final String arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(final String arg0, final int arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(final String arg0, final int arg1, final int arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(final String arg0, final int arg1, final int arg2, final int arg3)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(final String arg0, final int[] arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(final String arg0, final String[] arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void releaseSavepoint(final Savepoint arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void rollback() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void rollback(final Savepoint arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * Save all the current RdfCatalogs to a configuration file.
     *
     * Reloading this file may be used in the URL as the configuration location.
     *
     * @param f
     *            The file to write the configuration to
     * @throws IOException
     */
    public void saveConfig(final File f) throws IOException {
        dsProducer.save( f );
    }

    /**
     * Save all the current RdfCatalogs to a configuration file.
     *
     * Reloading this file may be used in the URL as the configuration location.
     *
     * @param the
     *            outputstream to write the configuration to.
     * @throws IOException
     */
    public void saveConfig(final OutputStream os) throws IOException {
        dsProducer.save( os );
    }

    @Override
    public void setAutoCommit(final boolean state) throws SQLException {
        autoCommit = state;
    }

    @Override
    public void setCatalog(final String catalog) throws SQLException {
        if ((getCatalog() == null) || !getCatalog().equals( catalog )) {
            if (LOG.isDebugEnabled()) {
                LOG.debug( "Setting catalog to '{}'", catalog );
            }
            if (catalogMap.get( catalog ) == null) {
                throw new SQLException( "Catalog " + catalog + " was not found" );
            }
            properties.setProperty( J4SPropertyNames.CATALOG_PROPERTY, catalog );
            setSchema( null );
        }
    }

    @Override
    public void setClientInfo(final Properties clientInfo) {
        this.clientInfo = clientInfo;
    }

    @Override
    public void setClientInfo(final String param, final String value) {
        if (value != null) {
            this.clientInfo.setProperty( param, value );
        } else {
            this.clientInfo.remove( param );
        }
    }

    @Override
    public void setHoldability(final int holdability) throws SQLException {
        // don't support ResultSet.CLOSE_CURSORS_AT_COMMIT
        if (holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            throw new SQLFeatureNotSupportedException( "Invalid holdability value" );
        }
        this.holdability = holdability;
    }

    @Override
    public void setNetworkTimeout(final Executor arg0, final int timeout) throws SQLException {
        this.networkTimeout = timeout;
    }

    @Override
    public void setReadOnly(final boolean state) throws SQLException {
        if (!state) {
            throw new SQLFeatureNotSupportedException( "Can not set ReadOnly=false" );
        }
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Savepoint setSavepoint(final String arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSchema(final String schema) throws SQLException {
        if (schema != null) {
            final Catalog cat = catalogMap.get( getCatalog() );
            if (cat == null) {
                throw new SQLException( String.format( "Catalog '%s' was not found", getCatalog() ) );
            }

            final Schema schem = cat.getSchema( schema );
            if (schem == null) {
                throw new SQLException(
                        String.format( "Schema '%s' was not found in catalog '%s'", schema, getCatalog() ) );
            }
            this.properties.setProperty( J4SPropertyNames.SCHEMA_PROPERTY, schema );
        } else {
            this.properties.remove( J4SPropertyNames.SCHEMA_PROPERTY );
        }
    }

    @Override
    public void setTransactionIsolation(final int arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTypeMap(final Map<String, Class<?>> arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String toString() {
        return new StringBuilder().append( "J4SConnection[" ).append( url.toString() ).append( "]" ).toString();
    }

    @Override
    public <T> T unwrap(final Class<T> arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}
