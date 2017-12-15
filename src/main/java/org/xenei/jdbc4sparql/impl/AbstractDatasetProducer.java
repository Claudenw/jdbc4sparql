package org.xenei.jdbc4sparql.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.util.iterator.WrappedIterator;
import org.apache.xerces.util.XMLChar;
import org.xenei.jdbc4sparql.J4SPropertyNames;
import org.xenei.jdbc4sparql.iface.DatasetProducer;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.meta.MetaCatalogBuilder;
import org.xenei.jdbc4sparql.utils.NoCloseZipInputStream;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.apache.jena.arq.querybuilder.SelectBuilder;
import org.apache.jena.atlas.lib.Sync;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.impl.Util;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;

/**
 * Interface that defines the dataset producer.
 *
 * The dataset producer produces the local dataset (set of graphs that represent
 * the local data) and the meta dataset (set of graphs that contain the
 * metadata)
 *
 * Implementations of this class should construct the dataset when first
 * requested and return the same dataset on all subsequent calls.
 */
abstract public class AbstractDatasetProducer implements DatasetProducer {
    public static String getModelURI(final EntityManager mgr, final String modelName) {
        String name = StringUtils.defaultString( modelName );
        if (StringUtils.isEmpty( name )) {
            name = RdfCatalog.Builder.getFQName( mgr, name );
        } else {
            final int i = Util.splitNamespaceXML( name );
            if (i == 1) {
                if (XMLChar.isNCNameStart( name.charAt( 0 ) )) {// we have a short
                    // name
                    name = RdfCatalog.Builder.getFQName( mgr, name );
                }
            }
        }
        return name;
    }

    private final Properties properties;

    private final RDFFormat format = RDFFormat.TRIG;
    protected Configuration cfg;
    private final RDFConnection localConnection;
    private final RDFConnection metaConnection;
    private final EntityManager metaMgr;

    protected AbstractDatasetProducer(final Properties properties, final Dataset metaDataset,
            final Dataset localDataset) {
        this( new Configuration( properties, localDataset, metaDataset ) );
    }

    protected AbstractDatasetProducer(final Configuration cfg) {
        this.cfg = cfg;
        properties = cfg.config;
        localConnection = RDFConnectionFactory.connect( cfg.localDataset );
        metaConnection = RDFConnectionFactory.connect( cfg.metaDataset );
        final EntityManager entityManager = EntityManagerFactory.create( metaConnection );
        final String name = AbstractDatasetProducer.getModelURI( entityManager, MetaCatalogBuilder.LOCAL_NAME );
        metaMgr = entityManager.getNamedManager( NodeFactory.createURI( name ) );
    }

    @Override
    public RDFConnection getMetaConnection() {
        return metaConnection;
    }

    /**
     * Close the datasets in preparation for shutdown.
     */
    @Override
    public void close() {
        getMetaDataEntityManager().close();
        localConnection.close();
        metaConnection.close();
    }

    private static String createFN(final String prefix, final RDFFormat format) {
        return String.format( "%s.%s", prefix, format.getLang().getFileExtensions().get( 0 ) );
    }

    /**
     * Get or construct the local dataset.
     *
     * @return the local dataset
     */
    @Override
    public RDFConnection getLocalConnection() {
        return localConnection;
    }

    @Override
    public EntityManager getMetaDataEntityManager(final String modelName) {
        return getEntityManager( getMetaDataEntityManager(), modelName );
    }

    /**
     * Get or construct the meta dataset.
     *
     * @return the meta dataset.
     */
    @Override
    public EntityManager getMetaDataEntityManager() {
        return metaMgr;
    }

    private EntityManager getEntityManager(final EntityManager entityManager, final String modelName) {
        final String name = AbstractDatasetProducer.getModelURI( entityManager, modelName );
        return entityManager.getNamedManager( NodeFactory.createURI( name ) );
    }

    @Override
    public Properties getProperties() {
        return new Properties( properties );
    }

    @Override
    public Iterator<String> listMetaDataNames() {
        final SelectBuilder sb = new SelectBuilder().setDistinct( true ).addVar( "?g" ).addGraph( "?g",
                new SelectBuilder().addWhere( "?s", "?p", "?o" ).setLimit( 1 ) );
        try {
            metaConnection.begin( ReadWrite.READ );
            final ResultSet rs = metaConnection.query( sb.build() ).execSelect();
            return WrappedIterator.create( rs ).mapWith( qs -> qs.getResource( "g" ).getURI() );
        } finally {
            metaConnection.end();
        }
    }

    /**
     * Default load implementation
     *
     * @param zis
     * @throws IOException
     */
    protected static Configuration load(final Properties props, final ZipInputStream zis) throws IOException {
        final Configuration cfg = new Configuration();
        cfg.config = props;
        ZipEntry e = zis.getNextEntry();

        if (e.getName().startsWith( DatasetProducer.META_PREFIX )) {
            cfg.metaDataset = AbstractDatasetProducer.loadMeta( zis, e );
        } else {
            throw new IllegalStateException( "Entry must start with " + DatasetProducer.META_PREFIX );
        }
        e = zis.getNextEntry();
        if (e.getName().startsWith( DatasetProducer.LOCAL_PREFIX )) {
            cfg.localDataset = AbstractDatasetProducer.loadLocal( zis, e );
        } else {
            throw new IllegalStateException( "Entry must start with " + DatasetProducer.LOCAL_PREFIX );
        }
        return cfg;
    }

    private static Dataset loadDataset(final ZipInputStream zis, final ZipEntry e, final String prefix,
            final RDFFormat format) {
        final String fn = AbstractDatasetProducer.createFN( prefix, format );
        if (e.getName().equals( fn )) {
            final Dataset ds = DatasetFactory.create();
            RDFDataMgr.read( ds, new NoCloseZipInputStream( zis ), "", format.getLang() );
            return ds;
        } else {
            throw new IllegalArgumentException( "Entry name must be " + fn );
        }
    }

    protected static Dataset loadLocal(final ZipInputStream zis, final ZipEntry e) {
        return AbstractDatasetProducer.loadDataset( zis, e, DatasetProducer.LOCAL_PREFIX, RDFFormat.TRIG );
    }

    protected static Dataset loadMeta(final ZipInputStream zis, final ZipEntry e) {
        return AbstractDatasetProducer.loadDataset( zis, e, DatasetProducer.META_PREFIX, RDFFormat.TRIG );
    }

    @Override
    final public void save(final File f) throws FileNotFoundException, IOException {
        save( new FileOutputStream( f ) );
    }

    @Override
    final public void save(final OutputStream out) throws IOException {
        final ZipOutputStream zos = new ZipOutputStream( out );
        try {
            properties.setProperty( J4SPropertyNames.DATASET_PRODUCER, this.getClass().getCanonicalName() );

            final ZipEntry e = new ZipEntry( DatasetProducer.PROPERTIES_ENTRY_NAME );
            zos.putNextEntry( e );
            properties.store( zos, "" );
            zos.closeEntry();

            getMetaConnection().begin( ReadWrite.WRITE );
            try {
                final DatasetGraph dsg = cfg.metaDataset.asDatasetGraph();
                if (dsg instanceof Sync) {
                    ((Sync) dsg).sync();
                }
                saveMeta( zos );
            } finally {
                getMetaConnection().commit();
            }

            getLocalConnection().begin( ReadWrite.WRITE );
            try {
                final DatasetGraph dsg = cfg.localDataset.asDatasetGraph();
                if (dsg instanceof Sync) {
                    ((Sync) dsg).sync();
                }               
                saveLocal( zos );
            } finally {
                getLocalConnection().commit();
            }
        } finally {
            zos.close();
        }
    }

    private void saveDataset(final ZipOutputStream out, final RDFConnection connection, final String prefix)
            throws IOException {
        final ZipEntry e = new ZipEntry( AbstractDatasetProducer.createFN( prefix, format ) );
        out.putNextEntry( e );
        final Dataset ds = connection.fetchDataset();
        try {
            RDFDataMgr.write( out, ds, format );
        } finally {
            out.closeEntry();
        }
    }

    protected void saveLocal(final ZipOutputStream out) throws IOException {
        saveDataset( out, localConnection, DatasetProducer.LOCAL_PREFIX );
    }

    protected void saveMeta(final ZipOutputStream out) throws IOException {
        saveDataset( out, metaConnection, DatasetProducer.META_PREFIX );
    }

    public static class Configuration {
        public Dataset localDataset;
        public Dataset metaDataset;
        public Properties config;

        public Configuration() {

        }

        public Configuration(final Properties config, final Dataset localDataset, final Dataset metaDataset) {
            this.config = config;
            this.localDataset = localDataset;
            this.metaDataset = metaDataset;
        }
    }

}
