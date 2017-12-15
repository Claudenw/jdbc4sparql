package org.xenei.jdbc4sparql.config;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileExistsException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.AndFileFilter;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.FileFileFilter;
import org.apache.commons.io.filefilter.NameFileFilter;
import org.apache.commons.io.filefilter.NotFileFilter;
import org.apache.commons.io.filefilter.OrFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.iface.DatasetProducer;
import org.xenei.jdbc4sparql.impl.AbstractDatasetProducer;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.tdb.TDBFactory;

public class TDBDatasetProducer extends AbstractDatasetProducer {

    private final Config cfg;

    /**
     * The TDBConfiguration.
     */
    private static class Config extends AbstractDatasetProducer.Configuration {
        private final Logger log = LoggerFactory.getLogger( Config.class );

        /**
         * The directory that contains the meta data
         */
        public final File metaDir = new File( System.getProperty( "java.io.tmpdir" ), UUID.randomUUID().toString() );
        /**
         * The directory that contains the local data
         */
        public File localDir = new File( System.getProperty( "java.io.tmpdir" ), UUID.randomUUID().toString() );

        private Config(final Properties properties) {
            config = properties;
            metaDataset = TDBFactory.createDataset( metaDir.getAbsolutePath() );
            localDataset = TDBFactory.createDataset( localDir.getAbsolutePath() );
        }

        private Config(final Properties properties, final ZipInputStream zis) throws FileExistsException, IOException {
            config = properties;
            readFiles( zis );
            metaDataset = TDBFactory.createDataset( metaDir.getAbsolutePath() );
            localDataset = TDBFactory.createDataset( localDir.getAbsolutePath() );            
        }

        private void readFiles(final ZipInputStream zis) throws FileExistsException, IOException {
            localDir.mkdirs();
            metaDir.mkdirs();
            ZipEntry e = zis.getNextEntry();
            File dir = null;
            String prefix = null;
            while (e != null) {
                final String name = e.getName();
                if (!name.endsWith( "/tdb.lock" )) {
                    if (name.startsWith( DatasetProducer.META_PREFIX )) {
                        prefix = DatasetProducer.META_PREFIX;
                        dir = metaDir;
                    } else if (name.startsWith( DatasetProducer.LOCAL_PREFIX )) {
                        prefix = DatasetProducer.LOCAL_PREFIX;
                        dir = localDir;
                    } else {
                        prefix = null;
                        dir = null;
                    }
                    if (prefix == null) {
                        log.warn( "skipping " + e.getName() + " files in configuration" );
                    } else {
                        final int prefixLen = prefix.length() + 1;
                        File f = null;
                        f = new File( dir, name.substring( prefixLen ) );

                        if (!f.createNewFile()) {
                            throw new FileExistsException( f.getName() );
                        }
                        final FileOutputStream fos = new FileOutputStream( f );
                        try {
                            IOUtils.copy( zis, fos );
                        } finally {
                            IOUtils.closeQuietly( fos );
                            zis.closeEntry();
                        }
                    }
                }
                e = zis.getNextEntry();

            }
        }
    }

    public TDBDatasetProducer(final Config cfg) {
        super( cfg );
        this.cfg = cfg;
    }

    public TDBDatasetProducer(final Properties properties) {
        this( new Config( properties ) );
    }

    public TDBDatasetProducer(final Properties properties, final ZipInputStream zis) throws IOException {
        this( new Config( properties, zis ) );
    }

    @Override
    public void close() {
        super.close();
        FileUtils.deleteQuietly( cfg.metaDir );
        FileUtils.deleteQuietly( cfg.localDir );
    }

    @Override
    protected void saveLocal(final ZipOutputStream out) throws IOException {
        saveDataset( out, cfg.localDir, DatasetProducer.LOCAL_PREFIX, cfg.localDir.getAbsolutePath() );
    }

    @Override
    protected void saveMeta(final ZipOutputStream out) throws IOException {
        saveDataset( out, cfg.metaDir, DatasetProducer.META_PREFIX, cfg.metaDir.getAbsolutePath() );
    }

    private String createFN(final String prefix, final String fnPrefix, final File f) {
        final String fPart = f.getAbsolutePath().substring( fnPrefix.length() );
        return prefix + fPart;
    }

    private void saveDataset(final ZipOutputStream out, final File dir, final String prefix, final String fnPrefix) throws IOException {
        FileFilter filter = new AndFileFilter( FileFileFilter.FILE,
                new NotFileFilter( new NameFileFilter( "tdb.lock" ) ) );
        for (final File f : dir.listFiles( filter )) {
            final ZipEntry e = new ZipEntry( createFN( prefix, fnPrefix, f ) );
            out.putNextEntry( e );
            try {
                IOUtils.copyLarge( new FileInputStream( f ), out );
            } finally {
                out.closeEntry();
            }
        }
        filter = new AndFileFilter( DirectoryFileFilter.INSTANCE,
                new NotFileFilter( new OrFileFilter( new NameFileFilter( "." ), new NameFileFilter( ".." ) ) ) );
        for (final File f : dir.listFiles( filter )) {
            saveDataset( out, f, prefix, fnPrefix );
        }
    }

}