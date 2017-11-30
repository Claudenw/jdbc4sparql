package org.xenei.jdbc4sparql.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileExistsException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.iface.DatasetProducer;
import org.xenei.jdbc4sparql.impl.AbstractDatasetProducer;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.impl.EntityManagerImpl;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.tdb.transaction.DatasetGraphTransaction;

public class TDBDatasetProducer extends AbstractDatasetProducer {
	
	private final Config cfg;

	private static class Config extends AbstractDatasetProducer.Configuration {
		private final Logger log = LoggerFactory
				.getLogger(Config.class);
		public final File metaDir = new File(System.getProperty("java.io.tmpdir"), UUID
				.randomUUID().toString());
		public File localDir = new File(System.getProperty("java.io.tmpdir"), UUID
				.randomUUID().toString());
		
		
		Config(Properties properties)
		{
			metaDataset = TDBFactory.createDataset(metaDir.getAbsolutePath());				
			localDataset = TDBFactory.createDataset(localDir.getAbsolutePath());		
		}
		
		Config(Properties properties, ZipInputStream zis) throws FileExistsException,
		IOException
		{
			readFiles(zis);
			metaDataset = TDBFactory.createDataset(metaDir.getAbsolutePath());				
			localDataset = TDBFactory.createDataset(localDir.getAbsolutePath());	
		}
		
		private void readFiles(final ZipInputStream zis) throws FileExistsException,
				IOException {
			localDir.mkdirs();
			metaDir.mkdirs();
			ZipEntry e = zis.getNextEntry();
			File dir = null;
			String prefix = null;
			while (e != null) {
				String name = e.getName();

				if (name.startsWith( DatasetProducer.META_PREFIX))
				{
					prefix = DatasetProducer.META_PREFIX;
					dir = metaDir;
				} else if (name.startsWith( DatasetProducer.LOCAL_PREFIX))
				{
					prefix = DatasetProducer.LOCAL_PREFIX;
					dir = localDir;
				} else {
					prefix = null;
					dir = null;
				}
				if (prefix == null) {
					log.warn("skipping " + e.getName() + " files in configuration");
				}
				else {
					final int prefixLen = prefix.length() + 1;
					File f = null;
					f = new File(dir, name.substring(prefixLen));

					if (!f.createNewFile()) {
						throw new FileExistsException(f.getName());
					}
					final FileOutputStream fos = new FileOutputStream(f);
					try {
						IOUtils.copy(zis, fos);
					} finally {
						IOUtils.closeQuietly(fos);
						zis.closeEntry();
					}
				}
				e = zis.getNextEntry();
			}
		}


	}
	
	public TDBDatasetProducer(Config cfg) {
		super( cfg );
		this.cfg = cfg;
	}
		
	public TDBDatasetProducer(final Properties properties) {
		this(new Config( properties ));		
	}

	public TDBDatasetProducer(final Properties properties,
			final ZipInputStream zis) throws IOException {
		this(new Config( properties, zis ));		
	}

	@Override
	public void close() {
		super.close();
		FileUtils.deleteQuietly(cfg.metaDir);
		FileUtils.deleteQuietly(cfg.localDir);
	}

}