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

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.tdb.transaction.DatasetGraphTransaction;

public class TDBDatasetProducer extends AbstractDatasetProducer {
	private final Lock metaDataLock = new ReentrantLock();
	private boolean metaDataLoaded = false;
	private final File metaDir;

	private final Lock localDataLock = new ReentrantLock();
	private boolean localDataLoaded = false;
	private final File localDir;

	private final Logger log = LoggerFactory
			.getLogger(TDBDatasetProducer.class);

	public TDBDatasetProducer(final Properties properties) {
		super(properties, null, null);
		metaDir = new File(System.getProperty("java.io.tmpdir"), UUID
				.randomUUID().toString());
		metaData = TDBFactory.createDataset(metaDir.getAbsolutePath());
		metaDataLoaded = true;
		localDir = new File(System.getProperty("java.io.tmpdir"), UUID
				.randomUUID().toString());
		localData = TDBFactory.createDataset(localDir.getAbsolutePath());
		localDataLoaded = true;
	}

	public TDBDatasetProducer(final Properties properties,
			final ZipInputStream zis) throws IOException {
		super(properties, null, null);
		metaDir = new File(System.getProperty("java.io.tmpdir"), UUID
				.randomUUID().toString());
		localDir = new File(System.getProperty("java.io.tmpdir"), UUID
				.randomUUID().toString());
		load(zis);
		// zis.close(); closed in another thread
	}

	@Override
	public void close() {
		super.close();
		FileUtils.deleteQuietly(metaDir);
		FileUtils.deleteQuietly(localDir);
	}

	@Override
	public Dataset getLocalDataset() {
		if (!localDataLoaded) {
			localDataLock.lock();
			try {
				// if synchronized was captured then data is loaded
				if (!localDataLoaded) {
					throw new IllegalStateException(
							"Local data should be loaded");
				}
			} finally {
				localDataLock.unlock();
			}
		}
		return localData;
	}

	@Override
	public Dataset getMetaDataset() {
		if (!metaDataLoaded) {
			metaDataLock.lock();
			try {
				// if lock was captured then data is loaded
				if (!metaDataLoaded) {
					throw new IllegalStateException("Metadata should be loaded");
				}
			} finally {
				metaDataLock.unlock();

			}
		}
		return metaData;
	}

	@Override
	public void load(final ZipInputStream zis) throws IOException {
		final CountDownLatch latch = new CountDownLatch(1);
		new Thread(null, new Runnable() {

			@Override
			public void run() {
				try {
					threadLoad(latch, zis);
				} finally {
					IOUtils.closeQuietly(zis);
				}

			}
		}, "loadingThread").start();
		// allow other thread to get started

		try {
			latch.await();
		} catch (final InterruptedException e) {
			log.info("Load pause interrupted", e);
		}
	}

	private ZipEntry readFiles(final ZipInputStream zis, ZipEntry e,
			final String prefix, final File dir) throws FileExistsException,
			IOException {
		if ((e == null) || !e.getName().startsWith(prefix)) {
			log.warn("No " + prefix + " files in configuration");
		}
		else {
			final int prefixLen = prefix.length() + 1;
			String name = null;
			File f = null;
			while ((e != null) && e.getName().startsWith(prefix)) {
				name = e.getName();
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
				e = zis.getNextEntry();
			}
		}
		return e;
	}

	private void saveDir(final ZipOutputStream out, final File dir,
			final String pth) throws IOException {
		for (final File f : dir.listFiles()) {
			final String nm = String.format("%s/%s", pth, f.getName());
			if (f.isDirectory()) {
				saveDir(out, f, nm);
			}
			else {
				final ZipEntry e = new ZipEntry(nm);
				out.putNextEntry(e);
				try {
					final FileInputStream fis = new FileInputStream(f);
					try {
						IOUtils.copy(fis, out);
					} finally {
						IOUtils.closeQuietly(fis);
					}
				} finally {
					out.closeEntry();
				}
			}
		}
	}

	@Override
	protected void saveLocal(final ZipOutputStream out) throws IOException {
		localDataLock.lock();
		try {
			localDataLoaded = false;
			((DatasetGraphTransaction) (localData.asDatasetGraph())).sync();
			saveDir(out, localDir, DatasetProducer.LOCAL_PREFIX);
			localDataLoaded = true;
		} finally {
			localDataLock.unlock();
		}
	}

	@Override
	protected void saveMeta(final ZipOutputStream out) throws IOException {
		metaDataLock.lock();
		try {
			metaDataLoaded = false;
			((DatasetGraphTransaction) (metaData.asDatasetGraph())).sync();
			saveDir(out, metaDir, DatasetProducer.META_PREFIX);
			metaDataLoaded = true;
		} finally {
			metaDataLock.unlock();
		}
	}

	private void threadLoad(final CountDownLatch latch, final ZipInputStream zis) {

		localDataLock.lock();
		try {
			ZipEntry e = null;
			metaDataLock.lock();
			try {
				latch.countDown(); // release the calling thread
				metaDir.mkdirs();
				e = readFiles(zis, zis.getNextEntry(),
						DatasetProducer.META_PREFIX, metaDir);
				metaData = TDBFactory.createDataset(metaDir.getAbsolutePath());
				metaDataLoaded = true;
			} catch (final Exception e1) { // must catch here as when we unlock
				// app may stop on failure.
				log.error("Error reading meta data stream", e1);
				return;
			} finally {
				metaDataLock.unlock();
			}
			localDir.mkdirs();
			e = readFiles(zis, e, DatasetProducer.LOCAL_PREFIX, localDir);
			localData = TDBFactory.createDataset(localDir.getAbsolutePath());
			localDataLoaded = true;
		} catch (final Exception e1) { // must catch here as when we unlock app
			// may stop on failure.
			log.error("Error reading local data stream", e1);
			return;
		} finally {
			localDataLock.unlock();
		}

	}
}