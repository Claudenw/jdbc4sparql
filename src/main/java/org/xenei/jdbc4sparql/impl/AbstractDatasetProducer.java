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
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.xerces.util.XMLChar;
import org.xenei.jdbc4sparql.J4SPropertyNames;
import org.xenei.jdbc4sparql.iface.DatasetProducer;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.utils.NoCloseZipInputStream;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.impl.EntityManagerImpl;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.impl.Util;

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
	public static String getModelURI(EntityManager mgr,final String modelName) {
		String name = StringUtils.defaultString(modelName);
		if (StringUtils.isEmpty(name)) {
			name = RdfCatalog.Builder.getFQName(mgr,name);
		}
		else {
			final int i = Util.splitNamespaceXML(name);
			if (i == 1) {
				if (XMLChar.isNCNameStart(name.charAt(0))) {// we have a short
					// name
					name = RdfCatalog.Builder.getFQName(mgr,name);
				}
			}
		}
		return name;
	}

	private final Properties properties;

	private final RDFFormat format = RDFFormat.TRIG;

	protected final Dataset localData;
	protected final EntityManager localMgr;
	protected final Dataset metaData;
	protected final EntityManager metaMgr;

	protected AbstractDatasetProducer(final Properties properties,
			final Dataset metaDataset, final Dataset localDataset) {
		this.properties = properties;
		this.metaData = metaDataset;
		this.localData = localDataset;
		this.metaMgr = new EntityManagerImpl( metaData );
		this.localMgr = new EntityManagerImpl( localData );
	}

	@Override
	public void addLocalDataModel(final String modelName, final Model model) {
		final String name = AbstractDatasetProducer.getModelURI(localMgr,modelName);
		getLocalDataset().addNamedModel(name, model);
	}

	/**
	 * Close the datasets in preparation for shutdown.
	 */
	@Override
	public void close() {
		getMetaDataset().close();
		getLocalDataset().close();
	}

	private String createFN(final String prefix) {
		return String.format("%s.%s", prefix, format.getLang()
				.getFileExtensions().get(0));
	}

	@Override
	public EntityManager getLocalDataEntityManager(final String modelName) {
		return getEntityManager(getLocalDataset(), modelName);
	}

	/**
	 * Get or construct the local dataset.
	 *
	 * @return the local dataset
	 */
	protected Dataset getLocalDataset() {
		return localData;
	}

	@Override
	public EntityManager getMetaDataEntityManager(final String modelName) {
		return getEntityManager(getMetaDataset(), modelName);
	}

	/**
	 * Get or construct the meta dataset.
	 *
	 * @return the meta dataset.
	 */
	protected Dataset getMetaDataset() {
		return metaData;
	}

	/**
	 * Retrieve the model that is the union of all models in the data set.
	 *
	 * @return
	 */
	@Override
	public Model getMetaDatasetUnionModel() {
		return getMetaDataset().getNamedModel("urn:x-arq:UnionGraph");
	}

	private EntityManager getEntityManager(final Dataset dataset, final String modelName) {
		final String name = AbstractDatasetProducer.getModelURI(localMgr, modelName);
		if (!dataset.containsNamedModel(name)) {
			dataset.addNamedModel(name, ModelFactory.createDefaultModel());
		}
		
		return new EntityManagerImpl( model );

	}

	@Override
	public Properties getProperties() {
		return new Properties(properties);
	}

	@Override
	public Iterator<String> listMetaDataNames() {
		return getMetaDataset().listNames();
	}

	/**
	 * Default load implementation
	 *
	 * @param zis
	 * @throws IOException
	 */
	protected void load(final ZipInputStream zis) throws IOException {
		ZipEntry e = zis.getNextEntry();

		if (e.getName().startsWith(DatasetProducer.META_PREFIX)) {
			loadMeta(zis, e);
		}
		else {
			throw new IllegalStateException("Entry must start with "
					+ DatasetProducer.META_PREFIX);
		}
		e = zis.getNextEntry();
		if (e.getName().startsWith(DatasetProducer.LOCAL_PREFIX)) {
			loadLocal(zis, e);
		}
		else {
			throw new IllegalStateException("Entry must start with "
					+ DatasetProducer.LOCAL_PREFIX);
		}

	}

	private void loadDataset(final ZipInputStream zis, final ZipEntry e,
			final Dataset ds, final String prefix) {
		if (e.getName().equals(createFN(prefix))) {

			RDFDataMgr.read(ds, new NoCloseZipInputStream(zis),
					format.getLang());
		}
		else {
			throw new IllegalArgumentException("Entry name must be "
					+ createFN(prefix));
		}
	}

	protected void loadLocal(final ZipInputStream zis, final ZipEntry e) {
		loadDataset(zis, e, localData, DatasetProducer.LOCAL_PREFIX);
	}

	protected void loadMeta(final ZipInputStream zis, final ZipEntry e) {
		loadDataset(zis, e, metaData, DatasetProducer.META_PREFIX);
	}

	@Override
	final public void save(final File f) throws FileNotFoundException,
	IOException {
		save(new FileOutputStream(f));
	}

	@Override
	final public void save(final OutputStream out) throws IOException {
		final ZipOutputStream zos = new ZipOutputStream(out);
		try {
			properties.setProperty(J4SPropertyNames.DATASET_PRODUCER, this
					.getClass().getCanonicalName());

			final ZipEntry e = new ZipEntry(
					DatasetProducer.PROPERTIES_ENTRY_NAME);
			zos.putNextEntry(e);
			properties.store(zos, "");
			zos.closeEntry();

			saveMeta(zos);

			saveLocal(zos);
		} finally {
			zos.close();
		}
	}

	private void saveDataset(final ZipOutputStream out, final Dataset ds,
			final String prefix) throws IOException {
		final ZipEntry e = new ZipEntry(createFN(prefix));
		out.putNextEntry(e);
		RDFDataMgr.write(out, ds, format);
		out.closeEntry();
	}

	protected void saveLocal(final ZipOutputStream out) throws IOException {
		saveDataset(out, getLocalDataset(), DatasetProducer.LOCAL_PREFIX);
	}

	protected void saveMeta(final ZipOutputStream out) throws IOException {
		saveDataset(out, getMetaDataset(), DatasetProducer.META_PREFIX);
	}

}
