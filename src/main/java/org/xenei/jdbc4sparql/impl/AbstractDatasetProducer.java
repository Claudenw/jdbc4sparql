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
import org.apache.jena.util.iterator.WrappedIterator;
import org.apache.xerces.util.XMLChar;
import org.xenei.jdbc4sparql.J4SPropertyNames;
import org.xenei.jdbc4sparql.iface.DatasetProducer;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.utils.NoCloseZipInputStream;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.impl.EntityManagerImpl;
import org.apache.jena.arq.querybuilder.SelectBuilder;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.impl.Util;
import org.apache.jena.rdfconnection.RDFConnection;

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

	protected EntityManager localMgr;
	protected EntityManager metaMgr;

	protected AbstractDatasetProducer(final Properties properties,
			final Dataset metaDataset, final Dataset localDataset) {
		this.properties = properties;
		this.metaMgr = new EntityManagerImpl( metaDataset );
		this.localMgr = new EntityManagerImpl( localDataset );
	}

	@Override
	public void addLocalDataModel(final String modelName, final Model model) {
		final String name = AbstractDatasetProducer.getModelURI(localMgr,modelName);
		getLocalEntityManager().getConnection().put(name, model);
	}

	/**
	 * Close the datasets in preparation for shutdown.
	 */
	@Override
	public void close() {
		getMetaDataEntityManager().getConnection().close();
		getLocalEntityManager().getConnection().close();
	}

	private String createFN(final String prefix) {
		return String.format("%s.%s", prefix, format.getLang()
				.getFileExtensions().get(0));
	}

	@Override
	public EntityManager getLocalDataEntityManager(final String modelName) {
		return getEntityManager(getLocalEntityManager(), modelName);
	}

	/**
	 * Get or construct the local dataset.
	 *
	 * @return the local dataset
	 */
	protected EntityManager getLocalEntityManager() {
		return localMgr;
	}

	@Override
	public EntityManager getMetaDataEntityManager(final String modelName) {
		return getEntityManager(getMetaDataEntityManager(), modelName);
	}

	/**
	 * Get or construct the meta dataset.
	 *
	 * @return the meta dataset.
	 */
	public EntityManager getMetaDataEntityManager() {
		return metaMgr;
	}

//	/**
//	 * Retrieve the model that is the union of all models in the data set.
//	 *
//	 * @return
//	 */
//	@Override
//	public EntityManager getMetaDatasetUnionConnection() {
//		return getMetaEntityManager("urn:x-arq:UnionGraph");//.getConnection()
//			//	.fetch("urn:x-arq:UnionGraph");
//	}

	private EntityManager getEntityManager(final EntityManager dataset, final String modelName) {
		final String name = AbstractDatasetProducer.getModelURI(localMgr, modelName);
		return dataset.getNamedManager(NodeFactory.createURI(name));
	}

	@Override
	public Properties getProperties() {
		return new Properties(properties);
	}

	@Override
	public Iterator<String> listMetaDataNames() {
		SelectBuilder sb = new SelectBuilder().addVar( "?g" )
				.addGraph( "?g", new SelectBuilder().addWhere( "?s", "?p", "?o").setLimit(1));
		ResultSet rs = getMetaDataEntityManager().getConnection().query( sb.build() ).execSelect();
		return WrappedIterator.create(rs).mapWith( qs -> qs.getResource("g").getURI());
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
			final EntityManager em, final String prefix) {
		if (e.getName().equals(createFN(prefix))) {
			Dataset ds = DatasetFactory.create();
			RDFDataMgr.read(ds, new NoCloseZipInputStream(zis),
					format.getLang());
			em.getConnection().putDataset(ds);
		}
		else {
			throw new IllegalArgumentException("Entry name must be "
					+ createFN(prefix));
		}
	}

	protected void loadLocal(final ZipInputStream zis, final ZipEntry e) {
		loadDataset(zis, e, localMgr, DatasetProducer.LOCAL_PREFIX);
	}

	protected void loadMeta(final ZipInputStream zis, final ZipEntry e) {
		loadDataset(zis, e, metaMgr, DatasetProducer.META_PREFIX);
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

	private void saveDataset(final ZipOutputStream out, final EntityManager em,
			final String prefix) throws IOException {
		final ZipEntry e = new ZipEntry(createFN(prefix));
		out.putNextEntry(e);		
		RDFDataMgr.write(out, em.getConnection().fetchDataset(), format);
		out.closeEntry();
	}

	protected void saveLocal(final ZipOutputStream out) throws IOException {
		saveDataset(out, getLocalEntityManager(), DatasetProducer.LOCAL_PREFIX);
	}

	protected void saveMeta(final ZipOutputStream out) throws IOException {
		saveDataset(out, getMetaDataEntityManager(), DatasetProducer.META_PREFIX);
	}

}
