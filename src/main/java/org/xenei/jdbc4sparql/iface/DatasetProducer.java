package org.xenei.jdbc4sparql.iface;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.Iterator;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.lang.StringUtils;
import org.xenei.jdbc4sparql.J4SPropertyNames;

import com.hp.hpl.jena.rdf.model.Model;

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
public interface DatasetProducer {
	public static class Loader {
		public static DatasetProducer load(final Properties props)
				throws IOException {
			if (StringUtils.isEmpty(props
					.getProperty(J4SPropertyNames.DATASET_PRODUCER))) {
				throw new IllegalStateException(
						J4SPropertyNames.DATASET_PRODUCER + " property not set");
			}
			try {
				final Class<? extends DatasetProducer> clazz = Class.forName(
						props.getProperty(J4SPropertyNames.DATASET_PRODUCER))
						.asSubclass(DatasetProducer.class);
				final Constructor<? extends DatasetProducer> c = clazz
						.getConstructor(Properties.class);
				return c.newInstance(props);
			} catch (final ClassNotFoundException e1) {
				throw new IllegalStateException(e1);
			} catch (final NoSuchMethodException e1) {
				throw new IllegalStateException(e1);
			} catch (final SecurityException e1) {
				throw new IllegalStateException(e1);
			} catch (final InstantiationException e1) {
				throw new IllegalStateException(e1);
			} catch (final IllegalAccessException e1) {
				throw new IllegalStateException(e1);
			} catch (final InvocationTargetException e1) {
				throw new IllegalStateException(e1);
			}
		}

		public static DatasetProducer load(final Properties properties,
				final URL url) throws IOException {
			final ZipInputStream zis = new ZipInputStream(url.openStream());
			// read properties
			final ZipEntry e = zis.getNextEntry();
			final String name = e.getName();
			if (!DatasetProducer.PROPERTIES_ENTRY_NAME.equals(name)) {
				throw new IllegalStateException(
						DatasetProducer.PROPERTIES_ENTRY_NAME
						+ " was not the first entry");
			}
			final Properties props = new Properties();
			props.load(zis);

			if (StringUtils.isEmpty(props
					.getProperty(J4SPropertyNames.DATASET_PRODUCER))) {
				throw new IllegalStateException(
						J4SPropertyNames.DATASET_PRODUCER + " property not set");
			}

			// merge the properties
			properties.setProperty(J4SPropertyNames.DATASET_PRODUCER,
					props.getProperty(J4SPropertyNames.DATASET_PRODUCER));
			final Properties finalProps = new Properties(props);
			finalProps.putAll(properties);
			try {
				final Class<? extends DatasetProducer> clazz = Class.forName(
						props.getProperty(J4SPropertyNames.DATASET_PRODUCER))
						.asSubclass(DatasetProducer.class);
				final Constructor<? extends DatasetProducer> c = clazz
						.getConstructor(Properties.class, ZipInputStream.class);
				return c.newInstance(finalProps, zis);
			} catch (final ClassNotFoundException e1) {
				throw new IllegalStateException(e1);
			} catch (final NoSuchMethodException e1) {
				throw new IllegalStateException(e1);
			} catch (final SecurityException e1) {
				throw new IllegalStateException(e1);
			} catch (final InstantiationException e1) {
				throw new IllegalStateException(e1);
			} catch (final IllegalAccessException e1) {
				throw new IllegalStateException(e1);
			} catch (final InvocationTargetException e1) {
				throw new IllegalStateException(e1);
			}
		}
	}

	public static final String PROPERTIES_ENTRY_NAME = "/META_INF/properties.txt";
	public static final String META_PREFIX = "/meta";

	public static final String LOCAL_PREFIX = "/local";

	public void addLocalDataModel(final String modelName, Model model);

	/**
	 * Close the datasets in preparation for shutdown.
	 */
	public void close();

	public Model getLocalDataModel(final String modelName);

	/**
	 * Get or construct the local dataset.
	 *
	 * @return the local dataset
	 */
	// public Dataset getLocalDataset();

	public Model getMetaDataModel(final String modelName);

	/**
	 * Retrieve the model that is the union of all models in the data set.
	 *
	 * @return
	 */
	public Model getMetaDatasetUnionModel();

	public Properties getProperties();

	public Iterator<String> listMetaDataNames();

	/**
	 * Get or construct the meta dataset.
	 *
	 * @return the meta dataset.
	 */
	// public Dataset getMetaDataset();

	public void save(File f) throws IOException, FileNotFoundException;

	public void save(final OutputStream out) throws IOException;
}
