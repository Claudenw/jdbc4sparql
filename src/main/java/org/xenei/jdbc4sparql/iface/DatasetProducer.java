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

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdfconnection.RDFConnection;
import org.xenei.jdbc4sparql.J4SPropertyNames;
import org.xenei.jena.entities.EntityManager;


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
    
    /**
     * A class to load the data set from a standard stored repository.
     *
     */
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

		/**
		 * Load the producer with the properties and the specified zip input stream
		 * @param properties the properties for the producer
		 * @param url the URL of the zip file containing the values.
		 * @return the configured dataset producer.
		 * @throws IOException on error.
		 */
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

	/**
	 * The entry name for the properties in the zip file.
	 */
	public static final String PROPERTIES_ENTRY_NAME = "/META_INF/properties.txt";
	/**
	 * The entry prefix for meta data.
	 */
	public static final String META_PREFIX = "/meta";

	/**
	 * The entry prefix for local data.
	 */
	public static final String LOCAL_PREFIX = "/local";

	/**
	 * Get the connection to the local data.
	 * @return the local data connection.
	 */
	public RDFConnection getLocalConnection();

	/**
	 * Close the datasets in preparation for shutdown.
	 */
	public void close();


	/**
	 * Get an EntityManager on the default metadata dataset.
	 * @return an EntityManager.
	 */
	public EntityManager getMetaDataEntityManager();
	
	/**
     * Get an EntityManager on the specified metadata dataset.
     * @param datasetName the name of the dataset to retrieve.  This is a named graph in the metadata dataset.
     * @return an EntityManager.
     */
    public EntityManager getMetaDataEntityManager(final String datasetName);
    
    /**
     * Get the connection the metadata dataset.
     * @return the metadata connection.
     */
	public RDFConnection getMetaConnection();

	/**
	 * @return The properties this producer was created with.
	 */
	public Properties getProperties();

	/**
	 * @return an iterator over the names of the graphs in the metadata dataset.
	 */
	public Iterator<String> listMetaDataNames();

	/**
	 * Save this producer to the specified file.
	 * 
	 * Dataset producer is saved as a zip file containing properties, local and metadata.
	 * 
	 * @param file The file to save to.
	 * @throws IOException on output error.
	 * @throws FileNotFoundException if file can not be created.
	 */
	public void save(File file) throws IOException, FileNotFoundException;

	/**
	 * Save this producer to the spcified output stream.
	 * 
     * Dataset producer is streamed as a zip file containing properties, local and metadata.
     * 
	 * @param out the output stream to send the producer to.
	 * @throws IOException on output error.
	 */
	public void save(final OutputStream out) throws IOException;
}
