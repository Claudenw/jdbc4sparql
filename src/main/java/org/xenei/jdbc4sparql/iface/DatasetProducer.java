package org.xenei.jdbc4sparql.iface;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;

/**
 * Interface that defines the dataset producer.
 * 
 * The dataset producer produces the local dataset (set of graphs that 
 * represent the local data) and the meta dataset (set of graphs that 
 * contain the metadata)
 * 
 * Implementations of this class should construct the dataset when first 
 * requested and return the same dataset on all subsequent calls.
 */
public interface DatasetProducer
{
	/**
	 * Get or construct the local dataset.
	 * @return the local dataset
	 */
	public Dataset getLocalDataset();
	
	/**
	 * Get or construct the meta dataset.
	 * @return the meta dataset.
	 */
	public Dataset getMetaDataset();
	
	/**
	 * Retrieve the model that is the union of all models in the data set.
	 * @return
	 */
	public Model getMetaDatasetUnionModel();
	
	/**
	 * Close the datasets in preparation for shutdown.
	 */
	public void close();
}
