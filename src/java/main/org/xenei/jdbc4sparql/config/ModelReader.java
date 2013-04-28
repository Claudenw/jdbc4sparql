package org.xenei.jdbc4sparql.config;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.util.FileUtils;

import java.io.InputStream;
import java.io.Reader;

/**
 * A class to read models into the driver.
 * 
 * This is used to populate local file tables when the configuration is reloaded
 * from disk. It does not cause a reparse of the configuration, but does replace
 * the
 * data in the model. This could also be used to repopulate the model with new
 * data.
 */
public abstract class ModelReader
{
	abstract public Model getModel();

	/**
	 * Read the input stream in to the model using base for relative URLs.
	 * 
	 * @param in
	 *            the input stream to read must be RDF/XML format.
	 * @param base
	 *            The base for relative URLs.
	 */
	public void read( final InputStream in, final String base )
	{
		this.read(in, base, "RDF/XML");
	}

	/**
	 * Read the input stream in the specified format using the specified base.
	 * 
	 * lang must be on of the languages supported by Apache Jena.
	 * 
	 * @param in
	 *            the stream to read.
	 * @param base
	 *            the base for relative URLs
	 * @param lang
	 *            the language (type) to parse the input stream as.
	 */
	public void read( final InputStream in, final String base, final String lang )
	{
		getModel().read(in, base, lang);
	}

	/**
	 * Read the model into the graph(s).
	 * 
	 * @param model
	 *            The model to read.
	 */
	public void read( final Model model )
	{
		getModel().add(model);
	}

	/**
	 * Read the reader in to the model using base for relative URLs.
	 * 
	 * @param reader
	 *            the reader to read, must be in RDF/XML format.
	 * @param base
	 *            The base for relative URLs.
	 */
	public void read( final Reader reader, final String base )
	{
		this.read(reader, base, "RDF/XML");
	}

	/**
	 * Read the reader into the model using base for relative URLs.
	 * 
	 * lang must be on of the languages supported by Apache Jena.
	 * 
	 * @param reader
	 *            the reader to read.
	 * @param base
	 *            The base for relative URLs.
	 * @param lang
	 *            the language (type) to parse the input stream as.
	 */
	public void read( final Reader reader, final String base, final String lang )
	{
		getModel().read(reader, base, lang);
	}

	/**
	 * Read the URL into the model.
	 * 
	 * The contents of the URL will be determined by url extension and will
	 * default to RDF/XML format.
	 * 
	 * @param url
	 *            The URL to read
	 */
	public void read( final String url )
	{
		this.read(url, FileUtils.guessLang(url));
	}

	/**
	 * Read the URL into the model using the lang format.
	 * 
	 * lang must be on of the languages supported by Apache Jena.
	 * 
	 * Relative URLs will be resolved to the URL.
	 * 
	 * @param url
	 *            The url to read.
	 * @param lang
	 *            the language of the content.
	 */
	public void read( final String url, final String lang )
	{
		this.read(url, "", lang);
	}

	/**
	 * Read the URL into the model using the lang. format and base for the base
	 * of relative URLs.
	 * 
	 * lang must be on of the languages supported by Apache Jena.
	 * 
	 * @param url
	 *            The url to read.
	 * @param base
	 *            The base to resolve relative URLs to.
	 * @param lang
	 *            the language of the content.
	 */
	public void read( final String url, final String base, final String lang )
	{
		getModel().read(url, base, lang);
	}

}
