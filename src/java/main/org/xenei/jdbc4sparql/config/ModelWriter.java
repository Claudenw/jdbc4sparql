package org.xenei.jdbc4sparql.config;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.util.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URL;
import java.net.URLConnection;

/**
 * Write a model.
 */
public class ModelWriter
{
	private OutputStream os;
	private Writer writer;
	private String lang;

	/**
	 * Write the model to a file
	 * 
	 * Language will be determined by the file extension and will default to
	 * TTL.
	 * 
	 * @param f
	 *            The file to write to.
	 * @throws FileNotFoundException
	 */
	public ModelWriter( final File f ) throws FileNotFoundException
	{
		this(new FileOutputStream(f), FileUtils.guessLang(f.getName(), "TTL"));
	}

	/**
	 * Write the model to a file with a specific language.
	 * 
	 * Lang must be one of the Apache Jena recognized languages.
	 * 
	 * @param f
	 *            The file to write to.
	 * @param lang
	 *            The language to write with.
	 * @throws FileNotFoundException
	 */
	public ModelWriter( final File f, final String lang )
			throws FileNotFoundException
	{
		this(new FileOutputStream(f), lang);
	}

	/**
	 * Write the model to an output stream in TTL language.
	 * 
	 * @param os
	 *            output stream to write to.
	 */
	public ModelWriter( final OutputStream os )
	{
		this(os, "TTL");
	}

	/**
	 * Write the model to an output stream with a specific language.
	 * 
	 * Lang must be one of the Apache Jena recognized languages.
	 * 
	 * @param os
	 *            the output stream
	 * @param lang
	 *            the lang
	 */
	public ModelWriter( final OutputStream os, final String lang )
	{
		this.os = os;
		this.lang = lang;
	}

	/**
	 * Write the model to a URL.
	 * 
	 * output format determiend by URL extension, defaults to TTL
	 * 
	 * @param url
	 *            The URL to write to.
	 * @throws IOException
	 */
	public ModelWriter( final URL url ) throws IOException
	{
		this(url, FileUtils.guessLang(url.getPath(), "TTL"));
	}

	/**
	 * Write the model to a URL
	 * 
	 * Lang must be one of the Apache Jena recognised languages.
	 * 
	 * URL must be capable of output.
	 * 
	 * @param url
	 *            The URL to write to.
	 * @param lang
	 *            The language to write with.
	 * @throws IOException
	 */
	public ModelWriter( final URL url, final String lang ) throws IOException
	{
		final URLConnection connection = url.openConnection();
		connection.setDoOutput(true);
		writer = new OutputStreamWriter(connection.getOutputStream());
	}

	/**
	 * Write the model to a writer in TTL language.
	 * 
	 * @param writer
	 *            the writer to write to.
	 */
	public ModelWriter( final Writer writer )
	{
		this(writer, "TTL");
	}

	/**
	 * Write the model to an output stream with a specific language.
	 * 
	 * Lang must be one of the Apache Jena recognized languages.
	 * 
	 * @param writer
	 *            the writer to write to.
	 * @param lang
	 *            the language to write in.
	 */
	public ModelWriter( final Writer writer, final String lang )
	{
		this.writer = writer;
		this.lang = lang;
	}

	/**
	 * Write the model to the writer output.
	 * 
	 * @param model
	 *            The model to write.
	 */
	public final void write( final Model model )
	{
		if (os == null)
		{
			try
			{
				model.write(writer, lang);
			}
			finally
			{
				try
				{
					writer.close();
				}
				catch (final IOException e)
				{
					// do nothing
				}
			}

		}
		else
		{
			try
			{
				model.write(os, lang);
			}
			finally
			{
				try
				{
					os.close();
				}
				catch (final IOException e)
				{
					// do nothing
				}
			}
		}
	}

}
