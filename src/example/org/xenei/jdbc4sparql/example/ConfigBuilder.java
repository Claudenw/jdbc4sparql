package org.xenei.jdbc4sparql.example;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.tdb.TDBFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.jena.riot.RDFLanguages;
import org.xenei.jdbc4sparql.J4SConnection;
import org.xenei.jdbc4sparql.J4SDriver;
import org.xenei.jdbc4sparql.J4SPropertyNames;
import org.xenei.jdbc4sparql.J4SUrl;
import org.xenei.jdbc4sparql.config.TDBDatasetProducer;
import org.xenei.jena.entities.MissingAnnotation;

public class ConfigBuilder
{

	private static File gatherSchema( final CommandLine cmd )
			throws IOException, URISyntaxException
	{

		final File dir = File.createTempFile("configBldr", "schema");
		dir.delete();
		dir.mkdir();
		final Dataset ds = TDBFactory.createDataset(dir.getCanonicalPath());
		try
		{
			// final Model ontologyModel = ModelFactory.createInfModel(
			// ReasonerRegistry.(), ds.getDefaultModel());
			final Model ontologyModel = ModelFactory.createRDFSModel(ds
					.getDefaultModel());

			URL fUrl = null;
			for (final String s : cmd.getOptionValues("f"))
			{
				fUrl = ConfigBuilder.class.getResource(s);
				System.out.println("Processing: " + fUrl);
				if (fUrl == null)
				{
					throw new IllegalArgumentException("Can not locate " + s);
				}
				ontologyModel.read(fUrl.toURI().toASCIIString(), RDFLanguages
						.filenameToLang(fUrl.getPath()).getName());
			}

			System.out.println("creating output file");
			final File outfile = File.createTempFile("cfgbld", ".ttl");
			final FileOutputStream fos = new FileOutputStream(outfile);
			try
			{
				ontologyModel.write(fos, "TURTLE");
			}
			finally
			{
				fos.close();
				ontologyModel.close();
				System.out.println("created output file: "
						+ outfile.getCanonicalPath());
			}
			return outfile;
		}
		finally
		{
			System.out.print("Cleaning up.");
			ds.close();
			System.out.print(".");
			FileUtils.deleteQuietly(dir);
			System.out.println(". complete");
		}
	}

	private static Options getOptions()
	{
		final Options retval = new Options();

		Option opt = new Option("c", "catalog", true, "The catalog name");
		opt.setRequired(true);
		retval.addOption(opt);

		retval.addOption("h", "help", false, "This help display");

		opt = new Option("b", "builder", true,
				"The schema builder class to use");
		opt.setRequired(true);
		retval.addOption(opt);

		opt = new Option("f", "file", true,
				"A file to add to the input.  Multiple files may be used");
		opt.setRequired(true);
		retval.addOption(opt);

		opt = new Option("o", "output", true, "The name for the output file.");
		opt.setRequired(true);
		retval.addOption(opt);

		// retval.addOption( "d", "dataset", false,
		// "The dataset producer class to use (defaults to TDBDataset)");

		// retval.addOption("c", "catalog", true,
		// "The name for the catalog being built." );
		return retval;
	}

	private static void help()
	{
		final HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("ConfigBuilder", ConfigBuilder.getOptions());
	}

	/**
	 * @param args
	 * @throws URISyntaxException
	 * @throws IOException
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws MissingAnnotation
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * @throws ParseException
	 */
	public static void main( final String[] args ) throws URISyntaxException,
			IOException, SQLException, ClassNotFoundException,
			InstantiationException, IllegalAccessException, MissingAnnotation,
			ParseException
	{
		final Options options = ConfigBuilder.getOptions();
		final CommandLineParser parser = new BasicParser();
		final CommandLine cmd = parser.parse(options, args);

		if (cmd.hasOption("h"))
		{
			ConfigBuilder.help();
		}

		final File outFile = ConfigBuilder.gatherSchema(cmd);

		final J4SDriver driver = new J4SDriver();

		// final String urlStr =
		// "jdbc:j4s?builder=org.xenei.jdbc4sparql.sparql.builders.RDFSBuilder&type=turtle:file:"
		final String urlStr = String.format(
				"jdbc:j4s?builder=%s&type=turtle:file:%s",
				cmd.getOptionValue("b"), outFile.getCanonicalPath());

		final J4SUrl url = new J4SUrl(urlStr);

		System.out.println("Opening " + url);

		final Properties properties = new Properties();
		properties.setProperty(J4SPropertyNames.DATASET_PRODUCER,
				TDBDatasetProducer.class.getCanonicalName());
		properties.setProperty("catalog", cmd.getOptionValue("c"));

		final J4SConnection connection = new J4SConnection(driver, url,
				properties);

		final DatabaseMetaData metaData = connection.getMetaData();

		System.out.println("creating metadata file");
		final File outfile = File.createTempFile("cfgmtd", ".ttl");
		final FileOutputStream fos2 = new FileOutputStream(outfile);
		System.out.println("Writing metadata to " + outfile.getCanonicalPath());
		connection.getDatasetProducer().getMetaDatasetUnionModel()
				.write(fos2, "N-TRIPLE");

		metaData.getColumns(null, null, null, null);
		System.out.println("Writing configuration to "
				+ cmd.getOptionValue("o"));
		final File f = new File(cmd.getOptionValue("o"));
		final FileOutputStream fos = new FileOutputStream(f);
		connection.saveConfig(fos);
		fos.close();
		connection.close();
		System.out.println("done");
	}
}
