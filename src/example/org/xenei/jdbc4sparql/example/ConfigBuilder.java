package org.xenei.jdbc4sparql.example;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.tdb.TDBFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.jena.riot.RDFLanguages;
import org.xenei.jdbc4sparql.J4SConnection;
import org.xenei.jdbc4sparql.J4SDriver;
import org.xenei.jdbc4sparql.J4SUrl;
import org.xenei.jena.entities.MissingAnnotation;

public class ConfigBuilder
{

	private static File gatherSchema( final List<String> schemaFiles )
			throws IOException, URISyntaxException
	{
		final File dir = File.createTempFile("configBldr", "schema");
		dir.delete();
		dir.mkdir();
		final Dataset ds = TDBFactory.createDataset(dir.getCanonicalPath());
		try
		{
			final Model ontologyModel = ds.getDefaultModel();
			URL fUrl = null;
			for (final String s : schemaFiles)
			{
				fUrl = ConfigBuilder.class.getResource(s);
				if (fUrl == null)
				{
					throw new IllegalArgumentException("Can not locate " + s);
				}
				ontologyModel.read(fUrl.toURI().toASCIIString(), RDFLanguages
						.filenameToLang(fUrl.getPath()).getName());
			}

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
			}
			return outfile;
		}
		finally
		{
			ds.close();
			FileUtils.deleteQuietly(dir);
		}
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
	 */
	public static void main( final String[] args ) throws URISyntaxException,
			IOException, SQLException, ClassNotFoundException,
			InstantiationException, IllegalAccessException, MissingAnnotation
	{
		final File outFile = ConfigBuilder.gatherSchema(Arrays.asList(args)
				.subList(1, args.length));

		final J4SDriver driver = new J4SDriver();
		final String urlStr = "jdbc:j4s?builder=org.xenei.jdbc4sparql.sparql.builders.RDFSBuilder&type=turtle:file:"
				+ outFile.getCanonicalPath();
		// + "/tmp/cfgbld4968555827338576078.ttl";
		final J4SUrl url = new J4SUrl(urlStr);

		final J4SConnection connection = new J4SConnection(driver, url,
				new Properties());

		final DatabaseMetaData metaData = connection.getMetaData();
		final ResultSet rs = metaData.getColumns(null, null, null, null);
		final ResultSetMetaData rsmd = rs.getMetaData();
		while (rs.next())
		{
			for (int i = 1; i <= rsmd.getColumnCount(); i++)
			{
				System.out.print(String.format("[%s: %s]",
						rsmd.getColumnName(i), rs.getString(i)));
			}
			System.out.println();
		}
		final File f = new File(args[0]);
		final FileOutputStream fos = new FileOutputStream(f);
		connection.saveConfig(fos);
		fos.close();
		connection.close();
		// RDFSBuilder builder = new RDFSBuilder(ontologyModel);
		//
		// ModelFactory.createDefaultModel();
		//
		// final URL cfgUrl = new URL(fUrl.toExternalForm().replace("foaf.rdf",
		// "example.ttl"));
		//
		// // SimpleBuilder builder = new SimpleBuilder();
		// // final Catalog catalog = new RdfCatalog.Builder()
		// // .setLocalModel(dataModel).setName( "catalog" ).build(model);
		//
		// // final Schema schema = new
		// //
		// RdfSchema.Builder().setCatalog((RdfCatalog)catalog).setName("schema").build(model);
		//
		// // schema.addTables(builder.getTables((RdfCatalog)catalog));
		// //
		// // final ConfigSerializer cs = new ConfigSerializer();
		// // cs.add(catalog);
		// // cs.save(new ModelWriter(new File(cfgUrl.getPath())));
		// // cs.save( new ModelWriter( System.out));
		//
		// Class.forName("org.xenei.jdbc4sparql.J4SDriver");
		//
		// url = new J4SUrl("jdbc:J4S:" + cfgUrl.toExternalForm());
		// connection = new J4SConnection(driver, url, new Properties());
		// connection.setCatalog("catalog");
		// connection.setSchema("schema");
		// ConfigBuilder.getMetaData(connection);
		// connection.close();
		//
		// /*
		// * fUrl = ConfigBuilder.class.getResource("./rdf_am-data.ttl");
		// *
		// * final J4SDriver driver = new J4SDriver();
		// * J4SUrl url = new J4SUrl("jdbc:J4S:"+ cfgUrl.toExternalForm() );
		// * J4SConnection connection = new J4SConnection( driver, url, null );
		// * connection.getModelReader().read( fUrl.toString() );
		// * connection.setCatalog(catalog.getLocalName());
		// * checkResults( connection );
		// */
	}
}
