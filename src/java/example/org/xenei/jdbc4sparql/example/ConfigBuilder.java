package org.xenei.jdbc4sparql.example;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.jena.riot.RDFLanguages;
import org.xenei.jdbc4sparql.J4SConnection;
import org.xenei.jdbc4sparql.J4SDriver;
import org.xenei.jdbc4sparql.J4SUrl;
import org.xenei.jdbc4sparql.config.ConfigSerializer;
import org.xenei.jdbc4sparql.config.ModelWriter;
import org.xenei.jdbc4sparql.sparql.builders.RDFSBuilder;

public class ConfigBuilder
{

	private static void getMetaData( final J4SConnection connection )
			throws SQLException
	{
		final DatabaseMetaData dmd = connection.getMetaData();
		final ResultSet rs = dmd.getTables("catalog", "schema", null, null);
		while (rs.next())
		{
			ConfigBuilder.listTable(dmd, rs.getString("TABLE_CAT"),
					rs.getString("TABLE_SCHEM"), rs.getString("TABLE_NAME"));
		}
	}

	private static void listTable( final DatabaseMetaData dmd,
			final String catalog, final String schema, final String table )
			throws SQLException
	{

		final ResultSet rs = dmd.getColumns(catalog, schema, table, null);
		while (rs.next())
		{
			final String s = String.format(
					"Cat %s Schema %s tbl %s col %s Nullable: '%s'", catalog,
					schema, table, rs.getString("COLUMN_NAME"),
					rs.getString("IS_NULLABLE"));
			System.out.println(s);
		}
	}

	/**
	 * @param args
	 * @throws URISyntaxException
	 * @throws IOException
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public static void main( final String[] args ) throws URISyntaxException,
			IOException, SQLException, ClassNotFoundException
	{
		// String [] schemaFiles = {
		// "rdf_ElementsGr2.rdf",
		// "rdf_am-people-rdagr2-schema.ttl",
		// "rdf_am-people-skos-schema.ttl",
		// "rdf_am-schema.ttl",
		// "rdf_am-thesaurus-schema.ttl"
		// };
		final String[] schemaFiles = { "foaf.rdf" };
		final Model ontologyModel = ModelFactory.createDefaultModel();
		// read the schemas
		URL fUrl = null;
		for (final String s : schemaFiles)
		{
			fUrl = ConfigBuilder.class.getResource(String.format("./%s", s));
			ontologyModel.read(fUrl.toURI().toASCIIString(), RDFLanguages
					.filenameToLang(fUrl.getPath()).getName());
		}
		final RDFSBuilder builder = new RDFSBuilder(ontologyModel);

		final Model dataModel = ModelFactory.createDefaultModel();
		;

		final URL cfgUrl = new URL(fUrl.toExternalForm().replace("foaf.rdf",
				"example.ttl"));

		// SimpleBuilder builder = new SimpleBuilder();
		final SparqlCatalog catalog = new SparqlCatalog("http://example.com/",
				dataModel, "catalog");
		final SparqlSchema schema = new SparqlSchema(catalog,
				"http://example.com/", "schema");
		catalog.addSchema(schema);
		schema.addTableDefs(builder.getTableDefs(catalog));
		final ConfigSerializer cs = new ConfigSerializer();
		cs.add(catalog);
		cs.save(new ModelWriter(new File(cfgUrl.getPath())));
		// cs.save( new ModelWriter( System.out));

		Class.forName("org.xenei.jdbc4sparql.J4SDriver");

		final J4SDriver driver = new J4SDriver();
		final J4SUrl url = new J4SUrl("jdbc:J4S:" + cfgUrl.toExternalForm());
		final J4SConnection connection = new J4SConnection(driver, url, null);
		connection.setCatalog("catalog");
		connection.setSchema("schema");
		ConfigBuilder.getMetaData(connection);

		/*
		 * fUrl = ConfigBuilder.class.getResource("./rdf_am-data.ttl");
		 * 
		 * final J4SDriver driver = new J4SDriver();
		 * J4SUrl url = new J4SUrl("jdbc:J4S:"+ cfgUrl.toExternalForm() );
		 * J4SConnection connection = new J4SConnection( driver, url, null );
		 * connection.getModelReader().read( fUrl.toString() );
		 * connection.setCatalog(catalog.getLocalName());
		 * checkResults( connection );
		 */
	}
}
