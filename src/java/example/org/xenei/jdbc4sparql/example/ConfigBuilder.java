package org.xenei.jdbc4sparql.example;

import com.hp.hpl.jena.rdf.model.InfModel;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


import org.apache.jena.riot.RDFLanguages;
import org.xenei.jdbc4sparql.J4SConnection;
import org.xenei.jdbc4sparql.J4SDriver;
import org.xenei.jdbc4sparql.J4SDriverTest;
import org.xenei.jdbc4sparql.J4SUrl;
import org.xenei.jdbc4sparql.config.ConfigSerializer;
import org.xenei.jdbc4sparql.config.ModelReader;
import org.xenei.jdbc4sparql.config.ModelWriter;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.sparql.SparqlCatalog;
import org.xenei.jdbc4sparql.sparql.SparqlColumnDef;
import org.xenei.jdbc4sparql.sparql.SparqlSchema;
import org.xenei.jdbc4sparql.sparql.SparqlTable;
import org.xenei.jdbc4sparql.sparql.SparqlTableDef;
import org.xenei.jdbc4sparql.sparql.SparqlColumnDef.Builder;
import org.xenei.jdbc4sparql.sparql.builders.RDFSBuilder;
import org.xenei.jdbc4sparql.sparql.builders.SimpleBuilder;

public class ConfigBuilder
{

	/**
	 * @param args
	 * @throws URISyntaxException 
	 * @throws IOException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	public static void main( String[] args ) throws URISyntaxException, IOException, SQLException, ClassNotFoundException
	{
//		String [] schemaFiles = { 
//				"rdf_ElementsGr2.rdf",
//				"rdf_am-people-rdagr2-schema.ttl",
//				"rdf_am-people-skos-schema.ttl",
//				"rdf_am-schema.ttl",
//				"rdf_am-thesaurus-schema.ttl"
//		};
		String [] schemaFiles = { 
				"foaf.rdf"
		};
		Model ontologyModel = ModelFactory.createDefaultModel();
		// read the schemas
		URL fUrl = null;
		for (String s : schemaFiles )
		{
			fUrl = ConfigBuilder.class.getResource(String.format("./%s", s)); 
			ontologyModel.read( fUrl.toURI().toASCIIString(), RDFLanguages.filenameToLang(fUrl.getPath()).getName());
		}
		RDFSBuilder builder = new RDFSBuilder( ontologyModel );
		
		Model dataModel = ModelFactory.createDefaultModel();;
		
		URL cfgUrl = new URL(fUrl.toExternalForm().replace( "foaf.rdf", "example.ttl"));

		//SimpleBuilder builder = new SimpleBuilder();
		SparqlCatalog catalog = new SparqlCatalog( "http://example.com/", dataModel, "catalog");
		SparqlSchema schema = new SparqlSchema( catalog, "http://example.com/", "schema");
		catalog.addSchema(schema);
		schema.addTableDefs( builder.getTableDefs( catalog ));
		ConfigSerializer cs = new ConfigSerializer();
		cs.add(catalog);
		cs.save( new ModelWriter(new File( cfgUrl.getPath() )));
		//cs.save( new ModelWriter( System.out));
		
		Class.forName("org.xenei.jdbc4sparql.J4SDriver");


		final J4SDriver driver = new J4SDriver();
		J4SUrl url = new J4SUrl("jdbc:J4S:"+ cfgUrl.toExternalForm() );
		J4SConnection connection = new J4SConnection( driver, url, null );
		connection.setCatalog("catalog");
		connection.setSchema("schema");	
		getMetaData( connection );
		
/*		
		fUrl = ConfigBuilder.class.getResource("./rdf_am-data.ttl"); 
		
		final J4SDriver driver = new J4SDriver();
		J4SUrl url = new J4SUrl("jdbc:J4S:"+ cfgUrl.toExternalForm() );
		J4SConnection connection = new J4SConnection( driver, url, null );
		connection.getModelReader().read( fUrl.toString() );
		connection.setCatalog(catalog.getLocalName());
		checkResults( connection );
		*/
	}
	
	private static void checkResults( J4SConnection connection ) throws SQLException
	{
		
		java.sql.Statement stmt = connection.createStatement();
		ResultSet rs = stmt.executeQuery( "Select nameOfThePerson, titleOfThePerson from Person");
		while (rs.next())
		{
			System.out.println( rs.getString(1));
		}
		rs.close();
		stmt.close();
		connection.close();
	
//		J4SUrl url = new J4SUrl("jdbc:J4S?type=ttl:"
//				+ fUrl.toURI().normalize().toASCIIString());
//		
//		final J4SDriver driver = new J4SDriver();
//		final J4SConnection connection = new J4SConnection(driver, url, null);
//		connection.saveConfig( new ModelWriter( System.out ));
//		
//		connection.saveConfig( new ModelWriter( cfgFile ));
		
		

	}

	private static void getMetaData(J4SConnection connection) throws SQLException
	{
		DatabaseMetaData dmd = connection.getMetaData();
		ResultSet rs = dmd.getTables("catalog", "schema", null, null);
		while (rs.next())
		{
			listTable( dmd, rs.getString( "TABLE_CAT"), rs.getString("TABLE_SCHEM"), rs.getString("TABLE_NAME"));
		}
	}
	
	private static void listTable(DatabaseMetaData dmd,String catalog, String schema, String table) throws SQLException {
	
		ResultSet rs = dmd.getColumns(catalog, schema, table, null);
		while (rs.next())
		{
			String s =  String.format( "Cat %s Schema %s tbl %s col %s Nullable: %s", catalog, schema, table, rs.getString("COLUMN_NAME"), rs.getString("IS_NULLABLE"));
			System.out.println( s );
		}
	}
}
