package org.xenei.jdbc4sparql.impl.rdf;

import static org.junit.Assert.*;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Set;



import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;

public class CatalogBuilderTest
{

	private Model model;
	private SchemaBuilder schemaBldr;

	@Before
	public void setUp() throws Exception
	{
		model = ModelFactory.createDefaultModel();
		
		schemaBldr = new SchemaBuilder()
				.setName("testSchema");
	}

	@After
	public void tearDown() throws Exception
	{
		model.close();
	}

	@Test
	public void testDefault()
	{
		Builder builder = new Builder()
		.setName( "catalog");
		
		Catalog catalog = builder.build( model );
		catalog.getSchemas();
		assertEquals( "catalog",  catalog.getName());
		assertNotNull( catalog.getSchemas() );
		assertEquals( 0,  catalog.getSchemas().size() );
	}

	@Test
	public void testAddSchema()
	{
		Builder builder = new Builder()
		.setName( "catalog");
		
		Catalog catalog = builder.build( model );
		
		schemaBldr.setCatalog(catalog);
		
		Schema schema = schemaBldr.build(model);
		
		catalog.getSchemas();
		assertEquals( "catalog",  catalog.getName());
		assertNotNull( catalog.getSchemas() );
		assertEquals( 1,  catalog.getSchemas().size() );
	}
	
	@Test
	public void testAddTableTableRead()
	{
		Builder builder = new Builder()
		.setName( "catalog");
		
		Catalog catalog = builder.build( model );
		assertEquals( 0,  catalog.getSchemas().size() );
		
		schemaBldr.setCatalog(catalog);
		
		Schema schema = schemaBldr.build(model);
			
		assertEquals( "catalog",  catalog.getName());
		assertNotNull( catalog.getSchemas() );
		assertEquals( 1,  catalog.getSchemas().size() );
	}
	
	@Test
	public void testFindSchema()
	{
		Builder builder = new Builder()
		.setName( "catalog");
		
		Catalog catalog = builder.build( model );
		
		schemaBldr.setCatalog(catalog).build(model);
		
		assertNotNull(catalog.findSchemas("testSchema"));
		
		assertNotNull(catalog.findSchemas(null));
	}
	
	@Test
	public void testGetSchema() throws Exception
	{
		Builder builder = new Builder()
		.setName( "catalog");
		
		Catalog catalog = builder.build( model );
		
		schemaBldr.setCatalog(catalog).build(model);
		
		model.write( System.out, "TURTLE" );
		model.write( new FileOutputStream( new File( "dump.ttl")), "TURTLE" );
		
		assertNotNull(catalog.getSchema("testSchema"));
		
	}
	
	@Test
	public void testGetSchemas()
	{
		Builder builder = new Builder()
		.setName( "catalog");
		
		Catalog catalog = builder.build( model );
		
		schemaBldr.setCatalog(catalog);
		
		Schema schema = schemaBldr.build(model);
		
		Set<Schema> schemas = catalog.getSchemas();
		assertNotNull( schemas );
		assertEquals( 1, schemas.size() );

	}
}
