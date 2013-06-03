package org.xenei.jdbc4sparql.meta;

import static org.junit.Assert.*;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.Catalog;

public class MetaCatalogTests
{
	Model model = ModelFactory.createDefaultModel();
	

	@Test
	public void testConstructor()
	{
		Catalog cat = MetaCatalogBuilder.getInstance(model);
		model.write( System.out, "TURTLE");
	}

}
