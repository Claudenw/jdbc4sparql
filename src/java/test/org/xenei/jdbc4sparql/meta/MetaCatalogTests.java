package org.xenei.jdbc4sparql.meta;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import org.junit.Test;

public class MetaCatalogTests
{
	Model model = ModelFactory.createDefaultModel();

	@Test
	public void testConstructor()
	{
		MetaCatalogBuilder.getInstance(model);
		model.write(System.out, "TURTLE");
	}

}
