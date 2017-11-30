/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xenei.jdbc4sparql.sparql.builders;

import java.net.URL;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfSchema;
import org.xenei.jdbc4sparql.impl.rdf.RdfTable;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.impl.EntityManagerImpl;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;

public class RDFSBuilderTest {
	private RdfCatalog catalog;
	private RdfSchema schema;
	// data model
	private Model model;
	private RDFConnection mgr;
	// schema model
	private Model schemaModel;
	private EntityManager sMgr;

	private SchemaBuilder builder;

	private final String[] tableNames = {
			"Project", "Version", "Repository", "Agent", "Document", "Group",
			"Image", "Online_Account", "Person", "Spatial_Thing", "Concept",
			"Thing"
	};

	@Test
	public void buildRdfTableTest() throws SQLException {
		final Set<RdfTable> tables = builder.getTables(schema);
		final List<String> tblNames = Arrays.asList(tableNames);
		for (final RdfTable tbl : tables) {
			Assert.assertTrue(tbl.getName() + " missing from table list",
					tblNames.contains(tbl.getName().getShortName()));
		}
	}

	@Before
	public void setup() {
		model = ModelFactory.createDefaultModel();
		mgr = RDFConnectionFactory.connect( DatasetFactory.create(model) );
		schemaModel = ModelFactory.createDefaultModel();
		sMgr = new EntityManagerImpl( schemaModel );
		URL url = RDFSBuilderTest.class.getResource("./foaf.rdf");
		model.read(url.toExternalForm());
		url = RDFSBuilderTest.class.getResource("./jena.rdf");
		model.read(url.toExternalForm());
		url = RDFSBuilderTest.class.getResource("./doap.rdf");
		model.read(url.toExternalForm());
		model = ModelFactory.createRDFSModel(model);
		catalog = new RdfCatalog.Builder().setLocalConnection(mgr)
				.setName("SimpleSparql").build( sMgr );

		schema = new RdfSchema.Builder().setCatalog(catalog)
				.setName("builderTest").build(sMgr);


		schemaModel.write( System.out, "TURTLE");
		
		builder = new RDFSBuilder();
		

	}

}
