package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.rdf.model.Resource;

import java.sql.Types;

import javax.persistence.Column;

import org.xenei.jdbc4sparql.impl.NamespaceImpl;
import org.xenei.jdbc4sparql.meta.MetaColumn;

public class SparqlColumn extends MetaColumn
{

	private NamespaceImpl sparqlNamespace;
	private SparqlTableDef tableDef;
	private Resource resource;
	
	public SparqlColumn( SparqlTableDef tableDef, Resource resource  )
	{
		super(resource.getLocalName(), Types.VARCHAR);
		this.resource = resource;
		this.tableDef = tableDef;
	}

}
