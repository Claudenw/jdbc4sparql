package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.rdf.model.Resource;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.NamedObject;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jena.entities.EntityManagerRequiredException;
import org.xenei.jena.entities.annotations.Predicate;
import org.xenei.jena.entities.annotations.Subject;

@Subject( namespace = "http://org.xenei.jdbc4sparql/entity/Column#" )
public class RdfColumn extends RdfNamespacedObject implements Column
{
	@Override
	public Catalog getCatalog()
	{
		return getSchema().getCatalog();
	}

	@Override
	@Predicate( impl=true )
	public RdfColumnDef getColumnDef()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate( impl=true, namespace = "http://www.w3.org/2000/01/rdf-schema#", name="label" )
	public String getName()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate( impl= true )
	public Resource getResource()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate( impl=true )
	public Schema getSchema()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	public String getSPARQLName()
	{
		return NameUtils.getSPARQLName(this);
	}

	@Override
	public String getSQLName()
	{
		return NameUtils.getDBName(this);
	}

	@Override
	@Predicate( impl=true )
	public Table getTable()
	{
		throw new EntityManagerRequiredException();
	}

}
