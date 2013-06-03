package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.rdf.model.Resource;

import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jena.entities.EntityManagerRequiredException;
import org.xenei.jena.entities.annotations.Predicate;
import org.xenei.jena.entities.annotations.Subject;

@Subject( namespace = "http://org.xenei.jdbc4sparql/entity/ColumnDef#" )
public class RdfColumnDef implements ColumnDef
{

	@Override
	@Predicate( impl=true )
	public Resource getResource()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate( impl=true )
	public String getColumnClassName()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate( impl=true )
	public int getDisplaySize()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate( impl=true )
	public int getNullable()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate( impl=true )
	public int getPrecision()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate( impl=true )
	public int getScale()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate( impl=true )
	public int getType()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate( impl=true )
	public String getTypeName()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate( impl=true )
	public boolean isAutoIncrement()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate( impl=true )
	public boolean isCaseSensitive()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate( impl=true )
	public boolean isCurrency()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate( impl=true )
	public boolean isDefinitelyWritable()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate( impl=true )
	public boolean isReadOnly()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate( impl=true )
	public boolean isSearchable()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate( impl=true )
	public boolean isSigned()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate( impl=true )
	public boolean isWritable()
	{
		throw new EntityManagerRequiredException();
	}

}
