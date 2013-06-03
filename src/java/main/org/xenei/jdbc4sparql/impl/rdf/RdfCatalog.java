package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.rdf.model.Resource;

import java.util.Set;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jena.entities.EntityManagerRequiredException;
import org.xenei.jena.entities.annotations.Predicate;
import org.xenei.jena.entities.annotations.Subject;

@Subject( namespace = "http://org.xenei.jdbc4sparql/entity/Catalog#" )
public class RdfCatalog implements Catalog
{
	public class ChangeListener extends AbstractChangeListener<Catalog, Schema>
	{

		public ChangeListener()
		{
			super(RdfCatalog.this.getResource(), RdfCatalog.class, "schemas",
					RdfSchema.class);
		}

		@Override
		protected void addObject( final Schema t )
		{
			schemaList.add(t);
		}

		@Override
		protected void clearObjects()
		{
			schemaList = null;
		}

		@Override
		protected boolean isListening()
		{
			return schemaList != null;
		}

		@Override
		protected void removeObject( final Schema t )
		{
			schemaList.remove(t);
		}

	}

	private Set<Schema> schemaList;

	@Override
	public void close()
	{
		// TODO Auto-generated method stub
	}

	@Override
	public NameFilter<Schema> findSchemas( final String schemaNamePattern )
	{
		return new NameFilter<Schema>(schemaNamePattern, readSchemas());
	}

	@Override
	@Predicate( impl=true, namespace = "http://www.w3.org/2000/01/rdf-schema#", name="label" )
	public String getName()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate( impl=true )
	public Resource getResource()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	public Schema getSchema( final String schemaName )
	{
		final NameFilter<Schema> nf = findSchemas(schemaName);
		return nf.hasNext() ? nf.next() : null;
	}

	@Override
	@Predicate( impl=true, type = RdfSchema.class )
	public Set<Schema> getSchemas()
	{
		throw new EntityManagerRequiredException();
	}

	private Set<Schema> readSchemas()
	{
		if (schemaList == null)
		{
			schemaList = getSchemas();
		}
		return schemaList;
	}

}
