package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.shared.Lock;

import java.util.Set;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jena.entities.EntityManagerRequiredException;
import org.xenei.jena.entities.ResourceWrapper;
import org.xenei.jena.entities.annotations.Predicate;
import org.xenei.jena.entities.annotations.Subject;

@Subject( namespace = "http://org.xenei.jdbc4sparql/entity/Schema#" )
public class RdfSchema<CatType extends RdfCatalog> extends RdfNamespacedObject implements Schema
{
	public class ChangeListener extends AbstractChangeListener<Schema,Table>
	{

		public ChangeListener()
		{
			super(RdfSchema.this.getResource(), RdfSchema.class, "tables",
					RdfTable.class);
		}

		@Override
		protected void addObject( final Table t )
		{
			tableList.add(t);
		}

		@Override
		protected void clearObjects()
		{
			tableList = null;
		}

		@Override
		protected boolean isListening()
		{
			return tableList != null;
		}

		@Override
		protected void removeObject( final Table t )
		{
			tableList.remove(t);
		}

	}

	private Set<Table> tableList;

	@Predicate( impl=true )
	public void addTables( final Table table )
	{
		throw new EntityManagerRequiredException();
	}

	public void delete()
	{
		for (final Table tbl : readTables())
		{
			tbl.delete();
		}

		final Model model = getResource().getModel();
		model.enterCriticalSection(Lock.WRITE);
		try
		{
			getResource().getModel().remove(null, null, getResource());
			getResource().getModel().remove(getResource(), null, null);
		}
		finally
		{
			model.leaveCriticalSection();
		}
	}

	@Override
	public NameFilter<Table> findTables( final String tableNamePattern )
	{
		return new NameFilter<Table>(tableNamePattern, readTables());
	}

	@Override
	@Predicate( impl=true )
	public CatType getCatalog()
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
	@Predicate( impl=true )
	public Resource getResource()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	public Table getTable( final String tableName )
	{
		final NameFilter<Table> nf = findTables(tableName);
		return nf.hasNext() ? nf.next() : null;
	}

	@Override
	@Predicate( impl=true, type=Table.class )
	public Set<Table> getTables()
	{
		throw new EntityManagerRequiredException();
	}

	private Set<Table> readTables()
	{
		if (tableList == null)
		{
			tableList = getTables();
		}
		return tableList;
	}

}
