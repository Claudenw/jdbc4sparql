package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.shared.Lock;
import com.hp.hpl.jena.vocabulary.RDFS;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.EntityManagerRequiredException;
import org.xenei.jena.entities.MissingAnnotation;
import org.xenei.jena.entities.annotations.Predicate;
import org.xenei.jena.entities.annotations.Subject;

@Subject( namespace = "http://org.xenei.jdbc4sparql/entity/Schema#" )
public class RdfSchema extends RdfNamespacedObject implements Schema
{
	public static class Builder implements Schema
	{
		private String name;
		private Catalog catalog;
		private final Set<Table> tables = new HashSet<Table>();

		public RdfSchema build( final Model model )
		{
			checkBuildState();
			final Class<?> typeClass = RdfSchema.class;
			final String fqName = getFQName();
			final ResourceBuilder builder = new ResourceBuilder(model);

			final EntityManager entityManager = EntityManagerFactory
					.getEntityManager();

			Resource schema = null;
			if (builder.hasResource(fqName))
			{
				schema = builder.getResource(fqName, typeClass);
			}
			else
			{
				schema = builder.getResource(fqName, typeClass);
				schema.addLiteral(RDFS.label, name);

				for (final Table tbl : tables)
				{
					schema.addProperty(builder.getProperty(typeClass, "table"),
							tbl.getResource());
				}
			}

			try
			{
				final RdfSchema retval = entityManager.read(schema,
						RdfSchema.class);
				catalog.getResource().addProperty(
						builder.getProperty(RdfCatalog.class, "schemas"),
						schema);
				model.register(retval.new ChangeListener());
				return retval;
			}
			catch (final MissingAnnotation e)
			{
				throw new RuntimeException(e);
			}
		}

		private void checkBuildState()
		{
			if (name == null)
			{
				throw new IllegalStateException("Name must be set");
			}

			if (catalog == null)
			{
				throw new IllegalStateException("catalog must be set");
			}

		}

		@Override
		public NameFilter<Table> findTables( final String tableNamePattern )
		{
			return new NameFilter<Table>(tableNamePattern, tables);
		}

		@Override
		public Catalog getCatalog()
		{
			return catalog;
		}

		private String getFQName()
		{
			final StringBuilder sb = new StringBuilder()
					.append(catalog.getResource().getURI()).append(" ")
					.append(name);

			return String
					.format("%s/instance/N%s", ResourceBuilder
							.getFQName(RdfSchema.class),

					UUID.nameUUIDFromBytes(sb.toString().getBytes()).toString());
		}

		@Override
		public String getName()
		{
			return name;
		}

		@Override
		@Predicate
		public Resource getResource()
		{
			return ResourceFactory.createResource(getFQName());
		}

		@Override
		public Table getTable( final String tableName )
		{
			final NameFilter<Table> nf = findTables(tableName);
			return nf.hasNext() ? nf.next() : null;
		}

		@Override
		public Set<Table> getTables()
		{
			return tables;
		}

		public Builder setCatalog( final Catalog catalog )
		{
			this.catalog = catalog;
			return this;
		}

		public Builder setName( final String name )
		{
			this.name = name;
			return this;
		}

	}

	public class ChangeListener extends
			AbstractChangeListener<Schema, RdfTable>
	{

		public ChangeListener()
		{
			super(RdfSchema.this.getResource(), RdfSchema.class, "tables",
					RdfTable.class);
		}

		@Override
		protected void addObject( final RdfTable t )
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
		protected void removeObject( final RdfTable t )
		{
			tableList.remove(t);
		}

	}

	private Set<RdfTable> tableList;

	@Predicate( impl = true )
	public void addTables( final RdfTable table )
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
	public NameFilter<RdfTable> findTables( final String tableNamePattern )
	{
		return new NameFilter<RdfTable>(tableNamePattern, readTables());
	}

	@Override
	@Predicate( impl = true )
	public RdfCatalog getCatalog()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate( impl = true, namespace = "http://www.w3.org/2000/01/rdf-schema#", name = "label" )
	public String getName()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate( impl = true )
	public Resource getResource()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	public RdfTable getTable( final String tableName )
	{
		final NameFilter<RdfTable> nf = findTables(tableName);
		return nf.hasNext() ? nf.next() : null;
	}

	@Override
	@Predicate( impl = true, type = RdfTable.class )
	public Set<RdfTable> getTables()
	{
		throw new EntityManagerRequiredException();
	}

	private Set<RdfTable> readTables()
	{
		if (tableList == null)
		{
			tableList = getTables();
		}
		return tableList;
	}

}
