package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFList;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.Key;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.EntityManagerRequiredException;
import org.xenei.jena.entities.MissingAnnotation;
import org.xenei.jena.entities.annotations.Predicate;
import org.xenei.jena.entities.annotations.Subject;

@Subject( namespace = "http://org.xenei.jdbc4sparql/entity/TableDef#" )
public class RdfTableDef extends RdfNamespacedObject implements TableDef
{
	public static class Builder implements TableDef
	{
		private final List<ColumnDef> columnDefs = new ArrayList<ColumnDef>();
		private RdfKey primaryKey;
		private RdfKey sortKey;
		private RdfTableDef superTable;
		private final Class<? extends RdfTableDef> typeClass = RdfTableDef.class;
		private final Class<? extends RdfColumnDef> colDefClass = RdfColumnDef.class;
		private final List<String> querySegments = new ArrayList<String>();

		public Builder addColumnDef( final ColumnDef column )
		{
			columnDefs.add(column);
			return this;
		}

		public Builder addQuerySegment( final String querySegment )
		{
			querySegments.add(querySegment);
			return this;
		}

		public RdfTableDef build( final Model model )
		{
			checkBuildState();

			final String fqName = getFQName();
			final ResourceBuilder builder = new ResourceBuilder(model);

			final EntityManager entityManager = EntityManagerFactory
					.getEntityManager();

			Resource tableDef = null;
			if (builder.hasResource(fqName))
			{
				tableDef = builder.getResource(fqName, typeClass);
			}
			else
			{

				tableDef = builder.getResource(fqName, typeClass);

				if (primaryKey != null)
				{

					tableDef.addProperty(
							builder.getProperty(typeClass, "primaryKey"),
							primaryKey.getResource());
				}

				if (sortKey != null)
				{
					tableDef.addProperty(
							builder.getProperty(typeClass, "sortKey"),
							sortKey.getResource());

				}

				if (superTable != null)
				{
					tableDef.addProperty(
							builder.getProperty(typeClass, "superTableDef"),
							superTable.getResource());

				}

				RDFList lst = null;

				for (final ColumnDef seg : columnDefs)
				{
					final Resource s = seg.getResource();
					if (lst == null)
					{
						lst = model.createList().with(s);
					}
					else
					{
						lst.add(s);
					}
				}
				final Property p = model.createProperty(ResourceBuilder
						.getFQName(colDefClass));
				tableDef.addProperty(p, lst);

				lst = null;
				for (final String seg : querySegments)
				{
					final Literal l = model.createTypedLiteral(seg);
					if (lst == null)
					{
						lst = model.createList().with(l);
					}
					else
					{
						lst.add(l);
					}
				}

				final Property querySegmentLst = builder.getProperty(
						RdfTableDef.class, "querySegments");
				tableDef.addProperty(querySegmentLst, lst);
				querySegments.clear();
			}
			try
			{
				final RdfTableDef rdfTableDef = entityManager.read(tableDef,
						typeClass);
				rdfTableDef.setColDefClass(colDefClass);
				return rdfTableDef;
			}
			catch (final MissingAnnotation e)
			{
				throw new RuntimeException(e);
			}
		}

		protected void checkBuildState()
		{
			if (columnDefs.size() == 0)
			{
				throw new IllegalStateException(
						"There must be at least one column defined");
			}
			if (querySegments.size() == 0)
			{
				querySegments.add("# no query segments provided");
			}
		}

		@Override
		public int getColumnCount()
		{
			return columnDefs.size();
		}

		@Override
		public ColumnDef getColumnDef( final int idx )
		{
			return columnDefs.get(idx);
		}

		@Override
		public List<ColumnDef> getColumnDefs()
		{
			return columnDefs;
		}

		@Override
		public int getColumnIndex( final ColumnDef column )
		{
			return columnDefs.indexOf(column);
		}

		public String getFQName()
		{
			final StringBuilder sb = new StringBuilder();
			for (final ColumnDef cd : columnDefs)
			{
				sb.append(cd.getResource().getURI()).append(" ");
			}
			if (primaryKey != null)
			{
				sb.append(primaryKey.getId()).append(" ");
			}
			if (sortKey != null)
			{
				sb.append(sortKey.getId()).append(" ");
			}
			if (superTable != null)
			{
				sb.append(superTable.getResource().getURI());
			}

			return String.format("%s/instance/UUID-%s",
					ResourceBuilder.getFQName(RdfTableDef.class),
					UUID.nameUUIDFromBytes(sb.toString().getBytes()));

		}

		@Override
		public RdfKey getPrimaryKey()
		{
			return primaryKey;
		}

		@Override
		public Resource getResource()
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public Key getSortKey()
		{
			return sortKey;
		}

		@Override
		public TableDef getSuperTableDef()
		{
			return superTable;
		}

		public Builder setPrimaryKey( final RdfKey key )
		{
			if (!key.isUnique())
			{
				throw new IllegalArgumentException(
						"primary key must be a unique key");
			}
			this.primaryKey = key;
			return this;
		}

		public Builder setSortKey( final RdfKey key )
		{
			this.sortKey = key;
			return this;
		}

		public Builder setSuperTableDef( final RdfTableDef tableDef )
		{
			this.superTable = tableDef;
			return this;
		}

	}

	private List<String> querySegments;

	private List<ColumnDef> columns;

	private Class<? extends RdfColumnDef> colDefClass;

	@Override
	public int getColumnCount()
	{
		return getColumnDefs().size();
	}

	@Override
	public ColumnDef getColumnDef( final int idx )
	{
		return getColumnDefs().get(idx);
	}

	@Override
	public List<ColumnDef> getColumnDefs()
	{

		if (columns == null)
		{
			final EntityManager entityManager = EntityManagerFactory
					.getEntityManager();
			columns = new ArrayList<ColumnDef>();
			final Resource resource = getResource();
			final Property p = resource.getModel().createProperty(
					ResourceBuilder.getFQName(RdfColumnDef.class));
			final List<RDFNode> resLst = resource.getRequiredProperty(p)
					.getResource().as(RDFList.class).asJavaList();
			for (final RDFNode n : resLst)
			{
				try
				{
					columns.add(entityManager.read(n.asResource(), colDefClass));
				}
				catch (final MissingAnnotation e)
				{
					throw new RuntimeException(e);
				}
			}
		}
		return columns;

	}

	@Override
	public int getColumnIndex( final ColumnDef column )
	{
		return getColumnDefs().indexOf(column);
	}

	/**
	 * get the primary key for the table
	 * 
	 * @return
	 */
	@Override
	@Predicate( impl = true )
	public Key getPrimaryKey()
	{
		throw new EntityManagerRequiredException();
	}

	@Predicate( impl = true )
	public RDFNode getQuerySegments()
	{
		throw new EntityManagerRequiredException();
	}

	public List<String> getQuerySegmentStrings()
	{
		if (querySegments == null)
		{
			querySegments = new ArrayList<String>();
			final RDFList lst = getQuerySegments().as(RDFList.class);
			for (final RDFNode node : lst.asJavaList())
			{
				querySegments.add(node.asLiteral().toString());
			}
		}
		return querySegments;
	}

	@Override
	@Predicate( impl = true )
	public Resource getResource()
	{
		throw new EntityManagerRequiredException();
	}

	/**
	 * Get the table sort order key.
	 * returns null if the table is not sorted.
	 * 
	 * @return
	 */
	@Override
	@Predicate( impl = true )
	public Key getSortKey()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate( impl = true )
	public RdfTableDef getSuperTableDef()
	{
		throw new EntityManagerRequiredException();
	}

	public void setColDefClass( final Class<? extends RdfColumnDef> colDefClass )
	{
		this.colDefClass = colDefClass;
	}

}
