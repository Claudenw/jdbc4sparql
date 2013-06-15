package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.shared.Lock;
import com.hp.hpl.jena.vocabulary.RDFS;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.EntityManagerRequiredException;
import org.xenei.jena.entities.MissingAnnotation;
import org.xenei.jena.entities.annotations.Predicate;
import org.xenei.jena.entities.annotations.Subject;

@Subject( namespace = "http://org.xenei.jdbc4sparql/entity/Column#" )
public class RdfColumn extends RdfNamespacedObject implements Column
{
	public static class Builder implements Column
	{
		private ColumnDef columnDef;
		private Table table;
		private String name;
		private Class<? extends RdfColumn> typeClass = RdfColumn.class;


		public RdfColumn build( final Model model )
		{
			checkBuildState();

			final ResourceBuilder builder = new ResourceBuilder(model);

			Resource column = null;
			if (builder.hasResource(getFQName()))
			{
				column = builder.getResource(getFQName(), typeClass);
			}
			else
			{
				column = builder.getResource(getFQName(), typeClass);

				column.addLiteral(RDFS.label, name);
				column.addProperty(builder.getProperty(typeClass, "columnDef"),
						columnDef.getResource());

				column.addProperty(builder.getProperty(typeClass, "table"),
						table.getResource());

			}

			final EntityManager entityManager = EntityManagerFactory
					.getEntityManager();
			try
			{
				return entityManager.read(column, typeClass);
			}
			catch (final MissingAnnotation e)
			{
				throw new RuntimeException(e);
			}
		}

		protected void checkBuildState()
		{
			if (columnDef == null)
			{
				throw new IllegalStateException("columnDef must be set");
			}

			if (table == null)
			{
				throw new IllegalStateException("table must be set");
			}

			if (StringUtils.isBlank(getName()))
			{
				throw new IllegalStateException("Name must be set");
			}
		}

		@Override
		public Catalog getCatalog()
		{
			return table.getSchema().getCatalog();
		}

		@Override
		public ColumnDef getColumnDef()
		{
			return columnDef;
		}

		public String getFQName()
		{
			final StringBuilder sb = new StringBuilder()
					.append(getColumnDef().getResource().getURI()).append(" ")
					.append(name);
			return String.format("%s/instance/UUID-%s",
					ResourceBuilder.getFQName(RdfColumn.class),
					UUID.nameUUIDFromBytes(sb.toString().getBytes()));
		}

		@Override
		public String getName()
		{
			return name;
		}

		@Override
		public Resource getResource()
		{
			return ResourceFactory.createResource(getFQName());
		}

		@Override
		public Schema getSchema()
		{
			return table.getSchema();
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
		public Table getTable()
		{
			return table;
		}

		public Builder setColumnDef( final ColumnDef columnDef )
		{
			this.columnDef = columnDef;
			return this;
		}

		public Builder setName( final String name )
		{
			this.name = name;
			return this;
		}

		public Builder setTable( final Table table )
		{
			this.table = table;
			return this;
		}
	}

	@Override
	public RdfCatalog getCatalog()
	{
		return getSchema().getCatalog();
	}

	@Override
	@Predicate( impl = true )
	public RdfColumnDef getColumnDef()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate( impl = true, namespace = "http://www.w3.org/2000/01/rdf-schema#", name = "label" )
	public String getName()
	{
		throw new EntityManagerRequiredException();
	}

	public List<Triple> getQuerySegments( final Node tableVar,
			final Node columnVar )
	{
		final List<Triple> retval = new ArrayList<Triple>();
		final String fqName = "<" + getFQName() + ">";
		for (final String segment : getColumnDef().getQuerySegmentStrings())
		{
			if (!segment.trim().startsWith("#"))
			{
				final List<String> parts = SparqlParser.Util
						.parseQuerySegment(String.format(segment, tableVar,
								columnVar, fqName));
				if (parts.size() != 3)
				{
					throw new IllegalStateException(getFQName()
							+ " query segment " + segment
							+ " does not parse into 3 components");
				}
				retval.add(new Triple(
						SparqlParser.Util.parseNode(parts.get(0)),
						SparqlParser.Util.parseNode(parts.get(1)),
						SparqlParser.Util.parseNode(parts.get(2))));
			}
		}
		return retval;
	}

	@Override
	@Predicate( impl = true )
	public Resource getResource()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate( impl = true )
	public RdfSchema getSchema()
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
	@Predicate( impl = true )
	public RdfTable getTable()
	{
		throw new EntityManagerRequiredException();
	}

	public boolean isOptional()
	{
		return getColumnDef().getNullable() != DatabaseMetaData.columnNoNulls;
	}
	
	public void delete()
	{
		final Model model = getResource().getModel();
		final Resource r = getResource();
		model.enterCriticalSection(Lock.WRITE);
		try
		{
			model.remove(null, null, r);
			model.remove(r, null, null);
		}
		finally
		{
			model.leaveCriticalSection();
		}
	}

}
