package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.QueryException;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.shared.Lock;
import com.hp.hpl.jena.sparql.lang.sparql_11.ParseException;
import com.hp.hpl.jena.sparql.syntax.Element;
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
import org.xenei.jena.entities.ResourceWrapper;
import org.xenei.jena.entities.annotations.Predicate;
import org.xenei.jena.entities.annotations.Subject;

@Subject( namespace = "http://org.xenei.jdbc4sparql/entity/Column#" )
public class RdfColumn extends RdfNamespacedObject implements Column,
		ResourceWrapper
{
	public static class Builder implements Column
	{
		public static RdfColumn fixupTable( final RdfTable table,
				final RdfColumn column )
		{
			column.table = table;
			return column;
		}

		private ColumnDef columnDef;
		private Table table;
		private String name;
		private final Class<? extends RdfColumn> typeClass = RdfColumn.class;
		private String remarks = null;

		private final List<String> querySegments = new ArrayList<String>();

		/**
		 * Add a query segment where %1$s = tableVar, and %2$s=columnVar
		 * 
		 * @param querySegment
		 * @return
		 */
		public Builder addQuerySegment( final String querySegment )
		{
			querySegments.add(querySegment);
			return this;
		}

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
				column.addLiteral(builder.getProperty(typeClass, "remarks"),
						StringUtils.defaultString(remarks));
				column.addProperty(builder.getProperty(typeClass, "columnDef"),
						((ResourceWrapper) columnDef).getResource());

			}

			if (!querySegments.isEmpty())
			{
				final String eol = System.getProperty("line.separator");
				final StringBuilder sb = new StringBuilder();
				for (final String seg : querySegments)
				{
					sb.append(seg).append(eol);
				}

				final Property querySegmentProp = builder.getProperty(
						typeClass, "querySegmentFmt");
				column.addLiteral(querySegmentProp, getQueryStrFmt());
				querySegments.clear();
			}

			final EntityManager entityManager = EntityManagerFactory
					.getEntityManager();
			try
			{
				final RdfColumn retval = entityManager.read(column, typeClass);
				RdfTable tbl = null;
				if (table instanceof RdfTable.Builder)
				{
					tbl = ((RdfTable.Builder) table).build(model);
				}
				else if (table instanceof RdfTable)
				{
					tbl = (RdfTable) table;
				}
				else
				{
					throw new IllegalArgumentException(
							"table not an rdf table or builder");
				}
				// table is now a real RdfTable so add it as a property
				column.addProperty(builder.getProperty(typeClass, "table"),
						tbl.getResource());
				return Builder.fixupTable(tbl, retval);
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

			if (!(columnDef instanceof ResourceWrapper))
			{
				throw new IllegalStateException(
						"columnDef must implement ResourceWrapper");
			}

			if (table == null)
			{
				throw new IllegalStateException("table must be set");
			}

			if (StringUtils.isBlank(getName()))
			{
				throw new IllegalStateException("Name must be set");
			}

			if (querySegments.size() == 0)
			{
				querySegments.add("# no query segments provided");
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
			final ResourceWrapper rw = (ResourceWrapper) getColumnDef();
			final StringBuilder sb = new StringBuilder()
					.append(rw.getResource().getURI()).append(" ").append(name)
					.append(" ").append(getQueryStrFmt());
			return String.format("%s/instance/UUID-%s",
					ResourceBuilder.getFQName(RdfColumn.class),
					UUID.nameUUIDFromBytes(sb.toString().getBytes()));
		}

		@Override
		public String getName()
		{
			return name;
		}

		private String getQueryStrFmt()
		{
			final String eol = System.getProperty("line.separator");
			final StringBuilder sb = new StringBuilder();
			if (!querySegments.isEmpty())
			{

				for (final String seg : querySegments)
				{
					sb.append(seg).append(eol);
				}
			}
			else
			{
				sb.append("# no segment provided").append(eol);
			}
			return sb.toString();
		}

		@Override
		public String getRemarks()
		{
			return remarks;
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
			if (columnDef instanceof ResourceWrapper)
			{
				this.columnDef = columnDef;
			}
			return this;
		}

		public Builder setName( final String name )
		{
			this.name = name;
			return this;
		}

		public Builder setRemarks( final String remarks )
		{
			this.remarks = remarks;
			return this;
		}

		public Builder setTable( final Table table )
		{
			this.table = table;
			return this;
		}
	}

	private RdfTable table;

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

	@Predicate( impl = true )
	public String getQuerySegmentFmt()
	{
		throw new EntityManagerRequiredException();
	}

	public Element getQuerySegments( final Node tableVar, final Node columnVar )
	{
		final String fmt = "{" + getQuerySegmentFmt() + "}";

		try
		{
			return SparqlParser.Util.parse(String.format(fmt, tableVar,
					columnVar));
		}
		catch (final ParseException e)
		{
			throw new IllegalStateException(getFQName() + " query segment "
					+ fmt, e);
		}
		catch (final QueryException e)
		{
			throw new IllegalStateException(getFQName() + " query segment "
					+ fmt, e);
		}
	}

	@Override
	@Predicate( impl = true )
	public String getRemarks()
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
	public RdfSchema getSchema()
	{
		return getTable().getSchema();
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
	public RdfTable getTable()
	{
		return table;
	}

	public boolean isOptional()
	{
		return getColumnDef().getNullable() != DatabaseMetaData.columnNoNulls;
	}

}
