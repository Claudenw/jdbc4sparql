package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.shared.Lock;
import com.hp.hpl.jena.vocabulary.RDFS;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.SparqlResultSet;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.EntityManagerRequiredException;
import org.xenei.jena.entities.MissingAnnotation;
import org.xenei.jena.entities.annotations.Predicate;
import org.xenei.jena.entities.annotations.Subject;

@Subject( namespace = "http://org.xenei.jdbc4sparql/entity/Table#" )
public class RdfTable extends RdfNamespacedObject implements Table
{
	public static class Builder implements Table
	{
		private RdfTableDef tableDef;
		private String name;
		private RdfSchema schema;
		private RdfColumn.Builder[] columns;
		private String type = "SPARQL TABLE";
		private final Class<? extends RdfTable> typeClass = RdfTable.class;

		public Builder()
		{
		}

		public RdfTable build( final Model model )
		{
			checkBuildState();
			final String fqName = getFQName();
			final ResourceBuilder builder = new ResourceBuilder(model);

			final EntityManager entityManager = EntityManagerFactory
					.getEntityManager();

			Resource table = null;
			if (builder.hasResource(fqName))
			{
				table = builder.getResource(fqName, typeClass);
			}
			else
			{

				table = builder.getResource(fqName, typeClass);

				table.addLiteral(RDFS.label, name);

				table.addProperty(builder.getProperty(typeClass, "tableDef"),
						tableDef.getResource());

				table.addProperty(builder.getProperty(typeClass, "schema"),
						schema.getResource());

				table.addLiteral(builder.getProperty(typeClass, "type"), type);

				for (final RdfColumn.Builder cBldr : columns)
				{
					cBldr.setTable(this);
					final Column col = cBldr.build(model);
					table.addProperty(builder.getProperty(typeClass, "column"),
							col.getResource());
				}

				schema.getResource().addProperty(
						builder.getProperty(RdfSchema.class, "tables"), table);
			}

			try
			{
				return entityManager.read(table, typeClass);
			}
			catch (final MissingAnnotation e)
			{
				throw new RuntimeException(e);
			}
		}

		private void checkBuildState()
		{
			if (StringUtils.isBlank(name))
			{
				throw new IllegalStateException("Name must be set");
			}

			if (StringUtils.isBlank(type))
			{
				throw new IllegalStateException("Type must be set");
			}

			if (schema == null)
			{
				throw new IllegalStateException("schema must be set");
			}

			for (int i = 0; i < columns.length; i++)
			{
				if (columns[i] == null)
				{
					throw new IllegalStateException(String.format(
							"column %s must be set", i));
				}
			}
		}

		@Override
		public void delete()
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public NameFilter<Column> findColumns( final String columnNamePattern )
		{
			return new NameFilter<Column>(columnNamePattern, getColumns());
		}

		@Override
		public Catalog getCatalog()
		{
			return getSchema().getCatalog();
		}

		@Override
		public Column getColumn( final int idx )
		{
			return columns[idx];
		}

		@Override
		public Column getColumn( final String name )
		{
			for (final Column c : columns)
			{
				if (c.getName().equals(name))
				{
					return c;
				}
			}
			return null;
		}

		@Override
		public int getColumnCount()
		{
			return tableDef.getColumnCount();
		}

		@Override
		public int getColumnIndex( final Column column )
		{
			for (int i = 0; i < columns.length; i++)
			{
				if (columns[i].getResource().equals(column.getResource()))
				{
					return i;
				}
			}
			return -1;
		}

		@Override
		public int getColumnIndex( final String name )
		{
			for (int i = 0; i < columns.length; i++)
			{
				if (name.equals(columns[i].getName()))
				{
					return i;
				}
			}
			return -1;
		}

		@Override
		public Iterator<? extends Column> getColumns()
		{
			return Arrays.asList(columns).iterator();
		}

		private String getFQName()
		{
			final StringBuilder sb = new StringBuilder()
					.append(schema.getResource().getURI()).append(" ")
					.append(name);

			return String
					.format("%s/instance/N%s", ResourceBuilder
							.getFQName(RdfTable.class),
							UUID.nameUUIDFromBytes(sb.toString().getBytes())
									.toString());
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
		public RdfSchema getSchema()
		{
			return schema;
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
		public RdfTable getSuperTable()
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public RdfTableDef getTableDef()
		{
			return tableDef;
		}

		@Override
		public String getType()
		{
			return type;
		}

		public Builder setColumn( final int idx, final String name )
		{
			if (tableDef == null)
			{
				throw new IllegalStateException(
						"TableDef must be specified before defining columns");
			}

			if ((idx < 0) || (idx >= columns.length))
			{
				throw new IllegalArgumentException(String.format(
						"index '%s' must be between 0 and %s inclusive", idx,
						columns.length - 1));
			}
			final RdfColumn.Builder builder = new RdfColumn.Builder()
					.setColumnDef(tableDef.getColumnDef(idx)).setName(name)
					.setTable(this);

			columns[idx] = builder;
			return this;
		}

		public Builder setColumns( final List<String> colNames )
		{
			if (colNames.size() != tableDef.getColumnCount())
			{
				throw new IllegalArgumentException(String.format(
						"There must be %s column names, %s provided",
						tableDef.getColumnCount(), colNames.size()));
			}
			for (int i = 0; i < colNames.size(); i++)
			{
				setColumn(i, colNames.get(i));
			}
			return this;
		}

		public Builder setName( final String name )
		{
			this.name = name;
			return this;
		}

		public Builder setSchema( final RdfSchema schema )
		{
			this.schema = schema;
			return this;
		}

		public Builder setTableDef( final RdfTableDef tableDef )
		{
			this.tableDef = tableDef;
			this.columns = new RdfColumn.Builder[tableDef.getColumnCount()];
			return this;
		}

		public Builder setType( final String type )
		{
			this.type = type;
			return this;
		}

	}

	private List<RdfColumn> columns;
	private RdfTableDef tableDef;

	private Class<RdfColumn> colType;

	private SparqlQueryBuilder queryBuilder;

	@Override
	public void delete()
	{
		final Model model = getResource().getModel();
		final ResourceBuilder builder = new ResourceBuilder(model);
		model.enterCriticalSection(Lock.WRITE);
		try
		{

			for (final Column col : readColumns())
			{
				final Resource colDef = col.getColumnDef().getResource();

				if (model
						.listSubjectsWithProperty(
								builder.getProperty(RdfColumn.class,
										"columnDef"), colDef).toList().size() == 1)
				{
					// ((ColumnDef)this).delete();
				}

				final Resource r = model.createResource(col.getResource()
						.getURI());
				model.remove(null, null, r);
				model.remove(r, null, null);
			}
			model.remove(null, null, getResource());
			model.remove(getResource(), null, null);
		}
		finally
		{
			model.leaveCriticalSection();
		}
	}

	@Override
	public NameFilter<RdfColumn> findColumns( final String columnNamePattern )
	{
		return new NameFilter<RdfColumn>(columnNamePattern, readColumns());
	}

	@Override
	public RdfCatalog getCatalog()
	{
		return getSchema().getCatalog();
	}

	@Override
	public Column getColumn( final int idx )
	{
		return readColumns().get(idx);
	}

	@Override
	public Column getColumn( final String name )
	{
		for (final Column c : readColumns())
		{
			if (c.getName().equals(name))
			{
				return c;
			}
		}
		return null;
	}

	@Override
	public int getColumnCount()
	{
		return readColumns().size();
	}

	@Override
	public int getColumnIndex( final Column column )
	{
		return readColumns().indexOf(column);
	}

	/**
	 * Returns the column index for hte name or -1 if not found
	 * 
	 * @param columnName
	 *            The name to search for
	 * @return the column index (0 based) or -1 if not found.
	 */
	@Override
	public int getColumnIndex( final String columnName )
	{
		readColumns();
		for (int i = 0; i < columns.size(); i++)
		{
			if (columns.get(i).getName().equals(columnName))
			{
				return i;
			}
		}
		return -1;
	}

	@Override
	public Iterator<RdfColumn> getColumns()
	{
		return readColumns().iterator();
	}

	@Override
	@Predicate( impl = true, namespace = "http://www.w3.org/2000/01/rdf-schema#", name = "label" )
	public String getName()
	{
		throw new EntityManagerRequiredException();
	}

	public Query getQuery() throws SQLException
	{
		if (queryBuilder == null)
		{
			queryBuilder = new SparqlQueryBuilder(getCatalog());
			queryBuilder.addTable(getSchema().getLocalName(), getLocalName());
			final Iterator<RdfColumn> iter = getColumns();
			while (iter.hasNext())
			{
				final RdfColumn col = iter.next();
				queryBuilder.addColumn(col);
				queryBuilder.addVar(col, col.getLocalName());
			}
		}
		return queryBuilder.build();
	}

	public List<Triple> getQuerySegments( final Node tableVar )
	{
		final List<Triple> retval = new ArrayList<Triple>();
		final String fqName = "<" + getFQName() + ">";
		for (final String segment : getTableDef().getQuerySegmentStrings())
		{
			if (!segment.trim().startsWith("#")) // skip comments
			{
				final List<String> parts = SparqlParser.Util
						.parseQuerySegment(String.format(segment, tableVar,
								fqName));
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

	public SparqlResultSet getResultSet() throws SQLException
	{
		return new SparqlResultSet(this);
	}

	@Override
	@Predicate( impl = true )
	public RdfSchema getSchema()
	{
		throw new EntityManagerRequiredException();
	}

	public String getSolutionName( final int idx )
	{
		return queryBuilder.getSolutionName(idx);
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
	public RdfTable getSuperTable()
	{
		final RdfTableDef superTableDef = readTableDef().getSuperTableDef();
		if (superTableDef != null)
		{
			final Builder tableBuilder = new Builder().setTableDef(
					superTableDef).setSchema(getSchema());
			return tableBuilder.build(getResource().getModel());
		}
		return null;
	}

	@Override
	@Predicate( impl = true )
	public RdfTableDef getTableDef()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate( impl = true )
	public String getType()
	{
		throw new EntityManagerRequiredException();
	}

	private synchronized List<RdfColumn> readColumns()
	{
		if (columns == null)
		{
			readTableDef(); // force read of table def.
			final EntityManager entityManager = EntityManagerFactory
					.getEntityManager();
			final Property p = entityManager.getSubjectInfo(RdfColumn.class)
					.getPredicateProperty("getTable");
			columns = new ArrayList<RdfColumn>();
			final Model model = this.getResource().getModel();

			// read the columns
			for (final Resource r : model.listSubjectsWithProperty(p,
					this.getResource()).toList())
			{
				try
				{
					columns.add(entityManager.read(r, colType));
				}
				catch (final MissingAnnotation e)
				{
					throw new RuntimeException(e);
				}
			}
			// sort the columns
			final Comparator<Column> comp = new Comparator<Column>() {

				@Override
				public int compare( final Column col1, final Column col2 )
				{
					return Integer.compare(
							tableDef.getColumnIndex(col1.getColumnDef()),
							tableDef.getColumnIndex(col2.getColumnDef()));

				}
			};
			Collections.sort(columns, comp);
		}
		return columns;
	}

	private RdfTableDef readTableDef()
	{
		if (tableDef == null)
		{
			tableDef = getTableDef();
		}
		return tableDef;
	}
}
