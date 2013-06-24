package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFList;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.shared.Lock;
import com.hp.hpl.jena.sparql.lang.sparql_11.ParseException;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.vocabulary.RDFS;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jdbc4sparql.impl.rdf.RdfTableDef.Builder;
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
		private String remarks = "";
		private RdfTableDef tableDef;
		private String name;
		private RdfSchema schema;
		private RdfColumn.Builder[] columns;
		private String type = "SPARQL TABLE";
		private final Class<? extends RdfTable> typeClass = RdfTable.class;
		private final List<String> querySegments = new ArrayList<String>();

		/**
		 * Add a string format where $1%s is the table name.
		 * @param querySegment
		 * @return
		 */
		public Builder addQuerySegment( final String querySegment )
		{
			querySegments.add(querySegment);
			return this;
		}
		
		public static RdfTable fixupSchema( RdfSchema schema, RdfTable table )
		{
			table.schema = schema;
			Property p = ResourceFactory.createProperty( ResourceBuilder.getNamespace( RdfSchema.class ), "tables" );
			schema.getResource().addProperty( p, table.getResource());
			return table;
		}
		
		private String getQueryStrFmt() {
			final String eol = System.getProperty("line.separator");
			StringBuilder sb = new StringBuilder().append("{").append(eol);
			
			if (! querySegments.isEmpty())
			{
				
				for (final String seg : querySegments)
				{
					sb.append( seg ).append( eol );
				}
			}
			else
			{
				sb.append( "# no query statements provided ").append( eol );
			}
			sb.append( "}");
			return sb.toString();
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

				table.addLiteral(builder.getProperty(typeClass, "type"), type);

				table.addLiteral(builder.getProperty(typeClass, "remarks"), type);
				
				if (! querySegments.isEmpty())
				{
					final Property querySegmentProp = builder.getProperty(
						RdfTable.class, "querySegmentFmt");
					table.addLiteral( querySegmentProp,  getQueryStrFmt() );
					querySegments.clear();
				}
				
				schema.getResource().addProperty(
						builder.getProperty(RdfSchema.class, "tables"), table);
				
				
			}

			try
			{
				final RdfTable retval = entityManager.read(table, typeClass);
				retval.schema = schema;
				// add the column names
				RDFList lst = null;
				for (final RdfColumn.Builder bldr : columns)
				{
					bldr.setTable(retval);
					final RdfColumn col = bldr.build(model);
					if (retval.columns == null)
					{
						retval.columns = new ArrayList<RdfColumn>();
					}
					retval.columns.add(col);

					if (lst == null)
					{
						lst = model.createList().with(col.getResource());
					}
					else
					{
						lst.add(col.getResource());
					}
				}

				final Property p = builder.getProperty(typeClass, "column");
				table.addProperty(p, lst);

				return retval;

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
			if (querySegments.size() == 0)
			{
				querySegments.add("# no query segments provided");
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
		public RdfColumn.Builder getColumn( final int idx )
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
					.append(name).append( " " ).append( getQueryStrFmt() );

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
		public Schema getSchema()
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
		
		public Builder setRemarks( final String remarks )
		{
			this.remarks = remarks;
			return this;
		}
		
		public String getRemarks()
		{
			return remarks;
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
    private RdfSchema schema;
	private SparqlQueryBuilder queryBuilder;

	@Override
	public void delete()
	{
		final Resource tbl = getResource();
		final Model model = tbl.getModel();
		// final ResourceBuilder builder = new ResourceBuilder(model);
		model.enterCriticalSection(Lock.WRITE);
		try
		{
			// preserve the column objects
			final List<RdfColumn> cols = readColumns();

			final Property p = model.createProperty(
					ResourceBuilder.getNamespace(RdfTable.class), "column");
			tbl.getRequiredProperty(p).getResource().as(RDFList.class)
					.removeList();

			// delete the column objects
			for (final RdfColumn col : cols)
			{
				col.delete();
			}
			model.remove(null, null, tbl);
			model.remove(tbl, null, null);
		}
		finally
		{
			model.leaveCriticalSection();
		}
	}

	@Predicate( impl = true )
	public String getQuerySegmentFmt()
	{
		throw new EntityManagerRequiredException();
	}
	
	@Override
	public NameFilter<Column> findColumns( final String columnNamePattern )
	{
		return new NameFilter<Column>(columnNamePattern, readColumns());
	}

	@Override
	public RdfCatalog getCatalog()
	{
		return getSchema().getCatalog();
	}

	@Override
	public RdfColumn getColumn( final int idx )
	{
		return readColumns().get(idx);
	}

	@Override
	public Column getColumn( final String name )
	{
		for (final RdfColumn col : readColumns())
		{
			if (col.getName().equals(name))
			{
				return col;
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
		final List<RdfColumn> cols = readColumns();
		for (int i = 0; i < cols.size(); i++)
		{
			if (cols.get(i).getName().equals(columnName))
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

	@Override
	@Predicate( impl = true )
	public String getRemarks()
	{
		throw new EntityManagerRequiredException();
	}
	
	public Query getQuery() throws SQLException
	{
		if (queryBuilder == null)
		{
			final RdfCatalog catalog = getCatalog();
			queryBuilder = new SparqlQueryBuilder(catalog);
			queryBuilder.addTable(getSchema().getName(), getName());
			final Iterator<RdfColumn> iter = getColumns();
			while (iter.hasNext())
			{
				final RdfColumn col = iter.next();
				queryBuilder.addColumn(col);
				queryBuilder.addVar(col, col.getName());
			}
			RdfKey key = tableDef.getSortKey();
			if (key == null)
			{
				key = tableDef.getPrimaryKey();
			}
			if (key != null)
			{
				queryBuilder.setOrderBy( key );
				if (key.isUnique())
				{
					queryBuilder.setDistinct();
				}
			} else {
				if (tableDef.isDistinct())
				{
					queryBuilder.setDistinct();
				}
			}
		}
		return queryBuilder.build();
	}

	public Element getQuerySegments( final Node tableVar )
	{
		String fmt = getQuerySegmentFmt();
		if (fmt != null)
		{
			try{
			System.out.println( String.format(fmt.toString(), tableVar) ); //FIXME remove this
			return SparqlParser.Util.parse(String.format(fmt, tableVar));
		}
		catch (final ParseException e)
		{
			throw new IllegalStateException(getFQName() + " query segment "
					+ fmt, e);
		}
		}
		else
		{
			return null;
		}
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
	public RdfSchema getSchema()
	{
		return schema;
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
			columns = new ArrayList<RdfColumn>();
			final Resource tbl = this.getResource();
			final Model model = tbl.getModel();

			final Property p = model.createProperty(
					ResourceBuilder.getNamespace(RdfTable.class), "column");

			final List<RDFNode> resLst = tbl.getRequiredProperty(p)
					.getResource().as(RDFList.class).asJavaList();
			for (final RDFNode n : resLst)
			{
				try
				{
					RdfColumn col = entityManager.read(n, RdfColumn.class);
					columns.add(RdfColumn.Builder.fixupTable(this, col)	);
				}
				catch (final MissingAnnotation e)
				{
					throw new RuntimeException(e);
				}
			}
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
