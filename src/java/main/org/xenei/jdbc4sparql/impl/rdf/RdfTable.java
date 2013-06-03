package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.shared.Lock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.EntityManagerRequiredException;
import org.xenei.jena.entities.MissingAnnotation;
import org.xenei.jena.entities.ResourceWrapper;
import org.xenei.jena.entities.annotations.Predicate;
import org.xenei.jena.entities.annotations.Subject;

@Subject( namespace = "http://org.xenei.jdbc4sparql/entity/Table#" )
public class RdfTable<CatType extends RdfCatalog, SchemaType extends RdfSchema<CatType>, ColType extends RdfColumn> implements Table
{
	private List<ColType> columns;
	private TableDef tableDef;
	private Class<ColType> colType;

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
								builder.getProperty(RdfColumn.class, "columnDef"),
								colDef).toList().size() == 1)
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
	public NameFilter<Column> findColumns( final String columnNamePattern )
	{
		return new NameFilter<Column>(columnNamePattern, readColumns());
	}

	@Override
	public CatType getCatalog()
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
	public Iterator<ColType> getColumns()
	{
		return readColumns().iterator();
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
	@Predicate( impl=true )
	public SchemaType getSchema()
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
	public Table getSuperTable()
	{
		final TableDef superTableDef = readTableDef().getSuperTableDef();
		if (superTableDef != null)
		{
			final TableBuilder tableBuilder = new TableBuilder().setTableDef(
					superTableDef).setSchema(getSchema());
			return tableBuilder.build(getResource().getModel());
		}
		return null;
	}

	@Override
	@Predicate( impl=true )
	public RdfTableDef getTableDef()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate( impl=true )
	public String getType()
	{
		throw new EntityManagerRequiredException();
	}

	private synchronized List<ColType> readColumns()
	{
		if (columns == null)
		{
			readTableDef(); // force read of table def.
			final EntityManager entityManager = EntityManagerFactory
					.getEntityManager();
			final Property p = entityManager.getSubjectInfo(RdfColumn.class)
					.getPredicateProperty("getTable");
			columns = new ArrayList<ColType>();
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

	private TableDef readTableDef()
	{
		if (tableDef == null)
		{
			tableDef = getTableDef();
		}
		return tableDef;
	}

}
