package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFList;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;

import java.util.ArrayList;
import java.util.List;

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
	private List<ColumnDef> columns;
	private Class<? extends RdfColumnDef> colDefClass;

	public void setColDefClass(Class<? extends RdfColumnDef> colDefClass)
	{
		this.colDefClass = colDefClass;
	}
	
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
					columns.add(entityManager.read(n.asResource(),	colDefClass));
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
	@Predicate( impl=true )
	public Key getPrimaryKey()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate( impl=true )
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
	@Predicate( impl=true )
	public Key getSortKey()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate( impl=true )
	public RdfTableDef getSuperTableDef()
	{
		throw new EntityManagerRequiredException();
	}

}
