package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

import org.xenei.jdbc4sparql.iface.KeySegment;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.MissingAnnotation;
import org.xenei.jena.entities.annotations.Predicate;

public class KeySegmentBuilder extends KeySegment
{

	private short idx;
	private boolean ascending = true;

	public KeySegment build( final Model model )
	{

		final Class<?> typeClass = KeySegment.class;
		final String fqName = String.format("%s/instance/%s",
				ResourceBuilder.getFQName(typeClass), getId());
		final ResourceBuilder builder = new ResourceBuilder(model);
		Resource keySegment = null;
		if (builder.hasResource(fqName))
		{
			keySegment = builder.getResource(fqName, typeClass);
		}
		else
		{
			keySegment = builder.getResource(fqName, typeClass);

			keySegment.addLiteral(builder.getProperty(typeClass, "idx"), idx);
			keySegment.addLiteral(builder.getProperty(typeClass, "ascending"),
					ascending);

		}

		final EntityManager entityManager = EntityManagerFactory
				.getEntityManager();
		try
		{
			return entityManager.read(keySegment, KeySegment.class);
		}
		catch (final MissingAnnotation e)
		{
			throw new RuntimeException(e);
		}
	}

	@Override
	public short getIdx()
	{
		return idx;
	}

	@Override
	@Predicate
	public Resource getResource()
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isAscending()
	{
		return ascending;
	}

	public KeySegmentBuilder setAscending( final boolean ascending )
	{
		this.ascending = ascending;
		return this;
	}

	public KeySegmentBuilder setIdx( final int idx )
	{
		if ((idx < 0) || (idx > Short.MAX_VALUE))
		{
			throw new IllegalArgumentException(
					"index must be in the range 0 - " + Short.MAX_VALUE);
		}
		this.idx = (short) idx;
		return this;
	}
}
