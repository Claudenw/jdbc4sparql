package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFList;
import com.hp.hpl.jena.rdf.model.Resource;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.xenei.jdbc4sparql.iface.Key;
import org.xenei.jdbc4sparql.iface.KeySegment;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.MissingAnnotation;
import org.xenei.jena.entities.annotations.Predicate;

public class KeyBuilder extends Key
{
	private final List<KeySegment> segments = new ArrayList<KeySegment>();
	private boolean unique;
	private String keyName;

	public KeyBuilder addSegment( final KeySegment segment )
	{
		for (final KeySegment seg : segments)
		{
			if (seg.getIdx() == segment.getIdx())
			{
				throw new IllegalArgumentException(
						"Same segment may not be added more than once");
			}
		}
		segments.add(segment);
		return this;
	}

	public Key build( final Model model )
	{
		checkBuildState();
		final Class<?> typeClass = Key.class;
		final String fqName = String.format("%s/instance/key-%s",
				ResourceBuilder.getFQName(typeClass), getId());
		final ResourceBuilder builder = new ResourceBuilder(model);
		Resource key = null;
		if (builder.hasResource(fqName))
		{
			key = builder.getResource(fqName, typeClass);
		}
		else
		{
			key = builder.getResource(fqName, typeClass);

			key.addLiteral(builder.getProperty(typeClass, "keyName"),
					StringUtils.defaultString(keyName, getId()));

			key.addLiteral(builder.getProperty(typeClass, "unique"), unique);

			RDFList lst = null;

			for (final KeySegment seg : segments)
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
					.getFQName(KeySegment.class));
			key.addProperty(p, lst);

		}

		final EntityManager entityManager = EntityManagerFactory
				.getEntityManager();
		try
		{
			return entityManager.read(key, Key.class);
		}
		catch (final MissingAnnotation e)
		{
			throw new RuntimeException(e);
		}
	}

	private void checkBuildState()
	{
		if (segments.size() == 0)
		{
			throw new IllegalStateException(
					"there must be at least one key segment");
		}
	}

	@Override
	public String getKeyName()
	{
		return keyName;
	}

	@Override
	@Predicate
	public Resource getResource()
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public List<KeySegment> getSegments()
	{
		return segments;
	}

	@Override
	public boolean isUnique()
	{
		return unique;
	}

	public KeyBuilder setKeyName( final String keyName )
	{
		this.keyName = keyName;
		return this;

	}

	public KeyBuilder setUnique( final boolean unique )
	{
		this.unique = unique;
		return this;
	}
}
