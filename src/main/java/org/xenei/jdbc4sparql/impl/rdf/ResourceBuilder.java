package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.RDF;

import org.xenei.jdbc4sparql.iface.NamespacedObject;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.annotations.Subject;

public class ResourceBuilder
{
	public static String getFQName( final Class<?> nsClass )
	{
		final String s = ResourceBuilder.getNamespace(nsClass);

		return s.substring(0, s.length() - 1);
	}

	public static String getNamespace( final Class<?> nsClass )
	{
		final EntityManager em = EntityManagerFactory.getEntityManager();
		final Subject subject = em.getSubject(nsClass);
		if (subject == null)
		{
			throw new IllegalArgumentException(String.format(
					"%s is does not have a subject annotation", nsClass));
		}
		return subject.namespace();
	}

	private final Model model;

	public ResourceBuilder( final Model model )
	{
		if (model == null)
		{
			throw new IllegalArgumentException("Model may not be null");
		}
		this.model = model;
	}

	public Property getProperty( final Class<?> typeClass,
			final String localName )
	{
		return model.createProperty(ResourceBuilder.getNamespace(typeClass),
				localName);
	}

	/**
	 * Get the resource from the model or create if if it does not exist.
	 * 
	 * @return
	 */
	public Resource getResource( final String fqName, final Class<?> typeClass )
	{
		final Resource type = model.createResource(ResourceBuilder
				.getFQName(typeClass));

		Resource retval;
		if (hasResource(fqName))
		{
			retval = model.getResource(fqName);
			if (!retval.hasProperty(RDF.type, type))
			{
				throw new IllegalStateException(String.format(
						"Object %s is of type %s not %s", retval.getURI(),
						retval.getRequiredProperty(RDF.type).getResource()
								.getURI(), type.getURI()));
			}
		}
		else
		{
			retval = model.createResource(fqName, type);
		}
		return retval;
	}

	/**
	 * Determine if the resource is in the model.
	 * 
	 * @param obj
	 * @return
	 */
	public boolean hasResource( final NamespacedObject obj )
	{
		return hasResource(obj.getFQName());
	}

	public boolean hasResource( final String fqName )
	{
		return model.contains(model.createResource(fqName), null);
	}
}
