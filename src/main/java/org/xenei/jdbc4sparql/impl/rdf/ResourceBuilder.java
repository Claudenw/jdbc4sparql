package org.xenei.jdbc4sparql.impl.rdf;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.RDF;
import org.xenei.jdbc4sparql.iface.NamespacedObject;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.MissingAnnotation;
import org.xenei.jena.entities.ResourceWrapper;
import org.xenei.jena.entities.annotations.Subject;


public class ResourceBuilder {
	
	public static String getFQName(EntityManager mgr, final Class<?> nsClass) {
		final String s = ResourceBuilder.getNamespace(mgr, nsClass);

		return s.substring(0, s.length() - 1);
	}

	public static String getNamespace(final EntityManager mgr, final Class<?> nsClass) {
		final Subject subject = mgr.getSubject(nsClass);
		if (subject == null) {
			throw new IllegalArgumentException(String.format(
					"%s is does not have a subject annotation", nsClass));
		}
		return subject.namespace();
	}

	private final EntityManager entityManager;

	public ResourceBuilder(final EntityManager entityManager) {
		if (entityManager == null) {
			throw new IllegalArgumentException("EntityManager may not be null");
		}
		this.entityManager = entityManager;
	}

	/**
	 * Get a property.
	 * 
	 * The namespace is retrieved from the class Subject annotation.
	 * 
	 * @param typeClass The class that the property is associated with.
	 * @param localName the local name for the property.
	 * @return the property.
	 */
	public Property getProperty(final Class<?> typeClass, final String localName) {
		return entityManager.createResource( ResourceBuilder.getNamespace(entityManager, typeClass)+localName ).as( Property.class );
	}

	/**
	 * Get the resource from the model or create if if it does not exist.
	 *
	 * @return
	 */
	public Resource getResource(final String fqName, final Class<?> typeClass) {
		final Resource type = entityManager.createResource(ResourceBuilder
				.getFQName(entityManager, typeClass));

		return entityManager.createResource(fqName, type);
	}

	/**
	 * Determine if the resource is in the model.
	 *
	 * @param obj
	 * @return
	 */
	public boolean hasResource(final NamespacedObject obj) {
		return hasResource(obj.getFQName());
	}

	public boolean hasResource(final String fqName) {
		return entityManager.hasResource(fqName);
	}
}
