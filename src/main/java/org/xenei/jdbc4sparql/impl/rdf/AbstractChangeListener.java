package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelChangedListener;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.vocabulary.RDF;

import java.util.List;

import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.MissingAnnotation;

public abstract class AbstractChangeListener<S, T> implements
		ModelChangedListener {

	private final ResourceBuilder rb;
	private final Resource s;
	private final Property p;
	private final EntityManager entityManager = EntityManagerFactory
			.getEntityManager();
	private final Class<? extends T> oClass;

	protected AbstractChangeListener(final Resource resource,
			final Class<? extends S> resourceClass, final String propertyName,
			final Class<? extends T> class1) {
		rb = new ResourceBuilder(resource.getModel());
		s = resource;
		p = rb.getProperty(resourceClass, propertyName);
		this.oClass = class1;
	}

	@Override
	public void addedStatement(final Statement stmt) {
		if (isListening()) {
			final T t = readObject(stmt);
			if (t != null) {
				addObject(t);
			}
		}
	}

	@Override
	public void addedStatements(final List<Statement> statements) {
		if (isListening()) {
			for (final Statement s : statements) {
				addedStatement(s);
			}
		}
	}

	@Override
	public void addedStatements(final Model m) {
		if (isListening()) {

			addedStatements(m.listStatements(s, p, (RDFNode) null));
		}
	}

	@Override
	public void addedStatements(final Statement[] statements) {
		if (isListening()) {
			for (final Statement s : statements) {
				addedStatement(s);
			}
		}
	}

	@Override
	public void addedStatements(final StmtIterator statements) {
		if (isListening()) {
			while (statements.hasNext()) {
				addedStatement(statements.next());
			}
		}
	}

	protected abstract void addObject(T t);

	protected abstract void clearObjects();

	protected abstract boolean isListening();

	@Override
	public void notifyEvent(final Model m, final Object event) {
		// do nothing
	}

	private T readObject(final Statement stmt) {
		T t = null;
		if (stmt.getSubject().equals(s) && stmt.getPredicate().equals(p)) {
			try {
				t = entityManager.read(stmt.getObject(), oClass);
			} catch (final MissingAnnotation e) {
				throw new RuntimeException(e);
			}
		}
		return t;
	}

	@Override
	public void removedStatement(final Statement stmt) {
		if (isListening()) {
			final T t = readObject(stmt);
			if (t != null) {
				removeObject(t);
			}
		}

		if (stmt.getSubject().equals(s) && stmt.getPredicate().equals(RDF.type)) {
			// deleting this
			s.getModel().unregister(this);
			clearObjects();
		}
	}

	@Override
	public void removedStatements(final List<Statement> statements) {
		if (isListening()) {
			for (final Statement s : statements) {
				removedStatement(s);
			}
		}
	}

	@Override
	public void removedStatements(final Model m) {
		if (isListening()) {
			removedStatements(m.listStatements(s, p, (RDFNode) null));
		}
	}

	@Override
	public void removedStatements(final Statement[] statements) {
		if (isListening()) {
			for (final Statement s : statements) {
				removedStatement(s);
			}
		}
	}

	@Override
	public void removedStatements(final StmtIterator statements) {
		if (isListening()) {
			while (statements.hasNext()) {
				removedStatement(statements.next());
			}
		}
	}

	protected abstract void removeObject(T t);

}
