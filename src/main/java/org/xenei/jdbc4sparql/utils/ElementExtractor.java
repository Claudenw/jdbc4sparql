package org.xenei.jdbc4sparql.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.syntax.ElementAssign;
import org.apache.jena.sparql.syntax.ElementBind;
import org.apache.jena.sparql.syntax.ElementData;
import org.apache.jena.sparql.syntax.ElementDataset;
import org.apache.jena.sparql.syntax.ElementExists;
import org.apache.jena.sparql.syntax.ElementFilter;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementMinus;
import org.apache.jena.sparql.syntax.ElementNamedGraph;
import org.apache.jena.sparql.syntax.ElementNotExists;
import org.apache.jena.sparql.syntax.ElementOptional;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.syntax.ElementService;
import org.apache.jena.sparql.syntax.ElementSubQuery;
import org.apache.jena.sparql.syntax.ElementTriplesBlock;
import org.apache.jena.sparql.syntax.ElementUnion;
import org.apache.jena.sparql.syntax.ElementVisitor;

/**
 * Class for test classes to extract element types from query.
 */
public class ElementExtractor implements ElementVisitor {
	private List<Element> extracted = new ArrayList<Element>();
	private Class<? extends Element> matchType;

	/**
	 * Set the type to match
	 *
	 * @param clazz
	 *            The class type to match
	 * @return this ElementExtractor for chaining
	 */
	public ElementExtractor setMatchType(final Class<? extends Element> clazz) {
		matchType = clazz;
		return this;
	}

	/**
	 * Reset the results.
	 *
	 * @return this ElementExtractor for chaining
	 */
	public ElementExtractor reset() {
		extracted = new ArrayList<Element>();
		return this;
	}

	public List<Element> getExtracted() {
		return extracted;
	}

	public ElementExtractor() {
		;
	}

	public ElementExtractor(final Class<? extends Element> clazz) {
		setMatchType(clazz);
	}

	@Override
	public void visit(final ElementTriplesBlock el) {
		if (matchType.isAssignableFrom(el.getClass())) {
			extracted.add(el);
		}
	}

	@Override
	public void visit(final ElementPathBlock el) {
		if (matchType.isAssignableFrom(el.getClass())) {
			extracted.add(el);
		}
	}

	@Override
	public void visit(final ElementFilter el) {
		if (matchType.isAssignableFrom(el.getClass())) {
			extracted.add(el);
		}
	}

	@Override
	public void visit(final ElementAssign el) {
		if (matchType.isAssignableFrom(el.getClass())) {
			extracted.add(el);
		}
	}

	@Override
	public void visit(final ElementBind el) {
		if (matchType.isAssignableFrom(el.getClass())) {
			extracted.add(el);
		}
	}

	@Override
	public void visit(final ElementData el) {
		if (matchType.isAssignableFrom(el.getClass())) {
			extracted.add(el);
		}
	}

	@Override
	public void visit(final ElementUnion el) {
		if (matchType.isAssignableFrom(el.getClass())) {
			extracted.add(el);
		}
		for (final Element e : el.getElements()) {
			e.visit(this);
		}
	}

	@Override
	public void visit(final ElementOptional el) {
		if (matchType.isAssignableFrom(el.getClass())) {
			extracted.add(el);
		}
		el.getOptionalElement().visit(this);
	}

	@Override
	public void visit(final ElementGroup el) {
		if (matchType.isAssignableFrom(el.getClass())) {
			extracted.add(el);
		}
		for (final Element e : el.getElements()) {
			e.visit(this);
		}
	}

	@Override
	public void visit(final ElementDataset el) {
		if (matchType.isAssignableFrom(el.getClass())) {
			extracted.add(el);
		}
		el.getElement().visit(this);
	}

	@Override
	public void visit(final ElementNamedGraph el) {
		if (matchType.isAssignableFrom(el.getClass())) {
			extracted.add(el);
		}
		el.getElement().visit(this);
	}

	@Override
	public void visit(final ElementExists el) {
		if (matchType.isAssignableFrom(el.getClass())) {
			extracted.add(el);
		}
		el.getElement().visit(this);
	}

	@Override
	public void visit(final ElementNotExists el) {
		if (matchType.isAssignableFrom(el.getClass())) {
			extracted.add(el);
		}
		el.getElement().visit(this);
	}

	@Override
	public void visit(final ElementMinus el) {
		if (matchType.isAssignableFrom(el.getClass())) {
			extracted.add(el);
		}
		el.getMinusElement().visit(this);
	}

	@Override
	public void visit(final ElementService el) {
		if (matchType.isAssignableFrom(el.getClass())) {
			extracted.add(el);
		}
		el.getElement().visit(this);
	}

	@Override
	public void visit(final ElementSubQuery el) {
		if (matchType.isAssignableFrom(el.getClass())) {
			extracted.add(el);
		}
		el.getQuery().getQueryPattern().visit(this);
	}

}
