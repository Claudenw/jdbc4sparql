package org.xenei.jdbc4sparql.sparql.visitor;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementAssign;
import com.hp.hpl.jena.sparql.syntax.ElementBind;
import com.hp.hpl.jena.sparql.syntax.ElementData;
import com.hp.hpl.jena.sparql.syntax.ElementDataset;
import com.hp.hpl.jena.sparql.syntax.ElementExists;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementMinus;
import com.hp.hpl.jena.sparql.syntax.ElementNamedGraph;
import com.hp.hpl.jena.sparql.syntax.ElementNotExists;
import com.hp.hpl.jena.sparql.syntax.ElementOptional;
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock;
import com.hp.hpl.jena.sparql.syntax.ElementService;
import com.hp.hpl.jena.sparql.syntax.ElementSubQuery;
import com.hp.hpl.jena.sparql.syntax.ElementTriplesBlock;
import com.hp.hpl.jena.sparql.syntax.ElementUnion;
import com.hp.hpl.jena.sparql.syntax.ElementVisitor;

/**
 * Class for test classes to extract element types from query.
 */
public class ElementExtractor implements ElementVisitor {
	private List<Element> extracted = new ArrayList<Element>();
	private Class<? extends Element> matchType;
	
	/**
	 * Set the type to match
	 * @param clazz The class type to match
	 * @return this ElementExtractor for chaining
	 */
	public ElementExtractor setMatchType(Class<? extends Element> clazz)
	{
		matchType = clazz;
		return this;
	}
	
	/**
	 * Reset the results.
	 * @return this ElementExtractor for chaining
	 */
	public ElementExtractor reset()
	{
		extracted.clear();
		return this;
	}
	
	public List<Element> getExtracted()
	{
		return extracted;
	}

	public ElementExtractor(Class<? extends Element> clazz) {
		setMatchType(clazz);
	}

	@Override
	public void visit(ElementTriplesBlock el) {
		if (matchType.isAssignableFrom(el.getClass()))
		{
			extracted.add(el);
		}
	}

	@Override
	public void visit(ElementPathBlock el) {
		if (matchType.isAssignableFrom(el.getClass()))
		{
			extracted.add(el);
		}
	}

	@Override
	public void visit(ElementFilter el) {
		if (matchType.isAssignableFrom(el.getClass()))
		{
			extracted.add(el);
		}
	}

	@Override
	public void visit(ElementAssign el) {
		if (matchType.isAssignableFrom(el.getClass()))
		{
			extracted.add(el);
		}
	}

	@Override
	public void visit(ElementBind el) {
		if (matchType.isAssignableFrom(el.getClass()))
		{
			extracted.add(el);
		}
	}

	@Override
	public void visit(ElementData el) {
		if (matchType.isAssignableFrom(el.getClass()))
		{
			extracted.add(el);
		}
	}

	@Override
	public void visit(ElementUnion el) {
		if (matchType.isAssignableFrom(el.getClass()))
		{
			extracted.add(el);
		}
		for (Element e : el.getElements())
		{
			e.visit(this);
		}
	}

	@Override
	public void visit(ElementOptional el) {
		if (matchType.isAssignableFrom(el.getClass()))
		{
			extracted.add(el);
		}
		el.getOptionalElement().visit( this );
	}

	@Override
	public void visit(ElementGroup el) {
		if (matchType.isAssignableFrom(el.getClass()))
		{
			extracted.add(el);
		}
		for (Element e : el.getElements())
		{
			e.visit(this);
		}
	}

	@Override
	public void visit(ElementDataset el) {
		if (matchType.isAssignableFrom(el.getClass()))
		{
			extracted.add(el);
		}
		el.getPatternElement().visit( this );
	}

	@Override
	public void visit(ElementNamedGraph el) {
		if (matchType.isAssignableFrom(el.getClass()))
		{
			extracted.add(el);
		}
		el.getElement().visit( this );
	}

	@Override
	public void visit(ElementExists el) {
		if (matchType.isAssignableFrom(el.getClass()))
		{
			extracted.add(el);
		}
		el.getElement().visit(this);
	}

	@Override
	public void visit(ElementNotExists el) {
		if (matchType.isAssignableFrom(el.getClass()))
		{
			extracted.add(el);
		}
		el.getElement().visit(this);
	}

	@Override
	public void visit(ElementMinus el) {
		if (matchType.isAssignableFrom(el.getClass()))
		{
			extracted.add(el);
		}
		el.getMinusElement().visit(this);
	}

	@Override
	public void visit(ElementService el) {
		if (matchType.isAssignableFrom(el.getClass()))
		{
			extracted.add(el);
		}
		el.getElement().visit(this);
	}

	@Override
	public void visit(ElementSubQuery el) {
		if (matchType.isAssignableFrom(el.getClass()))
		{
			extracted.add(el);
		}
		el.getQuery().getQueryPattern().visit(this);
	}

}
