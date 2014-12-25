package org.xenei.jdbc4sparql.sparql;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.sparql.core.PathBlock;
import com.hp.hpl.jena.sparql.core.TriplePath;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprVar;
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
 * Class to renumber bNodes to be unique within the query.
 */
public class BnodeRenumber {
	private class ElementHandler implements ElementVisitor {
		BnodeRenumber.EType type;

		public BnodeRenumber.EType getType() {
			return type;
		}

		@Override
		public void visit(final ElementAssign el) {
			type = EType.Assign;
		}

		@Override
		public void visit(final ElementBind el) {
			type = EType.Bind;
		}

		@Override
		public void visit(final ElementData el) {
			type = EType.Data;
		}

		@Override
		public void visit(final ElementDataset el) {
			type = EType.Dataset;
		}

		@Override
		public void visit(final ElementExists el) {
			type = EType.Exists;
		}

		@Override
		public void visit(final ElementFilter el) {
			type = EType.Filter;
		}

		@Override
		public void visit(final ElementGroup el) {
			type = EType.Group;
		}

		@Override
		public void visit(final ElementMinus el) {
			type = EType.Minus;
		}

		@Override
		public void visit(final ElementNamedGraph el) {
			type = EType.NamedGraph;
		}

		@Override
		public void visit(final ElementNotExists el) {
			type = EType.NotExists;
		}

		@Override
		public void visit(final ElementOptional el) {
			type = EType.Optional;
		}

		@Override
		public void visit(final ElementPathBlock el) {
			type = EType.PathBlock;
		}

		@Override
		public void visit(final ElementService el) {
			type = EType.Service;
		}

		@Override
		public void visit(final ElementSubQuery el) {
			type = EType.SubQuery;
		}

		@Override
		public void visit(final ElementTriplesBlock el) {
			type = EType.TriplesBlock;
		}

		@Override
		public void visit(final ElementUnion el) {
			type = EType.Union;
		}
	}

	enum EType {
		Assign, Bind, Data, Dataset, Exists, Filter, Group, Minus, NamedGraph, NotExists, Optional, PathBlock, Service, SubQuery, TriplesBlock, Union
	}

	private int bnodeCount = 0;
	private final ElementHandler handler = new ElementHandler();;
	private Map<String, Node> renumberMap = new HashMap<String, Node>();

	private final Stack<Map<String, Node>> renumberMapStack = new Stack<Map<String, Node>>();

	private Node nextAnon() {
		final AnonId id = new AnonId("?" + bnodeCount);
		bnodeCount++;
		return NodeFactory.createAnon(id);
	}

	private Expr processExpr(final Expr expr) {
		if (expr.isVariable()) {
			final ExprVar ev = expr.getExprVar();
			if (ev.asVar().isBlankNodeVar()) {
				return new ExprVar(nextAnon());
			}
		}
		return expr;
	}

	private Node processNode(Node n) {
		if (n.isBlank()) {
			Node retval = renumberMap.get(n.getBlankNodeLabel());
			if (retval == null) {
				retval = nextAnon();
				renumberMap.put(n.getBlankNodeLabel(), retval);
			}
			return retval;
		}
		if (n.isVariable()) {
			n = processVar((Var) n);
		}
		return n;
	}

	private Var processVar(final Var var) {
		Var v = var;
		if (v.isBlankNodeVar()) {
			final String s = v.getVarName();
			final AnonId id = new AnonId(s);
			Node n = renumberMap.get(id.getLabelString());
			if (n == null) {
				n = nextAnon();

				renumberMap.put(id.getLabelString(), n);
			}
			v = Var.alloc(n.getBlankNodeId().getLabelString());
		}
		return v;
	}

	public Element renumber(final Element e) {
		Element retval = e;
		e.visit(handler);
		switch (handler.getType()) {
			case Assign:
				retval = renumberAssign((ElementAssign) e);
				break;
			case Bind:
				retval = renumberBind((ElementBind) e);
				break;
			case Data:
				retval = renumberData((ElementData) e);
				break;
			case Dataset:
				throw new IllegalArgumentException(
						"Dataset should not be used in parser");

			case Exists:
				retval = new ElementExists(
						renumber(((ElementExists) e).getElement()));
				break;
			case Filter:
				retval = new ElementFilter(
						processExpr(((ElementFilter) e).getExpr()));
				break;
			case Group:
				retval = renumberGroup((ElementGroup) e);
				break;
			case Minus:
				retval = new ElementMinus(
						renumber(((ElementMinus) e).getMinusElement()));
				break;
			case NamedGraph:
				retval = renumberNamedGraph((ElementNamedGraph) e);
				break;
			case NotExists:
				retval = new ElementNotExists(
						renumber(((ElementNotExists) e).getElement()));
				break;
			case Optional:
				retval = new ElementOptional(
						renumber(((ElementOptional) e).getOptionalElement()));
				break;
			case PathBlock:
				retval = renumberPathBlock((ElementPathBlock) e);
				break;
			case Service:
				// default to returning e
				break;
			case SubQuery:
				// default to returning e
				break;
			case TriplesBlock:
				retval = renumberTriplesBlock((ElementTriplesBlock) e);
				break;
			case Union:
				retval = renumberUnion((ElementUnion) e);
				break;
		}
		return retval;

	}

	private ElementAssign renumberAssign(final ElementAssign e) {
		return new ElementAssign(processVar(e.getVar()),
				processExpr(e.getExpr()));
	}

	private ElementBind renumberBind(final ElementBind e) {
		return new ElementBind(processVar(e.getVar()), processExpr(e.getExpr()));
	}

	private ElementData renumberData(final ElementData e) {
		final List<Var> vars = e.getVars();
		boolean foundBlank = false;
		for (int i = 0; i < vars.size(); i++) {
			if (vars.get(i).isBlankNodeVar()) {
				foundBlank = true;
				vars.set(i, processVar(vars.get(i)));
			}
		}
		ElementData retval = e;
		if (foundBlank) {
			retval = new ElementData();
			for (final Var v : vars) {
				retval.add(v);
			}
		}
		return retval;
	}

	private ElementGroup renumberGroup(final ElementGroup e) {
		renumberMapStack.push(renumberMap);
		renumberMap = new HashMap<String, Node>();
		final ElementGroup retval = new ElementGroup();
		for (final Element el : e.getElements()) {
			retval.addElement(renumber(el));
		}
		renumberMap = renumberMapStack.pop();
		return retval;
	}

	private ElementNamedGraph renumberNamedGraph(final ElementNamedGraph e) {
		final Node n = e.getGraphNameNode();
		if (n != null) {
			return new ElementNamedGraph(processNode(n),
					renumber(e.getElement()));
		}
		else {
			return new ElementNamedGraph(renumber(e.getElement()));
		}
	}

	private ElementPathBlock renumberPathBlock(final ElementPathBlock e) {
		final ElementPathBlock retval = new ElementPathBlock();
		final PathBlock pb = e.getPattern();
		for (final TriplePath tp : pb.getList()) {
			if (tp.isTriple()) {
				final Triple t = new Triple(processNode(tp.getSubject()),
						tp.getPredicate(), processNode(tp.getObject()));
				retval.addTriple(t);
			}
			else {
				final TriplePath tp2 = new TriplePath(
						processNode(tp.getSubject()), tp.getPath(),
						processNode(tp.getObject()));
				retval.addTriple(tp2);
			}
		}
		return retval;
	}

	private ElementTriplesBlock renumberTriplesBlock(final ElementTriplesBlock e) {
		final ElementTriplesBlock retval = new ElementTriplesBlock();
		final Iterator<Triple> iter = e.patternElts();
		while (iter.hasNext()) {
			final Triple tp = iter.next();
			retval.addTriple(new Triple(processNode(tp.getSubject()), tp
					.getPredicate(), processNode(tp.getObject())));
		}
		return retval;
	}

	private ElementUnion renumberUnion(final ElementUnion e) {
		final ElementUnion retval = new ElementUnion();
		for (final Element el : e.getElements()) {
			retval.addElement(renumber(el));
		}
		return retval;
	}
}