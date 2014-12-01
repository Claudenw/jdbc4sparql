/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFList;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.xenei.jdbc4sparql.iface.Key;
import org.xenei.jdbc4sparql.iface.KeySegment;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.EntityManagerRequiredException;
import org.xenei.jena.entities.MissingAnnotation;
import org.xenei.jena.entities.ResourceWrapper;
import org.xenei.jena.entities.annotations.Predicate;
import org.xenei.jena.entities.annotations.Subject;

@Subject(namespace = "http://org.xenei.jdbc4sparql/entity/Key#")
public class RdfKey implements Key<RdfKeySegment>, ResourceWrapper  {

	public static class Builder implements Key<RdfKeySegment> {
		private final List<RdfKeySegment> segments = new ArrayList<RdfKeySegment>();
		private boolean unique;
		private String keyName;

		public Builder addSegment(final RdfKeySegment segment) {
			for (final KeySegment seg : segments) {
				if (seg.getIdx() == segment.getIdx()) {
					throw new IllegalArgumentException(
							"Same segment may not be added more than once");
				}
			}
			segments.add(segment);
			return this;
		}

		public RdfKey build(final Model model) {
			checkBuildState();
			final Class<?> typeClass = Key.class;
			final String fqName = String.format("%s/instance/key-%s",
					ResourceBuilder.getFQName(typeClass), getId());
			final ResourceBuilder builder = new ResourceBuilder(model);
			Resource key = null;
			if (builder.hasResource(fqName)) {
				key = builder.getResource(fqName, typeClass);
			} else {
				key = builder.getResource(fqName, typeClass);

				key.addLiteral(builder.getProperty(typeClass, "keyName"),
						StringUtils.defaultString(keyName, getId()));

				key.addLiteral(builder.getProperty(typeClass, "unique"), unique);

				RDFList lst = null;

				for (final RdfKeySegment seg : segments) {
					final Resource s = seg.getResource();
					if (lst == null) {
						lst = model.createList().with(s);
					} else {
						lst.add(s);
					}
				}
				final Property p = builder.getProperty(RdfKey.class,
						"keySegments");

				key.addProperty(p, lst);

			}

			final EntityManager entityManager = EntityManagerFactory
					.getEntityManager();
			try {
				return entityManager.read(key, RdfKey.class);
			} catch (final MissingAnnotation e) {
				throw new RuntimeException(e);
			}
		}

		private void checkBuildState() {
			if (segments.size() == 0) {
				throw new IllegalStateException(
						"there must be at least one key segment");
			}
		}

		@Override
		public int compare(final Object[] data1, final Object[] data2) {
			return Utils.compare(getSegments(), data1, data2);
		}

		@Override
		public String getId() {
			final StringBuilder sb = new StringBuilder().append(isUnique());
			for (final RdfKeySegment ks : getSegments()) {
				sb.append(ks.getId());
			}
			return UUID.nameUUIDFromBytes(sb.toString().getBytes()).toString();
		}

		@Override
		public String getKeyName() {
			return keyName;
		}

		@Override
		public List<RdfKeySegment> getSegments() {
			return segments;
		}

		@Override
		public boolean isUnique() {
			return unique;
		}

		public Builder setKeyName(final String keyName) {
			this.keyName = keyName;
			return this;

		}

		public Builder setUnique(final boolean unique) {
			this.unique = unique;
			return this;
		}
	}

	private List<RdfKeySegment> segments;

	@Override
	public final int compare(final Object[] data1, final Object[] data2) {
		return Utils.compare(getSegments(), data1, data2);
	}

	@Override
	public final String getId() {
		return UUID.nameUUIDFromBytes(toString().getBytes()).toString();
	}

	/**
	 * Get the key name (may be null)
	 *
	 * @return the key name.
	 */
	@Override
	@Predicate(impl = true)
	public String getKeyName() {
		throw new EntityManagerRequiredException();
	}

	@Predicate(impl = true)
	public RDFNode getKeySegments() {
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate(impl = true)
	public Resource getResource() {
		throw new EntityManagerRequiredException();
	}

	@Override
	public List<RdfKeySegment> getSegments() {
		if (segments == null) {
			final RDFList lst = getKeySegments().as(RDFList.class);
			final EntityManager entityManager = EntityManagerFactory
					.getEntityManager();
			segments = new ArrayList<RdfKeySegment>();

			for (final RDFNode n : lst.asJavaList()) {
				try {
					segments.add(entityManager.read(n.asResource(),
							RdfKeySegment.class));
				} catch (final MissingAnnotation e) {
					throw new RuntimeException(e);
				}
			}
		}
		return segments;
	}

	@Override
	@Predicate(impl = true)
	public boolean isUnique() {
		throw new EntityManagerRequiredException();
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder().append(isUnique());
		for (final RdfKeySegment ks : getSegments()) {
			sb.append(ks.getId());
		}
		return sb.toString();
	}

}
