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

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.xenei.jdbc4sparql.iface.KeySegment;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.EntityManagerRequiredException;
import org.xenei.jena.entities.MissingAnnotation;
import org.xenei.jena.entities.ResourceWrapper;
import org.xenei.jena.entities.SubjectInfo;
import org.xenei.jena.entities.annotations.Predicate;
import org.xenei.jena.entities.annotations.Subject;


@Subject(namespace = "http://org.xenei.jdbc4sparql/entity/KeySegment#")
public class RdfKeySegment implements KeySegment, ResourceWrapper {

	public static class Builder implements KeySegment {
		private short idx;
		private boolean ascending = true;

		public RdfKeySegment build(final EntityManager entityManager) {

			final Class<?> typeClass = RdfKeySegment.class;
			final String fqName = String.format("%s/instance/%s",
					ResourceBuilder.getFQName(entityManager,typeClass), getId());
			final ResourceBuilder builder = new ResourceBuilder(entityManager);
			Resource keySegment = null;
			if (builder.hasResource(fqName)) {
				keySegment = builder.getResource(fqName, typeClass);
			}
			else {
				keySegment = builder.getResource(fqName, typeClass);

				keySegment.addLiteral(builder.getProperty(typeClass, "idx"),
						idx);
				keySegment.addLiteral(
						builder.getProperty(typeClass, "ascending"), ascending);

			}

			try {
				return entityManager.read(keySegment, RdfKeySegment.class);
			} catch (final MissingAnnotation e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public int compare(final Comparable<Object>[] data1,
				final Comparable<Object>[] data2) {
			return Utils.compare(getIdx(), isAscending(), data1, data2);
		}

		@Override
		public String getId() {
			return (isAscending() ? "A" : "D") + getIdx();
		}

		@Override
		public short getIdx() {
			return idx;
		}

		@Override
		public boolean isAscending() {
			return ascending;
		}

		public Builder setAscending(final boolean ascending) {
			this.ascending = ascending;
			return this;
		}

		public Builder setIdx(final int idx) {
			if ((idx < 0) || (idx > Short.MAX_VALUE)) {
				throw new IllegalArgumentException(
						"index must be in the range 0 - " + Short.MAX_VALUE);
			}
			this.idx = (short) idx;
			return this;
		}
	}

	@Override
	public final int compare(final Comparable<Object>[] data1,
			final Comparable<Object>[] data2) {
		return Utils.compare(getIdx(), isAscending(), data1, data2);
	}

	@Override
	public final String getId() {
		return (isAscending() ? "A" : "D") + getIdx();
	}

	@Override
	@Predicate(impl = true)
	public short getIdx() {
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate(impl = true)
	public Resource getResource() {
		throw new EntityManagerRequiredException();
	}
	
	@Override
	@Predicate(impl = true)
	public EntityManager getEntityManager() {
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate(impl = true)
	public SubjectInfo getSubjectInfo() {
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate(impl = true)
	public boolean isAscending() {
		throw new EntityManagerRequiredException();
	}
}
