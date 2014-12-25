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
package org.xenei.jdbc4sparql.iface;

import java.util.Comparator;
import java.util.List;

import org.xenei.jena.entities.annotations.Subject;

@Subject(namespace = "http://org.xenei.jdbc4sparql/entity/Key#")
public interface Key<T extends KeySegment> extends Comparator<Comparable<Object>[]> {

	public String getId();

	/**
	 * Get the key name (may be null)
	 *
	 * @return the key name.
	 */
	public String getKeyName();

	public List<T> getSegments();

	public boolean isUnique();

	public static class Utils {

		public final static int compare(
				final List<? extends KeySegment> segments,
				final Comparable<Object>[] data1, final Comparable<Object>[] data2) {
			for (final KeySegment segment : segments) {
				final int retval = segment.compare(data1, data2);
				if (retval != 0) {
					return retval;
				}
			}
			return 0;
		}

	}
}
