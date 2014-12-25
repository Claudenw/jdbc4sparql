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

public interface KeySegment extends Comparator<Comparable<Object>[]> {

	public String getId();

	public short getIdx();

	public boolean isAscending();

	public static class Utils {

		public static final int compare(final int idx,
				final boolean isAscending, final Comparable<Object>[] data1,
				final Object[] data2) {
			final Object o1 = data1[idx];
			final Object o2 = data2[idx];
			int retval;
			if (o1 == null) {
				retval = o2 == null ? 0 : -1;
			}
			else if (o2 == null) {
				retval = 1;
			}
			else {
				return data1[idx].compareTo(data2[idx]);
//				retval = Comparable.class.cast(data1[idx])
//						.compareTo(data2[idx]);
			}
			return isAscending ? retval : -1 * retval;
		}

	}
}
