package org.xenei.jdbc4sparql.sparql.parser.jsqlparser;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueString;

class RegexNodeValue extends NodeValueString {
	private final boolean wildcard;

	public RegexNodeValue(final String str) {
		super(parseWildCard(str));
		this.wildcard = hasWildcard(str);
	}

	public boolean isWildcard() {
		return wildcard;
	}

	private final static String SLASH="\\";
	private final static String PATTERN = "[]^.?*+{}()|$_%"+SLASH;
	private final static Map<String, String> CONVERSION = new HashMap<String, String>();

	static {
		for (int i = 0; i < PATTERN.length(); i++) {
			String s = PATTERN.substring(i, i + 1);
			CONVERSION.put(s, SLASH + s);
		}
		CONVERSION.put("_", ".");
		CONVERSION.put("%", "(.+)");
	}

	private static boolean hasWildcard(final String part) {
		String tst = " " + part + " ";
		int barCnt = tst.split("_").length - 1;
		int escBarCnt = tst.split("\\\\_").length - 1;
		int pctCnt = tst.split("%").length - 1;
		int escPctCnt = tst.split("\\\\%").length - 1;
		return (barCnt > escBarCnt) || (pctCnt > escPctCnt);
	}

	private static String parseWildCard(final String part) {
		final StringTokenizer tokenizer = new StringTokenizer(part, PATTERN,
				true);
		final StringBuilder sb = new StringBuilder().append("^");
		final StringBuilder workingToken = new StringBuilder();
		int backslashCount = 0;
		boolean escaping = false;
		while (tokenizer.hasMoreTokens()) {
			final String candidate = tokenizer.nextToken();
			if ((candidate.length() == 1)
					&& CONVERSION.keySet().contains(candidate)) {
				// token

				// an even number of backslashes means that we are just creating backslashes.
				if (backslashCount>0 && backslashCount % 2 != 0) { 
					if (candidate.equals("%") || candidate.equals("_")) {
						sb.setCharAt(sb.length() - 2, candidate.charAt(0));
						sb.setLength(sb.length() - 1);
					} else if (candidate.equals(SLASH)) {
						sb.append( CONVERSION.get(candidate) );
					} else {
						sb.append(candidate);
					}
				} else {
					sb.append(
							workingToken.length() > 0 ? workingToken.toString()
									: "").append(CONVERSION.get(candidate));
					escaping = true;

				}
				if (candidate.equals(SLASH)) {
					backslashCount++;
				} else {
					backslashCount = 0;
				}
				workingToken.setLength(0);

			} else {
				workingToken.append(candidate);
			}
		}
		// end of while
		if (workingToken.length() > 0) {
			sb.append(workingToken.toString());
		}
		sb.append("$");
		// final RegexNodeValue retval = new RegexNodeValue(
		// wildcard ? sb.toString() : workingToken.toString(), wildcard);
		return escaping ? sb.toString() : workingToken.toString();
	}

}