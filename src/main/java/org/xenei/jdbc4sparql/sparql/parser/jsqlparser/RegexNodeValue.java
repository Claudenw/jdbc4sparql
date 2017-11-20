package org.xenei.jdbc4sparql.sparql.parser.jsqlparser;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.jena.sparql.expr.nodevalue.NodeValueString;

class RegexNodeValue extends NodeValueString {
	private final boolean wildcard;

	private RegexNodeValue(final String str, final boolean wildcard) {
		super(str);
		this.wildcard = wildcard;
	}

	public boolean isWildcard() {
		return wildcard;
	}

	private final static String SLASH = "\\";
	private final static String PATTERN = "[]^.?*+{}()|$_%" + SLASH;
	private final static Map<String, String> CONVERSION = new HashMap<String, String>();

	static {
		for (int i = 0; i < PATTERN.length(); i++) {
			final String s = PATTERN.substring(i, i + 1);
			CONVERSION.put(s, SLASH + s);
		}
		CONVERSION.put("_", ".");
		CONVERSION.put("%", "(.+)");
	}

	public static RegexNodeValue create(final String part) {
		final StringTokenizer tokenizer = new StringTokenizer(part, PATTERN,
				true);
		final StringBuilder sb = new StringBuilder().append("^");
		final StringBuilder plainSb = new StringBuilder();
		final StringBuilder workingToken = new StringBuilder();
		int backslashCount = 0;
		int wildcard = 0;
		int escaping = 0;
		while (tokenizer.hasMoreTokens()) {
			final String candidate = tokenizer.nextToken();
			plainSb.append(candidate);
			if ((candidate.length() == 1)
					&& CONVERSION.keySet().contains(candidate)) {
				// token

				// an even number of backslashes means that we are just creating
				// backslashes.
				if ((backslashCount > 0) && ((backslashCount % 2) != 0)) {
					if (candidate.equals("%") || candidate.equals("_")) {
						sb.setCharAt(sb.length() - 2, candidate.charAt(0));
						sb.setLength(sb.length() - 1);
						plainSb.setCharAt(sb.length() - 2, candidate.charAt(0));
						plainSb.setLength(sb.length() - 1);
						escaping--;
					}
					else if (candidate.equals(SLASH)) {
						sb.append(CONVERSION.get(candidate));
						escaping++;
					}
					else {
						sb.append(candidate);
					}
				}
				else {
					sb.append(
							workingToken.length() > 0 ? workingToken.toString()
									: "").append(CONVERSION.get(candidate));
					escaping++;
					if (candidate.equals("%") || candidate.equals("_")) {
						wildcard++;
					}

				}
				if (candidate.equals(SLASH)) {
					backslashCount++;
				}
				else {
					backslashCount = 0;
				}
				workingToken.setLength(0);

			}
			else {
				workingToken.append(candidate);
				backslashCount = 0;
			}
		}
		// end of while
		if (workingToken.length() > 0) {
			sb.append(workingToken.toString());
		}
		sb.append("$");

		return new RegexNodeValue(
				(escaping > 0) && (wildcard > 0) ? sb.toString()
						: plainSb.toString(), wildcard > 0);
	}

}