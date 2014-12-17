package org.codefamily.crabs.util;

public final class RegularExpressionHelper {

    private static final char SINGLE_CHAR = '_';

    private static final char MULTI_CHAR = '%';

    private static final String SINGLE_REGULAR_EXPRESSION = "\\E.*\\Q";

    private static final String MULTI_REGULAR_EXPRESSION = "\\E.\\Q";

    public static String toJavaRegularExpression(final String SQLRegularExpression) {
        if (SQLRegularExpression == null) {
            throw new IllegalArgumentException("Argument [SQLRegularExpression] is null.");
        }
        final StringBuilder stringBuilder = new StringBuilder(SQLRegularExpression.length());
        // From the JDK doc: \Q and \E protect everything between them
        stringBuilder.append("\\Q");
        boolean wasSlash = false;
        for (int i = 0; i < SQLRegularExpression.length(); i++) {
            final char character = SQLRegularExpression.charAt(i);
            if (wasSlash) {
                stringBuilder.append(character);
                wasSlash = false;
            } else {
                switch (character) {
                    case SINGLE_CHAR:
                        stringBuilder.append(MULTI_REGULAR_EXPRESSION);
                        break;
                    case MULTI_CHAR:
                        stringBuilder.append(SINGLE_REGULAR_EXPRESSION);
                        break;
                    case '\\':
                        wasSlash = true;
                        break;
                    default:
                        stringBuilder.append(character);
                }
            }
        }
        stringBuilder.append("\\E");
        // Found nothing interesting
        return stringBuilder.toString();
    }

    private RegularExpressionHelper() {
        // to do nothing.
    }

}
