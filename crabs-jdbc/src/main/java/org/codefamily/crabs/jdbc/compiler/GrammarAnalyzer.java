package org.codefamily.crabs.jdbc.compiler;

import org.codefamily.crabs.common.ExtensionClassCollector;
import org.codefamily.crabs.common.util.ReadonlyList;
import org.codefamily.crabs.jdbc.lang.Clause;
import org.codefamily.crabs.jdbc.lang.Clause.TableDeclare;
import org.codefamily.crabs.jdbc.lang.Expression;
import org.codefamily.crabs.jdbc.lang.Keyword;
import org.codefamily.crabs.jdbc.lang.Statement;
import org.codefamily.crabs.jdbc.lang.expression.Argument;
import org.codefamily.crabs.jdbc.lang.expression.Constant;
import org.codefamily.crabs.jdbc.lang.expression.Reference;
import org.codefamily.crabs.jdbc.lang.expression.util.ExtensionExpressionFactory;
import org.codefamily.crabs.jdbc.lang.extension.ReservedKeyword;
import org.codefamily.crabs.jdbc.lang.extension.clause.FromClause;
import org.codefamily.crabs.jdbc.lang.extension.expression.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;

public class GrammarAnalyzer {

    public static Statement analyze(final String sql) throws SQLException {
        if (sql == null) {
            throw new IllegalArgumentException("Argument[sql] is null.");
        }
        final GrammarAnalyzeContext context = new GrammarAnalyzeContext(sql);
        context.toNextToken(); // start.
        final Statement statement = StatementGrammarAnalyzer.analyze(context);
        context.toNextToken();// finish.
        if (context.currentTokenType() == TokenType.SYMBOL
                && context.currentTokenToSymbol() == ';') {
            context.toNextToken();
        }
        if (context.currentTokenType() == TokenType.EOF) {
            return statement;
        }
        throw newSQLException(context, "Unexpected sql end.", context.currentTokenStartPosition());
    }

    protected static String analyzeIdentifier(final GrammarAnalyzeContext context) throws SQLException {
        if (!(context.currentTokenType() == TokenType.NUMBERS
                || context.currentTokenType() == TokenType.LETTERS)) {
            return null;
        }
        final StringBuilder stringBuilder = context.getEmptyStringBuilder();
        context.currentTokenTo(stringBuilder);
        for (; ; ) {
            if (!context.toNextToken()) {
                switch (context.currentTokenType()) {
                    case NUMBERS:
                    case LETTERS:
                        context.currentTokenTo(stringBuilder);
                        continue;
                }
            }
            break;
        }
        return stringBuilder.toString();
    }

    protected static String analyzeGeneralizedIdentifier(
            final GrammarAnalyzeContext context) throws SQLException {
        switch (context.currentTokenType()) {
            case SYMBOL:
                final char expectEndSymbol;
                switch (context.currentTokenToSymbol()) {
                    case '`':
                        expectEndSymbol = '`';
                        break;
                    case '"':
                        expectEndSymbol = '"';
                        break;
                    case '\'':
                        expectEndSymbol = '\'';
                        break;
                    default:
                        return analyzeIdentifier(context);
                }
                // 字符串
                final int currentTokenStartPosition = context.currentTokenStartPosition();
                context.toNextToken(expectEndSymbol);
                final StringBuilder stringBuilder = context.getEmptyStringBuilder();
                context.currentTokenTo(stringBuilder);
                context.toNextToken();
                switch (context.currentTokenType()) {
                    case EOF:
                        throw newSQLException(context, null, null);
                    case SYMBOL:
                        if (context.currentTokenToSymbol() != expectEndSymbol) {
                            // 引号不匹配
                            throw newSQLException(context, "Expect '" + expectEndSymbol
                                    + "'. ", context.currentTokenStartPosition());
                        }
                        // 引号匹配
                        context.toNextToken();
                        final String identifier = stringBuilder.toString();
                        checkIdentifier(identifier);
                        return identifier;
                    default:
                        throw newSQLException(
                                context,
                                "Expect a string constant.",
                                currentTokenStartPosition
                        );
                }
            default:
                return analyzeIdentifier(context);
        }
    }

    protected static SQLException newSQLException(final String message) {
        return new SQLException(message);
    }

    protected static SQLException newSQLException(final GrammarAnalyzeContext context,
                                                  final String message,
                                                  final Integer statementStringPosition) {
        if (context.currentTokenType() == TokenType.EOF) {
            return new SQLException("EOF");
        }
        if (statementStringPosition == null) {
            return new SQLException(message);
        }
        String statementString = context.statementString;
        statementString = statementString.replace('\n', ' ');
        statementString = statementString.replace('\t', ' ');
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(message);
        stringBuilder.append('\n');
        stringBuilder.append(statementString);
        stringBuilder.append('\n');
        for (int i = 0; i < statementStringPosition; i++) {
            stringBuilder.append(' ');
        }
        stringBuilder.append('^');
        return new SQLException(stringBuilder.toString());
    }

    protected static void expectSymbol(final GrammarAnalyzeContext context,
                                       final char symbol) throws SQLException {
        if (context.currentTokenType() != TokenType.SYMBOL
                || context.currentTokenToSymbol() != symbol) {
            throw newSQLException(
                    context,
                    "Expect '" + symbol + "'.",
                    context.currentTokenStartPosition()
            );
        }
    }

    protected static void expectKeyword(final GrammarAnalyzeContext context,
                                        final Keyword keyword) throws SQLException {
        if (context.currentTokenType() != TokenType.KEYWORD
                || !context.currentTokenToKeyword().getName().equals(keyword.getName())) {
            throw newSQLException(
                    context,
                    "Expect keyword " + keyword.getName() + ".",
                    context.currentTokenStartPosition()
            );
        }
    }

    protected static Expression[] expressionsListToArray(final ArrayList<Expression> expressionList,
                                                         final int startListIndex) {
        final int endIndex = expressionList.size() - 1;
        final int count = expressionList.size() - startListIndex;
        final Expression[] expressions = new Expression[count];
        for (int index = endIndex; index >= startListIndex; index--) {
            expressions[index - startListIndex] = expressionList.remove(index);
        }
        return expressions;
    }

    protected static void checkIdentifier(final String identifier) throws SQLException {
        if (identifier.isEmpty()) {
            throw new SQLException("Illegal identifier. []");
        }
        for (int i = 0, identifierLength = identifier.length(); i < identifierLength; i++) {
            if (!Character.isJavaIdentifierPart(identifier.charAt(i))) {
                throw new SQLException("Illegal identifier. [" + identifier + "]");
            }
        }
    }

    private GrammarAnalyzer() {
        // nothing to do.
    }

    public enum TokenType {

        BOF,

        KEYWORD,

        NUMBERS,

        LETTERS,

        STRING,

        SYMBOL,

        EOF

    }

    public static final class GrammarAnalyzeContext {

        GrammarAnalyzeContext(final String statementString) {
            this.statementString = statementString;
            this.statementCharacters = statementString.toCharArray();
            this.keywordStringBuilder = new KeywordStringBuilder();
            this.currentTokenStartPosition = -1;
            this.currentTokenLength = 0;
            this.currentTokenType = TokenType.BOF;
            this.nextArgumentIndex = 0;
        }

        final String statementString;

        private final char[] statementCharacters;

        private final KeywordStringBuilder keywordStringBuilder;

        private int currentTokenStartPosition;

        private int currentTokenLength;

        private TokenType currentTokenType;

        private Keyword currentKeywordToken;

        private int nextArgumentIndex;

        private StringBuilder stringBuilder;

        private ArrayList<Expression> expressionList;

        public final Argument newArgument() {
            return new Argument(this.nextArgumentIndex++);
        }

        public final StringBuilder getEmptyStringBuilder() {
            if (this.stringBuilder == null) {
                this.stringBuilder = new StringBuilder();
            } else {
                this.stringBuilder.setLength(0);
            }
            return this.stringBuilder;
        }

        public final ArrayList<Expression> getExpressionList() {
            if (this.expressionList == null) {
                this.expressionList = new ArrayList<Expression>();
            } else {
                this.expressionList.clear();
            }
            return this.expressionList;
        }

        public final int currentTokenStartPosition() {
            return this.currentTokenStartPosition;
        }

        public final TokenType currentTokenType() {
            return this.currentTokenType;
        }

        public final void currentTokenTo(final StringBuilder stringBuilder) {
            switch (this.currentTokenType) {
                case BOF:
                case EOF:
                    return;
                default:
                    stringBuilder
                            .append(this.statementCharacters,
                                    this.currentTokenStartPosition,
                                    this.currentTokenLength);
            }
        }

        public final Keyword currentTokenToKeyword() {
            if (this.currentTokenType == TokenType.KEYWORD) {
                return this.currentKeywordToken;
            } else {
                throw new RuntimeException("Current token is not keyword.");
            }
        }

        public final char currentTokenToSymbol() {
            if (this.currentTokenType == TokenType.SYMBOL) {
                return this.statementCharacters[this.currentTokenStartPosition];
            } else {
                throw new RuntimeException("Current token is not symbol.");
            }
        }

        public final String currentTokenToString() {
            final StringBuilder stringBuilder = this.getEmptyStringBuilder();
            this.currentTokenTo(stringBuilder);
            return stringBuilder.toString();
        }

        /**
         * @return 是否过滤了空白字符
         */
        public final boolean toNextToken() {
            boolean filteredWhitespace = false;
            int currentPosition;
            switch (this.currentTokenType) {
                case BOF:
                    currentPosition = 0;
                    break;
                case EOF:
                    return filteredWhitespace;
                default:
                    currentPosition = this.currentTokenStartPosition + this.currentTokenLength;
            }
            if (this.currentTokenType == TokenType.EOF) {
                return filteredWhitespace;
            }
            final char[] statementCharacters = this.statementCharacters;
            final int statementCharacterCount = statementCharacters.length;
            for (; ; ) {
                if (currentPosition == statementCharacterCount) {
                    this.currentTokenStartPosition = currentPosition;
                    this.currentTokenLength = 0;
                    this.currentKeywordToken = null;
                    this.currentTokenType = TokenType.EOF;
                    return filteredWhitespace;
                }
                if (!Character.isWhitespace(statementCharacters[currentPosition])) {
                    break;
                }
                currentPosition++;
                filteredWhitespace = true;
            }
            this.currentTokenStartPosition = currentPosition;
            char currentCharacter = statementCharacters[currentPosition];
            if (Character.isJavaIdentifierStart(currentCharacter)) {
                switch (currentCharacter) {
                    case 'a':
                    case 'b':
                    case 'c':
                    case 'd':
                    case 'e':
                    case 'f':
                    case 'g':
                    case 'h':
                    case 'i':
                    case 'j':
                    case 'k':
                    case 'l':
                    case 'm':
                    case 'n':
                    case 'o':
                    case 'p':
                    case 'q':
                    case 'r':
                    case 's':
                    case 't':
                    case 'u':
                    case 'v':
                    case 'w':
                    case 'x':
                    case 'y':
                    case 'z':
                        currentCharacter = (char) (currentCharacter - ('a' - 'A'));
                }
                final KeywordStringBuilder keywordStringBuilder = this.keywordStringBuilder;
                keywordStringBuilder.reset();
                keywordStringBuilder.append(currentCharacter);
                for (; ; ) {
                    if ((++currentPosition) < statementCharacterCount) {
                        currentCharacter = statementCharacters[currentPosition];
                        if (Character.isJavaIdentifierPart(currentCharacter)) {
                            switch (currentCharacter) {
                                case 'a':
                                case 'b':
                                case 'c':
                                case 'd':
                                case 'e':
                                case 'f':
                                case 'g':
                                case 'h':
                                case 'i':
                                case 'j':
                                case 'k':
                                case 'l':
                                case 'm':
                                case 'n':
                                case 'o':
                                case 'p':
                                case 'q':
                                case 'r':
                                case 's':
                                case 't':
                                case 'u':
                                case 'v':
                                case 'w':
                                case 'x':
                                case 'y':
                                case 'z':
                                    currentCharacter = (char) (currentCharacter - ('a' - 'A'));
                            }
                            keywordStringBuilder.append(currentCharacter);
                            continue;
                        }
                    }
                    break;
                }
                if ((this.currentKeywordToken = keywordStringBuilder.tryToKeyword()) == null) {
                    this.currentTokenType = TokenType.LETTERS;
                } else {
                    this.currentTokenType = TokenType.KEYWORD;
                }
            } else {
                switch (currentCharacter) {
                    case '0':
                    case '1':
                    case '2':
                    case '3':
                    case '4':
                    case '5':
                    case '6':
                    case '7':
                    case '8':
                    case '9':
                        for (; ; ) {
                            if ((++currentPosition) < statementCharacterCount) {
                                switch (statementCharacters[currentPosition]) {
                                    case '0':
                                    case '1':
                                    case '2':
                                    case '3':
                                    case '4':
                                    case '5':
                                    case '6':
                                    case '7':
                                    case '8':
                                    case '9':
                                        continue;
                                }
                            }
                            break;
                        }
                        this.currentKeywordToken = null;
                        this.currentTokenType = TokenType.NUMBERS;
                        break;
                    default:
                        currentPosition++;
                        this.currentKeywordToken = null;
                        this.currentTokenType = TokenType.SYMBOL;
                }
            }
            this.currentTokenLength = currentPosition - this.currentTokenStartPosition;
            return filteredWhitespace;
        }

        public final void toNextToken(final char finishCharacter) {
            if (this.currentTokenType == TokenType.EOF) {
                return;
            }
            final char[] statementCharacters = this.statementCharacters;
            final int statementCharacterCount = statementCharacters.length;
            int currentPosition = this.currentTokenStartPosition + this.currentTokenLength;
            this.currentTokenStartPosition = currentPosition;
            for (; ; ) {
                if (currentPosition == statementCharacterCount
                        || statementCharacters[currentPosition] == finishCharacter) {
                    this.currentTokenType = TokenType.STRING;
                    this.currentTokenLength = currentPosition - this.currentTokenStartPosition;
                    return;
                }
                currentPosition++;
            }
        }

        public static void recollectKeywords() {
            KeywordStringBuilder.recollectKeywords();
        }

        private static final class KeywordStringBuilder {

            private static final Logger LOGGER = LoggerFactory.getLogger(KeywordStringBuilder.class);

            private static final int KEYWORD_ENTRY_MAP_CAPACITY = 'Z' - 'A' + 1;

            private static final KeywordMapEntry[][] KEYWORD_ENTRY_MAP
                    = new KeywordMapEntry[KEYWORD_ENTRY_MAP_CAPACITY][];

            static void recollectKeywords() {
                synchronized (KeywordStringBuilder.class) {
                    final ArrayList<Keyword> keywordList = new ArrayList<Keyword>();
                    Collections.addAll(keywordList, ReservedKeyword.values());
                    final Iterator<Class<? extends Keyword>> keywordClassIterator
                            = ExtensionClassCollector.getExtensionClasses(Keyword.class);
                    while (keywordClassIterator.hasNext()) {
                        final Class<? extends Keyword> keywordClass = keywordClassIterator.next();
                        if (keywordClass.isEnum()) {
                            Collections.addAll(keywordList, keywordClass.getEnumConstants());
                        } else {
                            try {
                                keywordList.add(keywordClass.newInstance());
                            } catch (Throwable t) {
                                LOGGER.error("Can not register keyword.", t);
                            }
                        }
                    }
                    final HashSet<String> keywordNameSet = new HashSet<String>();
                    @SuppressWarnings("unchecked")
                    final ArrayList<KeywordMapEntry>[] keywordMap = new ArrayList[KEYWORD_ENTRY_MAP_CAPACITY];
                    for (Keyword keyword : keywordList) {
                        final String upperKeywordName = keyword.getName().toUpperCase();
                        if (keywordNameSet.contains(upperKeywordName)) {
                            continue;
                        }
                        final KeywordMapEntry keywordMapEntry = new KeywordMapEntry(keyword);
                        final char[] key = keywordMapEntry.key;
                        for (int i = 0; i < key.length; i++) {
                            switch (key[i]) {
                                case 'A':
                                case 'B':
                                case 'C':
                                case 'D':
                                case 'E':
                                case 'F':
                                case 'G':
                                case 'H':
                                case 'I':
                                case 'J':
                                case 'K':
                                case 'L':
                                case 'M':
                                case 'N':
                                case 'O':
                                case 'P':
                                case 'Q':
                                case 'R':
                                case 'S':
                                case 'T':
                                case 'U':
                                case 'V':
                                case 'W':
                                case 'X':
                                case 'Y':
                                case 'Z':
                                case '_':
                                case '$':
                                    continue;
                            }
                            throw new RuntimeException("Keyword name contain illegal character. " + keyword.getName());
                        }
                        final int mapIndex = (key[0] - 'A') & (KEYWORD_ENTRY_MAP_CAPACITY - 1);
                        ArrayList<KeywordMapEntry> keywordMapEntryList = keywordMap[mapIndex];
                        if (keywordMapEntryList == null) {
                            keywordMapEntryList = new ArrayList<KeywordMapEntry>();
                            keywordMap[mapIndex] = keywordMapEntryList;
                        }
                        keywordMapEntryList.add(keywordMapEntry);
                        keywordNameSet.add(upperKeywordName);
                    }
                    for (int i = 0; i < KEYWORD_ENTRY_MAP_CAPACITY; i++) {
                        final ArrayList<KeywordMapEntry> keywordMapEntryList = keywordMap[i];
                        if (keywordMapEntryList != null) {
                            KEYWORD_ENTRY_MAP[i] = keywordMapEntryList.toArray(
                                    new KeywordMapEntry[keywordMapEntryList.size()]
                            );
                        }
                    }
                }
            }

            static {
                recollectKeywords();
            }

            KeywordStringBuilder() {
                this.characters = new char[10];
                this.length = 0;
            }

            private char[] characters;

            private int length;

            final Keyword tryToKeyword() {
                if (this.length != 0) {
                    final char[] characters = this.characters;
                    final int mapIndex = (characters[0] - 'A') & (KEYWORD_ENTRY_MAP_CAPACITY - 1);
                    if (mapIndex < KEYWORD_ENTRY_MAP_CAPACITY) {
                        final KeywordMapEntry[] keywordMapEntries = KEYWORD_ENTRY_MAP[mapIndex];
                        if (keywordMapEntries != null) {
                            final int length = this.length;
                            for (int i = 0; i < keywordMapEntries.length; i++) {
                                final KeywordMapEntry keywordMapEntry = keywordMapEntries[i];
                                final char[] key = keywordMapEntry.key;
                                if (key.length == length) {
                                    compare:
                                    {
                                        for (int j = 0; j < length; j++) {
                                            if (key[j] != characters[j]) {
                                                break compare;
                                            }
                                        }
                                        return keywordMapEntry.value;
                                    }
                                }
                            }
                        }
                    }
                }
                return null;
            }

            final void append(final char upperCharacter) {
                if (this.length == this.characters.length) {
                    final char[] newCharacters = new char[this.characters.length + 10];
                    System.arraycopy(this.characters, 0, newCharacters, 0, this.length);
                    this.characters = newCharacters;
                }
                this.characters[this.length] = upperCharacter;
                this.length++;
            }

            final void reset() {
                this.length = 0;
            }

            private static final class KeywordMapEntry {

                KeywordMapEntry(final Keyword keyword) {
                    this.key = keyword.getName().toUpperCase().toCharArray();
                    this.value = keyword;
                }

                final char[] key;

                final Keyword value;

            }

        }

    }

    private static final class StatementGrammarAnalyzer extends GrammarAnalyzer {

        static Statement analyze(final GrammarAnalyzeContext context) throws SQLException {
            switch (context.currentTokenType()) {
                case EOF:
                    return null;
                case SYMBOL:
                    if (context.currentTokenToSymbol() == '(') {
                        context.toNextToken();
                        final Statement statement = analyze(context);
                        expectSymbol(context, ')');
                        context.toNextToken();
                        return statement;
                    }
                    break;
                case KEYWORD:
                    return StatementFactory.toStatement(analyzeClauseList(context));
            }
            throw new SQLException("SQL must start with keyword or '('.");
        }

        private static ArrayList<Clause> analyzeClauseList(
                final GrammarAnalyzeContext context) throws SQLException {
            final ArrayList<Clause> clauseList = new ArrayList<Clause>();
            for (; ; ) {
                final Clause clause = ClauseGrammarAnalyzer.analyze(context);
                if (clause == null) {
                    return clauseList;
                }
                clauseList.add(clause);
            }
        }

        private StatementGrammarAnalyzer() {
            // to do nothing.
        }

    }

    public static abstract class ClauseGrammarAnalyzer extends GrammarAnalyzer {

        private static final Logger LOGGER = LoggerFactory.getLogger(ClauseGrammarAnalyzer.class);

        private static final HashMap<Keyword, AnalyzerMapEntry> ANALYZER_MAP
                = new HashMap<Keyword, AnalyzerMapEntry>();

        public static void recollectClauseGrammarAnalyzers() {
            synchronized (ClauseGrammarAnalyzer.class) {
                final Iterator<Class<? extends ClauseGrammarAnalyzer>> analyzerClassIterator
                        = ExtensionClassCollector.getExtensionClasses(ClauseGrammarAnalyzer.class);
                while (analyzerClassIterator.hasNext()) {
                    final Class<? extends ClauseGrammarAnalyzer> analyzerClass = analyzerClassIterator.next();
                    try {
                        if (registerClauseGrammarAnalyzer((Class<? extends ClauseGrammarAnalyzer>) analyzerClass)
                                != analyzerClass) {
                            throw new RuntimeException("Clause grammar analyzer conflicted." + analyzerClass.getName());
                        }
                    } catch (Throwable e) {
                        LOGGER.error("Can not register clause grammar analyzer.", e);
                    }
                }
            }
        }

        static Clause analyze(final GrammarAnalyzeContext context) throws SQLException {
            if (context.currentTokenType() == TokenType.KEYWORD) {
                ClauseGrammarAnalyzer analyzer = null;
                HashMap<Keyword, AnalyzerMapEntry> analyzerMap = ANALYZER_MAP;
                for (; ; ) {
                    final AnalyzerMapEntry analyzerMapEntry = analyzerMap.get(context.currentTokenToKeyword());
                    if (analyzerMapEntry == null) {
                        break;
                    } else {
                        analyzer = analyzerMapEntry.analyzer;
                        context.toNextToken();
                        if (context.currentTokenType() == TokenType.KEYWORD) {
                            analyzerMap = analyzerMapEntry.analyzerMap;
                        } else {
                            break;
                        }
                    }
                }
                if (analyzer != null) {
                    return analyzer.doAnalyze(context);
                }
            }
            return null;
        }

        private static Class<?> registerClauseGrammarAnalyzer(
                final Class<? extends ClauseGrammarAnalyzer> clauseGrammarAnalyzerClass)
                throws InstantiationException, IllegalAccessException {
            final ClauseGrammarAnalyzer clauseGrammarAnalyzer = clauseGrammarAnalyzerClass.newInstance();
            HashMap<Keyword, AnalyzerMapEntry> analyzerMap = ANALYZER_MAP;
            AnalyzerMapEntry analyzerMapEntry = null;
            final ReadonlyList<Keyword> prefixKeywordList = clauseGrammarAnalyzer.prefixKeywordList;
            for (int i = 0, prefixKeywordCount = prefixKeywordList.size(); i < prefixKeywordCount; i++) {
                final Keyword keyword = prefixKeywordList.get(i);
                analyzerMapEntry = analyzerMap.get(keyword);
                if (analyzerMapEntry == null) {
                    analyzerMapEntry = new AnalyzerMapEntry();
                    analyzerMap.put(keyword, analyzerMapEntry);
                }
                analyzerMap = analyzerMapEntry.analyzerMap;
            }
            if (analyzerMapEntry != null) {
                if (analyzerMapEntry.analyzer == null) {
                    analyzerMapEntry.analyzer = clauseGrammarAnalyzer;
                }
                return analyzerMapEntry.analyzer.getClass();
            } else {
                return null;
            }
        }

        static {
            recollectClauseGrammarAnalyzers();
        }

        protected ClauseGrammarAnalyzer(final ReadonlyList<Keyword> prefixKeywordList) {
            this.prefixKeywordList = prefixKeywordList;
        }

        private final ReadonlyList<Keyword> prefixKeywordList;

        protected abstract Clause doAnalyze(GrammarAnalyzeContext context) throws SQLException;

        protected static TableDeclare analyzeTableDeclare(
                final GrammarAnalyzeContext context) throws SQLException {
            // todo 当前只支持单表，不支持子查询、关联查询等
            final String tableIdentifier = analyzeGeneralizedIdentifier(context);
            if (tableIdentifier == null) {
                return null;
            }
            final String alias;
            if (context.currentTokenType() == TokenType.KEYWORD
                    && context.currentTokenToKeyword() == ReservedKeyword.AS) {
                context.toNextToken();
                final int currentTokenStartPosition = context.currentTokenStartPosition();
                alias = analyzeGeneralizedIdentifier(context);
                if (alias == null) {
                    throw newSQLException(context,
                            "Expect a column declare alias.",
                            currentTokenStartPosition);
                }
            } else {
                alias = analyzeGeneralizedIdentifier(context);
            }
            return new FromClause.SimpleTableDeclare(alias, tableIdentifier);
        }

        private static final class AnalyzerMapEntry {

            AnalyzerMapEntry() {
                this.analyzerMap = new HashMap<Keyword, AnalyzerMapEntry>();
            }

            ClauseGrammarAnalyzer analyzer;

            final HashMap<Keyword, AnalyzerMapEntry> analyzerMap;

        }

    }

    public static final class ExpressionGrammarAnalyzer extends GrammarAnalyzer {

        private static final Constant CONSTANT_TRUE = new Constant(Boolean.TRUE);

        private static final Constant CONSTANT_FALSE = new Constant(Boolean.FALSE);

        public static Expression analyze(
                final GrammarAnalyzeContext context) throws SQLException {
            return analyzeExpression(context, Byte.MAX_VALUE);// all level
            // expression
        }

        private static Expression analyzeExpression(final GrammarAnalyzeContext context,
                                                    final byte priorityLevel) throws SQLException {
            Expression expression = analyzePrimitiveExpression(context);
            if (expression == null) {
                return null;
            }
            if (priorityLevel < 0) {
                return expression;
            }
            switch (priorityLevel) {
                case 0:
                    expression = analyzePriority0Expression(context, expression);
                    return expression;
                case 1:
                    expression = analyzePriority0Expression(context, expression);
                    expression = analyzePriority1Expression(context, expression);
                    return expression;
                case 2:
                    expression = analyzePriority0Expression(context, expression);
                    expression = analyzePriority1Expression(context, expression);
                    expression = analyzePriority2Expression(context, expression);
                    return expression;
                case 3:
                    expression = analyzePriority0Expression(context, expression);
                    expression = analyzePriority1Expression(context, expression);
                    expression = analyzePriority2Expression(context, expression);
                    expression = analyzePriority3Expression(context, expression);
                    return expression;
                case 4:
                    expression = analyzePriority0Expression(context, expression);
                    expression = analyzePriority1Expression(context, expression);
                    expression = analyzePriority2Expression(context, expression);
                    expression = analyzePriority3Expression(context, expression);
                    expression = analyzePriority4Expression(context, expression);
                    return expression;
                case 5:
                default:
                    expression = analyzePriority0Expression(context, expression);
                    expression = analyzePriority1Expression(context, expression);
                    expression = analyzePriority2Expression(context, expression);
                    expression = analyzePriority3Expression(context, expression);
                    expression = analyzePriority4Expression(context, expression);
                    expression = analyzePriority5Expression(context, expression);
                    return expression;
            }
        }

        private static Expression analyzePriority0Expression(
                final GrammarAnalyzeContext context,
                final Expression firstExpression) throws SQLException {
            final byte secondExpressionPriority = (byte) -1;
            final Expression expression;
            switch (context.currentTokenType()) {
                case SYMBOL:
                    switch (context.currentTokenToSymbol()) {
                        case '*':
                            // 乘法运算
                            /*context.toNextToken();
                            expression = new MultiplicationExpression(
                                    firstExpression,
                                    analyzeExpectedExpression(context, secondExpressionPriority)
                            );
                            break;*/
                            // TODO
                            throw newSQLException(
                                    context,
                                    "Now, it does not support the multiplication operation.",
                                    context.currentTokenStartPosition()
                            );
                        case '/':
                            // 相除取整运算
                            /*context.toNextToken();
                            expression = new DivisionExpression(firstExpression,
                                    analyzeExpectedExpression(context,
                                            secondExpressionPriority));
                            break;*/
                            // TODO
                            throw newSQLException(
                                    context,
                                    "Now, it does not support the division operation.",
                                    context.currentTokenStartPosition()
                            );
                        case '%':
                            // 相除取余运算
                            /*context.toNextToken();
                            expression = new ModuloExpression(firstExpression,
                                    analyzeExpectedExpression(context,
                                            secondExpressionPriority));
                            break;*/
                            // TODO
                            throw newSQLException(
                                    context,
                                    "Now, it does not support the modulo operation.",
                                    context.currentTokenStartPosition()
                            );
                        default:
                            return firstExpression;
                    }
                    // TODO
//                    break;
                case KEYWORD:
                    final Keyword keyword = context.currentTokenToKeyword();
                    if (keyword == ReservedKeyword.MOD) {
                        /*context.toNextToken();
                        expression = new ModuloExpression(
                                firstExpression,
                                analyzeExpectedExpression(context, secondExpressionPriority)
                        );*/
                        // TODO
                        throw newSQLException(
                                context,
                                "Now, it does not support the modulo operation.",
                                context.currentTokenStartPosition()
                        );
                    } else {
                        return firstExpression;
                    }
                    // TODO
//                    break;
                default:
                    return firstExpression;
            }
            // TODO
//            return analyzePriority0Expression(context, expression);
        }

        private static Expression analyzePriority1Expression(
                final GrammarAnalyzeContext context,
                final Expression firstExpression) throws SQLException {
            final byte secondExpressionPriority = (byte) 0;
            final Expression expression;
            switch (context.currentTokenType()) {
                case SYMBOL:
                    switch (context.currentTokenToSymbol()) {
                        case '+':
                            // 加法运算
                            /*context.toNextToken();
                            expression = new AdditionExpression(
                                    firstExpression,
                                    analyzeExpectedExpression(context, secondExpressionPriority)
                            );
                            break;*/
                            // TODO
                            throw newSQLException(
                                    context,
                                    "Now, it does not support the addition operation.",
                                    context.currentTokenStartPosition()
                            );
                        case '-':
                            // 减法运算
                            /*context.toNextToken();
                            expression = new SubtractionExpression(
                                    firstExpression,
                                    analyzeExpectedExpression(context, secondExpressionPriority)
                            );
                            break;*/
                            // TODO
                            throw newSQLException(
                                    context,
                                    "Now, it does not support the subtraction operation.",
                                    context.currentTokenStartPosition()
                            );
                        default:
                            return firstExpression;
                    }
                    // TODO
//                    break;
                default:
                    return firstExpression;
            }
            // TODO
//            return analyzePriority1Expression(context, expression);
        }

        private static Expression analyzePriority2Expression(final GrammarAnalyzeContext context,
                                                             final Expression firstExpression) throws SQLException {
            final byte secondExpressionPriority = (byte) 1;
            final Expression expression;
            switch (context.currentTokenType()) {
                case SYMBOL:
                    switch (context.currentTokenToSymbol()) {
                        case '=':
                            // 等于
                            context.toNextToken();
                            expression = new EqualToExpression(firstExpression,
                                    analyzeExpectedExpression(context,
                                            secondExpressionPriority));
                            break;
                        case '<':
                            context.toNextToken();
                            if (context.currentTokenType() == TokenType.SYMBOL) {
                                switch (context.currentTokenToSymbol()) {
                                    case '=':
                                        // 小于等于
                                        context.toNextToken();
                                        expression = new LessThanOrEqualToExpression(
                                                firstExpression,
                                                analyzeExpectedExpression(context, secondExpressionPriority)
                                        );
                                        break;
                                    case '>':
                                        // 不等于
                                        context.toNextToken();
                                        expression = new UnequalToExpression(
                                                firstExpression,
                                                analyzeExpectedExpression(context, secondExpressionPriority)
                                        );
                                        break;
                                    default:
                                        // 小于
                                        expression = new LessThanExpression(
                                                firstExpression,
                                                analyzeExpectedExpression(context, secondExpressionPriority)
                                        );
                                }
                            } else {
                                // 小于
                                expression = new LessThanExpression(
                                        firstExpression,
                                        analyzeExpectedExpression(context, secondExpressionPriority)
                                );
                            }
                            break;
                        case '>':
                            context.toNextToken();
                            if (context.currentTokenType() == TokenType.SYMBOL) {
                                switch (context.currentTokenToSymbol()) {
                                    case '=':
                                        // 大于等于
                                        context.toNextToken();
                                        expression = new GreaterThanOrEqualToExpression(
                                                firstExpression,
                                                analyzeExpectedExpression(context, secondExpressionPriority)
                                        );
                                        break;
                                    default:
                                        // 大于
                                        expression = new GreaterThanExpression(
                                                firstExpression,
                                                analyzeExpectedExpression(context, secondExpressionPriority)
                                        );
                                }
                            } else {
                                // 大于
                                expression = new GreaterThanExpression(
                                        firstExpression,
                                        analyzeExpectedExpression(context, secondExpressionPriority)
                                );
                            }
                            break;
                        case '!':
                            context.toNextToken();
                            expectSymbol(context, '=');
                            // 不等于
                            context.toNextToken();
                            expression = new UnequalToExpression(
                                    firstExpression,
                                    analyzeExpectedExpression(context, secondExpressionPriority)
                            );
                            break;
                        default:
                            return firstExpression;
                    }
                    break;
                default:
                    return firstExpression;
            }
            return analyzePriority2Expression(context, expression);
        }

        private static Expression analyzePriority3Expression(final GrammarAnalyzeContext context,
                                                             final Expression firstExpression) throws SQLException {
            // final byte secondExpressionPriority = (byte) 2;
            final Expression expression;
            if (context.currentTokenType() == TokenType.KEYWORD) {
                Keyword keyword = context.currentTokenToKeyword();
                if (keyword == ReservedKeyword.IS) {
                    context.toNextToken();
                    if (context.currentTokenType() == TokenType.KEYWORD) {
                        keyword = context.currentTokenToKeyword();
                        if (keyword == ReservedKeyword.NOT) {
                            context.toNextToken();
                            expectKeyword(context, ReservedKeyword.NULL);
                            // 是否不为null表达式
                            context.toNextToken();
                            expression = new IsNotNullExpression(firstExpression);
                        } else if (keyword == ReservedKeyword.NULL) {
                            // 是否为null表达式
                            context.toNextToken();
                            expression = new IsNullExpression(firstExpression);
                        } else {
                            throw newSQLException(
                                    context, "Expect keyword "
                                            + ReservedKeyword.NOT.getName() + " or "
                                            + ReservedKeyword.NULL.getName() + " .",
                                    context.currentTokenStartPosition()
                            );
                        }
                    } else {
                        throw newSQLException(
                                context, "Expect keyword "
                                        + ReservedKeyword.NOT.getName() + " or "
                                        + ReservedKeyword.NULL.getName() + " .",
                                context.currentTokenStartPosition()
                        );
                    }
                } else if (keyword == ReservedKeyword.NOT) {
                    context.toNextToken();
                    if (context.currentTokenType() == TokenType.KEYWORD) {
                        keyword = context.currentTokenToKeyword();
                        if (keyword == ReservedKeyword.IN) {
                            context.toNextToken();
                            expression = new NotExpression(analyzeInExpression(context, firstExpression));
                        } else if (keyword == ReservedKeyword.LIKE) {
                            context.toNextToken();
                            expression = new NotExpression(analyzeLikeExpression(context, firstExpression));
                        } else if (keyword == ReservedKeyword.BETWEEN) {
                            context.toNextToken();
                            expression = new NotExpression(analyzeBetweenExpression(context, firstExpression));
                        } else {
                            return firstExpression;
                        }
                    } else {
                        return firstExpression;
                    }
                } else if (keyword == ReservedKeyword.IN) {
                    context.toNextToken();
                    expression = analyzeInExpression(context, firstExpression);
                } else if (keyword == ReservedKeyword.LIKE) {
                    context.toNextToken();
                    expression = analyzeLikeExpression(context, firstExpression);
                } else if (keyword == ReservedKeyword.BETWEEN) {
                    context.toNextToken();
                    expression = analyzeBetweenExpression(context, firstExpression);
                } else {
                    return firstExpression;
                }
            } else {
                return firstExpression;
            }
            return analyzePriority3Expression(context, expression);
        }

        private static Expression analyzePriority4Expression(final GrammarAnalyzeContext context,
                                                             final Expression firstExpression) throws SQLException {
            final byte secondExpressionPriority = (byte) 3;
            final Expression expression;
            if (context.currentTokenType() == TokenType.KEYWORD) {
                if (context.currentTokenToKeyword() == ReservedKeyword.AND) {
                    context.toNextToken();
                    expression = new AndExpression(
                            firstExpression,
                            analyzeExpectedExpression(context, secondExpressionPriority)
                    );
                } else {
                    return firstExpression;
                }
            } else {
                return firstExpression;
            }
            return analyzePriority4Expression(context, expression);
        }

        private static Expression analyzePriority5Expression(final GrammarAnalyzeContext context,
                                                             final Expression firstExpression) throws SQLException {
            final byte secondExpressionPriority = (byte) 4;
            final Expression expression;
            if (context.currentTokenType() == TokenType.KEYWORD) {
                if (context.currentTokenToKeyword() == ReservedKeyword.OR) {
                    context.toNextToken();
                    expression = new OrExpression(
                            firstExpression,
                            analyzeExpectedExpression(context, secondExpressionPriority)
                    );
                } else {
                    return firstExpression;
                }
            } else {
                return firstExpression;
            }
            return analyzePriority5Expression(context, expression);
        }

        /**
         * 解析优先级最高的表达式和前缀表达式
         */
        private static Expression analyzePrimitiveExpression(
                final GrammarAnalyzeContext context) throws SQLException {
            final String firstIdentifier;
            int currentTokenStartPosition;
            switch (context.currentTokenType()) {
                case EOF:
                    return null;
                case KEYWORD:
                    final Keyword keyword = context.currentTokenToKeyword();
                    if (keyword == ReservedKeyword.TRUE) {
                        // 逻辑真
                        context.toNextToken();
                        return CONSTANT_TRUE;
                    } else if (keyword == ReservedKeyword.FALSE) {
                        // 逻辑假
                        context.toNextToken();
                        return CONSTANT_FALSE;
                    } else if (keyword == ReservedKeyword.NOT) {
                        // 取反
                        /*context.toNextToken();
                        return new NotExpression(analyzeExpectedExpression(context, Byte.MAX_VALUE));*/
                        // TODO 暂不支持取反运算
                        throw new SQLException("Now, not is not supported in select clause.");
                    } else {
                        return null;
                    }
                case NUMBERS:
                    final Constant numberConstant;
                    String currentTokenString = context.currentTokenToString();
                    context.toNextToken();
                    if (context.currentTokenType == TokenType.SYMBOL
                            && context.currentTokenToSymbol() == '.') {
                        context.toNextToken();
                        if (context.currentTokenType == TokenType.NUMBERS) {
                            final double value = Double.parseDouble(
                                    currentTokenString + "." + context.currentTokenToString()
                            );
                            if (value > Float.MIN_VALUE && value <= Float.MAX_VALUE) {
                                numberConstant = new Constant((float) value);
                            } else if (value > Double.MIN_VALUE && value <= Double.MAX_VALUE) {
                                numberConstant = new Constant(value);
                            } else {
                                throw newSQLException(
                                        context,
                                        "Unsupported number." + value,
                                        context.currentTokenStartPosition()
                                );
                            }
                            context.toNextToken();
                        } else {
                            throw newSQLException(
                                    context,
                                    "Expect a number.",
                                    context.currentTokenStartPosition
                            );
                        }
                    } else {
                        final long value = Long.valueOf(currentTokenString);
                        if (value > Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
                            numberConstant = new Constant((int) value);
                        } else if (value > Long.MIN_VALUE && value <= Long.MAX_VALUE) {
                            numberConstant = new Constant(value);
                        } else {
                            throw newSQLException(
                                    context,
                                    "Unsupported number." + value,
                                    context.currentTokenStartPosition()
                            );
                        }
                    }
                    return numberConstant;
                case LETTERS:
                    currentTokenStartPosition = context.currentTokenStartPosition();
                    firstIdentifier = analyzeIdentifier(context);
                    if (firstIdentifier == null) {
                        throw newSQLException(
                                context,
                                "Expect an identifier.",
                                currentTokenStartPosition
                        );
                    }
                    if (context.currentTokenType() == TokenType.SYMBOL) {
                        switch (context.currentTokenToSymbol()) {
                            case '(':
                                // 函数
                                context.toNextToken();
                                final ArrayList<Expression> argumentExpressionList = context
                                        .getExpressionList();
                                final int startListIndex = argumentExpressionList
                                        .size();
                                boolean expectExpression = false;
                                for (; ; ) {
                                    final Expression expression
                                            = expectExpression ? analyzeExpectedExpression(context, Byte.MAX_VALUE)
                                            : analyzeExpression(context, Byte.MAX_VALUE);
                                    if (expression != null) {
                                        argumentExpressionList.add(expression);
                                        if (context.currentTokenType() == TokenType.SYMBOL
                                                && context.currentTokenToSymbol() == ',') {
                                            context.toNextToken();
                                            expectExpression = true;
                                            continue;
                                        }
                                    }
                                    break;
                                }
                                expectSymbol(context, ')');
                                // 括号匹配
                                context.toNextToken();
                                final Class<? extends Expression> functionClass
                                        = FunctionFactory.tryGetFunctionClass(firstIdentifier);
                                if (functionClass == null) {
                                    throw newSQLException("Function[" + firstIdentifier + "] is not supported now.");
                                }
                                try {
                                    return ExtensionExpressionFactory.newExtensionExpression(
                                            functionClass,
                                            expressionsListToArray(argumentExpressionList, startListIndex)
                                    );
                                } catch (Throwable t) {
                                    throw new SQLException(t.getMessage(), t);
                                }
                            case '.':
                                // 引用
                                context.toNextToken();
                                currentTokenStartPosition = context.currentTokenStartPosition();
                                final String secondIdentifier = analyzeGeneralizedIdentifier(context);
                                if (secondIdentifier == null) {
                                    if (context.currentTokenType() == TokenType.SYMBOL
                                            && context.currentTokenToSymbol() == '*') {
                                        context.toNextToken();
                                        return new Reference(firstIdentifier, Reference.ALL_COLUMN_IDENTIFIER);
                                    }
                                    throw newSQLException(context, "Expect an identifier.", currentTokenStartPosition);
                                }
                                return new Reference(firstIdentifier, secondIdentifier);
                        }
                    }
                    return new Reference(null, firstIdentifier);
                case SYMBOL:
                    final char symbol = context.currentTokenToSymbol();
                    final StringBuilder stringBuilder;
                    switch (symbol) {
                        case '?':
                            // 参数
                            context.toNextToken();
                            return context.newArgument();
                        case '+':
                            // 取正运算
                            context.toNextToken();
                            return new PositiveExpression(analyzeExpectedPrimitiveExpression(context));
                        case '-':
                            // 取负运算
                            context.toNextToken();
                            return new NegativeExpression(analyzeExpectedPrimitiveExpression(context));
                        case '*':
                            // 引用
                            context.toNextToken();
                            return new Reference((String) null, Reference.ALL_COLUMN_IDENTIFIER);
                        case '`':
                            currentTokenStartPosition = context.currentTokenStartPosition();
                            firstIdentifier = analyzeGeneralizedIdentifier(context);
                            if (firstIdentifier == null) {
                                throw newSQLException(
                                        context,
                                        "Expect an identifier.",
                                        currentTokenStartPosition
                                );
                            }
                            if (context.currentTokenType() == TokenType.SYMBOL
                                    && context.currentTokenToSymbol() == '.') {
                                currentTokenStartPosition = context.currentTokenStartPosition();
                                // 引用
                                context.toNextToken();
                                final String secondIdentifier = analyzeGeneralizedIdentifier(context);
                                if (secondIdentifier == null) {
                                    if (context.currentTokenType() == TokenType.SYMBOL
                                            && context.currentTokenToSymbol() == '*') {
                                        context.toNextToken();
                                        return new Reference(firstIdentifier, Reference.ALL_COLUMN_IDENTIFIER);
                                    }
                                    throw newSQLException(context,
                                            "Expect an identifier.",
                                            currentTokenStartPosition);
                                }
                                return new Reference(firstIdentifier, secondIdentifier);
                            }
                            return new Reference(null, firstIdentifier);
                        case '(':
                            // 优先运算
                            context.toNextToken();
                            final Expression expression = analyzeExpectedExpression(
                                    context, Byte.MAX_VALUE);
                            expectSymbol(context, ')');
                            // 括号匹配
                            context.toNextToken();
                            return new PreferentialExpression(expression);
                        case '"':
                        case '\'':
                            // 字符串
                            currentTokenStartPosition = context.currentTokenStartPosition();
                            context.toNextToken(symbol);
                            stringBuilder = context.getEmptyStringBuilder();
                            context.currentTokenTo(stringBuilder);
                            context.toNextToken();
                            switch (context.currentTokenType()) {
                                case EOF:
                                    throw newSQLException(context, null, null);
                                case SYMBOL:
                                    expectSymbol(context, symbol);
                                    context.toNextToken();
                                    if (context.currentTokenType() == TokenType.SYMBOL
                                            && context.currentTokenToSymbol() == '.') {
                                        firstIdentifier = stringBuilder.toString();
                                        checkIdentifier(firstIdentifier);
                                        context.toNextToken();
                                        return new Reference(firstIdentifier,
                                                analyzeGeneralizedIdentifier(context));
                                    } else {
                                        if (symbol == '\'' && stringBuilder.length() == 1) {
                                            return new Constant(String.valueOf(stringBuilder.charAt(0)));
                                        } else {
                                            return new Constant(stringBuilder.toString());
                                        }
                                    }
                                default:
                                    throw newSQLException(context,
                                            "Expect a string constant.",
                                            currentTokenStartPosition);
                            }
                        case '.':
                            context.toNextToken();
                            currentTokenStartPosition = context.currentTokenStartPosition;
                            if (context.currentTokenType == TokenType.NUMBERS) {
                                final double value = Double.parseDouble("." + context.currentTokenToString());
                                if (value > Float.MIN_VALUE && value <= Float.MAX_VALUE) {
                                    context.toNextToken();
                                    return new Constant((float) value);
                                } else if (value > Double.MIN_VALUE && value <= Double.MAX_VALUE) {
                                    context.toNextToken();
                                    return new Constant(value);
                                } else {
                                    throw newSQLException(
                                            context,
                                            "Expect a number.",
                                            currentTokenStartPosition
                                    );
                                }
                            }
                            break;
                        /*case '[':
                            // 引用
                            final char expectEndSymbol = ']';
                            currentTokenStartPosition = context.currentTokenStartPosition();
                            context.toNextToken(expectEndSymbol);
                            stringBuilder = context.getEmptyStringBuilder();
                            context.currentTokenTo(stringBuilder);
                            context.toNextToken();
                            switch (context.currentTokenType()) {
                                case EOF:
                                    throw newSQLException(context, null, null);
                                case SYMBOL:
                                    expectSymbol(context, expectEndSymbol);
                                    context.toNextToken();
                                    firstIdentifier = stringBuilder.toString();
                                    checkIdentifier(firstIdentifier);
                                    if (context.currentTokenType() == TokenType.SYMBOL
                                            && context.currentTokenToSymbol() == '.') {
                                        context.toNextToken();
                                        return new Reference(firstIdentifier, analyzeGeneralizedIdentifier(context));
                                    } else {
                                        return new Reference(null, firstIdentifier);
                                    }
                                default:
                                    throw newSQLException(
                                            context,
                                            "Expect a reference.",
                                            currentTokenStartPosition
                                    );
                            }*/
                        /*case '#':
                            // 日期
                            currentTokenStartPosition = context.currentTokenStartPosition();
                            context.toNextToken(symbol);
                            stringBuilder = context.getEmptyStringBuilder();
                            context.currentTokenTo(stringBuilder);
                            context.toNextToken();
                            switch (context.currentTokenType()) {
                                case EOF:
                                    throw newSQLException(context, null, null);
                                case SYMBOL:
                                    expectSymbol(context, symbol);
                                    // 引号匹配
                                    context.toNextToken();
                                    final String timestampString = stringBuilder.toString();
                                    try {
                                        return new Constant(
                                                new Date(
                                                        java.sql.Date.valueOf(timestampString).getTime()
                                                )
                                        );
                                    } catch (IllegalArgumentException e) {
                                        return new Constant(
                                                new Date(
                                                        Timestamp.valueOf(timestampString).getTime()
                                                )
                                        );
                                    }
                                default:
                                    throw newSQLException(
                                            context,
                                            "Expect a date constant.",
                                            currentTokenStartPosition
                                    );
                            }*/
                    }
                    break;
            }
            return null;
        }

        private static Expression analyzeExpectedExpression(
                final GrammarAnalyzeContext context,
                final byte expressionPriority) throws SQLException {
            final int currentTokenStartPosition = context
                    .currentTokenStartPosition();
            final Expression expression = analyzeExpression(context,
                    expressionPriority);
            if (expression == null) {
                throw newSQLException(context, "Expect an expression.",
                        currentTokenStartPosition);
            }
            return expression;
        }

        private static Expression analyzeExpectedPrimitiveExpression(
                final GrammarAnalyzeContext context) throws SQLException {
            final int currentTokenStartPosition = context
                    .currentTokenStartPosition();
            final Expression expression = analyzePrimitiveExpression(context);
            if (expression == null) {
                throw newSQLException(context, "Expect an expression.",
                        currentTokenStartPosition);
            }
            return expression;
        }

        private static Expression analyzeInExpression(
                final GrammarAnalyzeContext context,
                final Expression firstExpression) throws SQLException {
            expectSymbol(context, '(');
            context.toNextToken();
            final ArrayList<Expression> expressionList = context
                    .getExpressionList();
            final int startListIndex = expressionList.size();
            expressionList.add(firstExpression);
            boolean expectExpression = false;
            for (; ; ) {
                final Expression expression = expectExpression ? analyzeExpectedExpression(
                        context, (byte) 3) : analyzeExpression(context,
                        (byte) 3);
                if (expression != null) {
                    expressionList.add(expression);
                    if (context.currentTokenType() == TokenType.SYMBOL
                            && context.currentTokenToSymbol() == ',') {
                        context.toNextToken();
                        expectExpression = true;
                        continue;
                    }
                }
                break;
            }
            expectSymbol(context, ')');
            context.toNextToken();
            return new InExpression(expressionsListToArray(expressionList,
                    startListIndex));
        }

        private static Expression analyzeLikeExpression(
                final GrammarAnalyzeContext context,
                final Expression firstExpression) throws SQLException {
            return new LikeExpression(
                    firstExpression,
                    analyzeExpectedExpression(context, (byte) 3)
            );
        }

        private static Expression analyzeBetweenExpression(
                final GrammarAnalyzeContext context,
                final Expression firstExpression) throws SQLException {
            final Expression secondExpression = analyzeExpectedExpression(
                    context, (byte) 3); // level under AND expression
            expectKeyword(context, ReservedKeyword.AND);
            context.toNextToken();
            final Expression thirdExpression = analyzeExpectedExpression(
                    context, (byte) 3); // level under AND expression
            return new BetweenExpression(firstExpression, secondExpression,
                    thirdExpression);
        }

        private ExpressionGrammarAnalyzer() {
            // to do nothing.
        }

    }

}
