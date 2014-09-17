package org.codefamily.crabs.jdbc.engine.extension;

import org.codefamily.crabs.common.ExtensionClassCollector;
import org.codefamily.crabs.common.util.ReadonlyList;
import org.codefamily.crabs.core.DataType;
import org.codefamily.crabs.core.Identifier;
import org.codefamily.crabs.core.IndexDefinition;
import org.codefamily.crabs.core.TypeDefinition;
import org.codefamily.crabs.core.TypeDefinition.FieldDefinition;
import org.codefamily.crabs.core.client.AdvancedClient;
import org.codefamily.crabs.core.client.AdvancedClient.InternalDocumentRequestBuilder;
import org.codefamily.crabs.core.client.AdvancedClient.ResponseCallback;
import org.codefamily.crabs.core.exception.FieldNotExistsException;
import org.codefamily.crabs.core.exception.IndexNotExistsException;
import org.codefamily.crabs.core.exception.TypeNotExistsException;
import org.codefamily.crabs.exception.SQL4ESException;
import org.codefamily.crabs.jdbc.engine.ExecuteEnvironment;
import org.codefamily.crabs.jdbc.engine.SemanticAnalyzer;
import org.codefamily.crabs.jdbc.engine.StatementExecutePlan;
import org.codefamily.crabs.jdbc.engine.StatementExecutor;
import org.codefamily.crabs.jdbc.internal.InternalResultSet;
import org.codefamily.crabs.jdbc.lang.Expression;
import org.codefamily.crabs.jdbc.lang.expression.*;
import org.codefamily.crabs.jdbc.lang.extension.clause.FromClause.SimpleTableDeclare;
import org.codefamily.crabs.jdbc.lang.extension.clause.*;
import org.codefamily.crabs.jdbc.lang.extension.clause.OrderByClause.OrderSpecification;
import org.codefamily.crabs.jdbc.lang.extension.clause.SelectClause.ResultColumnDeclare;
import org.codefamily.crabs.jdbc.lang.extension.expression.*;
import org.codefamily.crabs.jdbc.lang.extension.statement.SelectStatement;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.codefamily.crabs.jdbc.Protocol.*;

public final class SelectStatementExecutor extends StatementExecutor<SelectStatement, InternalResultSet> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SelectStatementExecutor.class);

    public SelectStatementExecutor() {
        super(SelectStatement.class, InternalResultSet.class);
    }

    @Override
    protected final InternalResultSet execute(final AdvancedClient advancedClient,
                                              final SelectStatement statement,
                                              final ExecuteEnvironment environment,
                                              final Object[] argumentValues) throws SQL4ESException {
        final SearchExecuteContext context = new SearchExecuteContext(statement, environment, argumentValues);
        final SelectStatementExecutePlan statementExecutePlan = SelectStatementExecutePlan.buildExecutePlan(context);
        final InternalDocumentRequestBuilder requestBuilder = statementExecutePlan.createRequestBuilder();
        final SearchResponseCallback callback = statementExecutePlan.callback();
        // 以下是性能测试考虑
        final boolean benchmarkEnabled = Boolean.parseBoolean(
                environment.getProperty(
                        PROPERTY_ENTRY$BENCHMARK_ENABLED.identifier,
                        PROPERTY_ENTRY$BENCHMARK_ENABLED.defaultValue
                )
        );
        LOGGER.info("benchmarkEnabled: " + benchmarkEnabled);
        if (benchmarkEnabled) {
            environment.start();
        }
        advancedClient.execute(requestBuilder, callback, context);
        if (benchmarkEnabled) {
            environment.end();
        }
        return callback.getResultSet();
    }

    private static abstract class SelectStatementExecutePlan<RequestBuilder extends InternalDocumentRequestBuilder,
            Callback extends SearchResponseCallback> extends StatementExecutePlan {

        static SelectStatementExecutePlan buildExecutePlan(final SearchExecuteContext context) throws SQL4ESException {
            for (int index = 0, size = REGISTERED_EXECUTE_PLAN_CLASSES.size(); index < size; index++) {
                final Class<? extends SelectStatementExecutePlan> clazz = REGISTERED_EXECUTE_PLAN_CLASSES.get(index);
                final Constructor<? extends SelectStatementExecutePlan> constructor;
                try {
                    constructor = clazz.getDeclaredConstructor(SearchExecuteContext.class);
                } catch (NoSuchMethodException e) {
                    // nothing to do because the register method has already ensured the point.
                    continue;
                }
                final SelectStatementExecutePlan statementExecutePlan;
                try {
                    statementExecutePlan = constructor.newInstance(context);
                } catch (Exception e) {
                    throw new SQL4ESException(
                            "Failed to build execute plan for sql[" + context.statement + "]",
                            e
                    );
                }
                if (statementExecutePlan.accept()) {
                    return statementExecutePlan;
                }
            }
            throw new SQL4ESException(
                    "There's no proper execution plan generator for sql[" +
                            context.statement + "], maybe the sql is not supported now."
            );
        }

        private static final ArrayList<Class<? extends SelectStatementExecutePlan>> REGISTERED_EXECUTE_PLAN_CLASSES
                = new ArrayList<Class<? extends SelectStatementExecutePlan>>(3);

        // 用于去重
        private static final HashSet<Class<? extends SelectStatementExecutePlan>> REGISTERED_EXECUTE_PLAN_CLASS_SET
                = new HashSet<Class<? extends SelectStatementExecutePlan>>(3);

        static {
            final Iterator<Class<? extends SelectStatementExecutePlan>> iterator
                    = ExtensionClassCollector.getExtensionClasses(SelectStatementExecutePlan.class);
            while (iterator.hasNext()) {
                registerStatementExecutePlan(iterator.next());
            }
        }

        protected final SearchExecuteContext context;

        protected final boolean outputSQLToElasticsearchRequestMapping;

        protected SelectStatementExecutePlan(final SearchExecuteContext context) {
            this.context = context;
            this.outputSQLToElasticsearchRequestMapping = Boolean.parseBoolean(
                    context.environment.getProperty(
                            PROPERTY_ENTRY$OUTPUT_SQL_ES_MAPPING.identifier,
                            PROPERTY_ENTRY$OUTPUT_SQL_ES_MAPPING.defaultValue
                    )
            );
        }

        private RequestBuilder requestBuilder;

        public final InternalDocumentRequestBuilder createRequestBuilder() throws SQL4ESException {
            if (this.requestBuilder == null) {
                this.requestBuilder = this.doCreateRequestBuilder();
            }
            return this.requestBuilder;
        }

        private Callback callback;

        public final Callback callback() throws SQL4ESException {
            if (this.callback == null) {
                this.callback = this.doCallback();
            }
            return this.callback;
        }

        protected abstract boolean accept() throws SQL4ESException;

        protected abstract RequestBuilder doCreateRequestBuilder() throws SQL4ESException;

        protected abstract Callback doCallback() throws SQL4ESException;

        protected FilterBuilder buildFilterBuilder(final Expression expression) throws SQL4ESException {
            if (expression instanceof PreferentialExpression) {
                final PreferentialExpression realExpression = ((PreferentialExpression) expression);
                return this.buildFilterBuilder(realExpression.getOperandExpression(0));
            } else if (expression instanceof AndExpression) {
                final ReadonlyList<Expression> operandExpressionList = expression.getOperandExpressionList();
                final int size = operandExpressionList.size();
                FilterBuilder[] filterBuilders = new FilterBuilder[size];
                for (int index = 0; index < size; index++) {
                    filterBuilders[index] = this.buildFilterBuilder(operandExpressionList.get(index));
                }
                return FilterBuilders.andFilter(filterBuilders);
            } else if (expression instanceof OrExpression) {
                final ReadonlyList<Expression> operandExpressionList = expression.getOperandExpressionList();
                final int size = operandExpressionList.size();
                FilterBuilder[] filterBuilders = new FilterBuilder[size];
                for (int index = 0; index < size; index++) {
                    filterBuilders[index] = this.buildFilterBuilder(operandExpressionList.get(index));
                }
                return FilterBuilders.orFilter(filterBuilders);
            } else if (expression instanceof NotExpression) {
                return FilterBuilders.notFilter(
                        this.buildFilterBuilder(((NotExpression) expression).getOperandExpression(0))
                );
            } else {
                if (expression instanceof GreaterThanExpression) {
                    final GreaterThanExpression realExpression = (GreaterThanExpression) expression;
                    Expression operandOne = realExpression.getOperandExpression(0);
                    Expression operandTwo = realExpression.getOperandExpression(1);
                    if (operandOne instanceof Argument) {
                        operandOne = new Constant(
                                this.context.argumentValue((Argument) operandOne)
                        );
                    }
                    if (operandTwo instanceof Argument) {
                        operandTwo = new Constant(
                                this.context.argumentValue((Argument) operandTwo)
                        );
                    }
                    if (operandOne instanceof Constant) {
                        if (!(operandTwo instanceof Reference)) {
                            throw new SQL4ESException("Unsupported expression[" + realExpression + "]");
                        }
                        final Reference reference = Reference.class.cast(operandTwo);
                        final Constant constant = Constant.class.cast(operandOne);
                        return FilterBuilders
                                .rangeFilter(reference.columnIdentifier.toString())
                                .lte(this.parseConstantValue(constant, reference));
                    }
                    if (operandTwo instanceof Constant) {
                        if (!(operandOne instanceof Reference)) {
                            throw new SQL4ESException("Unsupported expression[" + realExpression + "]");
                        }
                        final Reference reference = Reference.class.cast(operandOne);
                        final Constant constant = Constant.class.cast(operandTwo);
                        return FilterBuilders
                                .rangeFilter(reference.columnIdentifier.toString())
                                .gt(this.parseConstantValue(constant, reference));
                    }
                    throw new SQL4ESException("Unsupported expression[" + realExpression + "]");

                } else if (expression instanceof GreaterThanOrEqualToExpression) {
                    final GreaterThanOrEqualToExpression realExpression = (GreaterThanOrEqualToExpression) expression;
                    Expression operandOne = realExpression.getOperandExpression(0);
                    Expression operandTwo = realExpression.getOperandExpression(1);
                    if (operandOne instanceof Argument) {
                        operandOne = new Constant(
                                this.context.argumentValue((Argument) operandOne)
                        );
                    }
                    if (operandTwo instanceof Argument) {
                        operandTwo = new Constant(
                                this.context.argumentValue((Argument) operandTwo)
                        );
                    }
                    if (operandOne instanceof Constant) {
                        if (!(operandTwo instanceof Reference)) {
                            throw new SQL4ESException("Unsupported expression[" + realExpression + "]");
                        }
                        final Reference reference = Reference.class.cast(operandTwo);
                        final Constant constant = Constant.class.cast(operandOne);
                        return FilterBuilders
                                .rangeFilter(reference.columnIdentifier.toString())
                                .lt(this.parseConstantValue(constant, reference));
                    }
                    if (operandTwo instanceof Constant) {
                        if (!(operandOne instanceof Reference)) {
                            throw new SQL4ESException("Unsupported expression[" + realExpression + "]");
                        }
                        final Reference reference = Reference.class.cast(operandOne);
                        final Constant constant = Constant.class.cast(operandTwo);
                        return FilterBuilders
                                .rangeFilter(reference.columnIdentifier.toString())
                                .gte(this.parseConstantValue(constant, reference));
                    }
                    throw new SQL4ESException("Unsupported expression[" + realExpression + "]");

                } else if (expression instanceof LessThanExpression) {
                    final LessThanExpression realExpression = (LessThanExpression) expression;
                    Expression operandOne = realExpression.getOperandExpression(0);
                    Expression operandTwo = realExpression.getOperandExpression(1);
                    if (operandOne instanceof Argument) {
                        operandOne = new Constant(
                                this.context.argumentValue((Argument) operandOne)
                        );
                    }
                    if (operandTwo instanceof Argument) {
                        operandTwo = new Constant(
                                this.context.argumentValue((Argument) operandTwo)
                        );
                    }
                    if (operandOne instanceof Constant) {
                        if (!(operandTwo instanceof Reference)) {
                            throw new SQL4ESException("Unsupported expression[" + realExpression + "]");
                        }
                        final Reference reference = Reference.class.cast(operandTwo);
                        final Constant constant = Constant.class.cast(operandOne);
                        return FilterBuilders
                                .rangeFilter(reference.columnIdentifier.toString())
                                .gte(this.parseConstantValue(constant, reference));
                    }
                    if (operandTwo instanceof Constant) {
                        if (!(operandOne instanceof Reference)) {
                            throw new SQL4ESException("Unsupported expression[" + realExpression + "]");
                        }
                        final Reference reference = Reference.class.cast(operandOne);
                        final Constant constant = Constant.class.cast(operandTwo);
                        return FilterBuilders
                                .rangeFilter(reference.columnIdentifier.toString())
                                .lt(this.parseConstantValue(constant, reference));
                    }
                    throw new SQL4ESException("Unsupported expression[" + realExpression + "]");

                } else if (expression instanceof LessThanOrEqualToExpression) {
                    final LessThanOrEqualToExpression realExpression = (LessThanOrEqualToExpression) expression;
                    Expression operandOne = realExpression.getOperandExpression(0);
                    Expression operandTwo = realExpression.getOperandExpression(1);
                    if (operandOne instanceof Argument) {
                        operandOne = new Constant(
                                this.context.argumentValue((Argument) operandOne)
                        );
                    }
                    if (operandTwo instanceof Argument) {
                        operandTwo = new Constant(
                                this.context.argumentValue((Argument) operandTwo)
                        );
                    }
                    if (operandOne instanceof Constant) {
                        if (!(operandTwo instanceof Reference)) {
                            throw new SQL4ESException("Unsupported expression[" + realExpression + "]");
                        }
                        final Reference reference = Reference.class.cast(operandTwo);
                        final Constant constant = Constant.class.cast(operandOne);
                        return FilterBuilders
                                .rangeFilter(reference.columnIdentifier.toString())
                                .gt(this.parseConstantValue(constant, reference));

                    } else if (operandTwo instanceof Constant) {
                        if (!(operandOne instanceof Reference)) {
                            throw new SQL4ESException("Unsupported expression[" + realExpression + "]");
                        }
                        final Reference reference = Reference.class.cast(operandOne);
                        final Constant constant = Constant.class.cast(operandTwo);
                        return FilterBuilders
                                .rangeFilter(reference.columnIdentifier.toString())
                                .lte(this.parseConstantValue(constant, reference));
                    }
                    throw new SQL4ESException("Unsupported expression[" + realExpression + "]");

                } else if (expression instanceof EqualToExpression) {
                    final EqualToExpression realExpression = (EqualToExpression) expression;
                    Expression operandOne = realExpression.getOperandExpression(0);
                    Expression operandTwo = realExpression.getOperandExpression(1);
                    if (operandOne instanceof Argument) {
                        operandOne = new Constant(
                                this.context.argumentValue((Argument) operandOne)
                        );
                    }
                    if (operandTwo instanceof Argument) {
                        operandTwo = new Constant(
                                this.context.argumentValue((Argument) operandTwo)
                        );
                    }
                    if (operandOne instanceof Constant) {
                        if (!(operandTwo instanceof Reference)) {
                            throw new SQL4ESException("Unsupported expression[" + realExpression + "]");
                        }
                        final Reference reference = Reference.class.cast(operandTwo);
                        final Constant constant = Constant.class.cast(operandOne);
                        final FieldDefinition fieldDefinition
                                = this.context.typeDefinition.getFieldDefinition(reference.columnIdentifier);
                        if (fieldDefinition.isPrimaryField()) {
                            return FilterBuilders
                                    .idsFilter(this.context.typeDefinition.getIdentifier().toString())
                                    .ids(this.parseConstantValue(constant, reference).toString());
                        } else {
                            return FilterBuilders.termFilter(
                                    reference.columnIdentifier.toString(),
                                    this.parseConstantValue(constant, reference)
                            );
                        }
                    }
                    if (operandTwo instanceof Constant) {
                        if (!(operandOne instanceof Reference)) {
                            throw new SQL4ESException("Unsupported expression[" + realExpression + "]");
                        }
                        final Reference reference = Reference.class.cast(operandOne);
                        final Identifier columnIdentifier = reference.columnIdentifier;
                        final FieldDefinition fieldDefinition
                                = this.context.typeDefinition.getFieldDefinition(columnIdentifier);
                        final Constant constant = Constant.class.cast(operandTwo);
                        if (fieldDefinition.isPrimaryField()) {
                            return FilterBuilders
                                    .idsFilter(this.context.typeDefinition.getIdentifier().toString())
                                    .ids(this.parseConstantValue(constant, reference).toString());
                        } else {
                            return FilterBuilders.termFilter(
                                    columnIdentifier.toString(),
                                    this.parseConstantValue(constant, reference)
                            );
                        }
                    }
                    throw new SQL4ESException("Unsupported expression[" + realExpression + "]");

                } else if (expression instanceof UnequalToExpression) {
                    final UnequalToExpression realExpression = (UnequalToExpression) expression;
                    Expression operandOne = realExpression.getOperandExpression(0);
                    Expression operandTwo = realExpression.getOperandExpression(1);
                    if (operandOne instanceof Argument) {
                        operandOne = new Constant(
                                this.context.argumentValue((Argument) operandOne)
                        );
                    }
                    if (operandTwo instanceof Argument) {
                        operandTwo = new Constant(
                                this.context.argumentValue((Argument) operandTwo)
                        );
                    }
                    if (operandOne instanceof Constant) {
                        if (!(operandTwo instanceof Reference)) {
                            throw new SQL4ESException("Unsupported expression[" + realExpression + "]");
                        }
                        final Reference reference = Reference.class.cast(operandTwo);
                        final Identifier columnIdentifier = reference.columnIdentifier;
                        final FieldDefinition fieldDefinition
                                = this.context.typeDefinition.getFieldDefinition(columnIdentifier);
                        final Constant constant = Constant.class.cast(operandOne);
                        if (fieldDefinition.isPrimaryField()) {
                            return FilterBuilders.notFilter(
                                    FilterBuilders
                                            .idsFilter(this.context.typeDefinition.getIdentifier().toString())
                                            .ids(this.parseConstantValue(constant, reference).toString())
                            );
                        } else {
                            return FilterBuilders.notFilter(
                                    FilterBuilders.termFilter(
                                            columnIdentifier.toString(),
                                            this.parseConstantValue(constant, reference)
                                    )
                            );
                        }
                    }
                    if (operandTwo instanceof Constant) {
                        if (!(operandOne instanceof Reference)) {
                            throw new SQL4ESException("Unsupported expression[" + realExpression + "]");
                        }
                        final Reference reference = Reference.class.cast(operandOne);
                        final Identifier columnIdentifier = reference.columnIdentifier;
                        final FieldDefinition fieldDefinition
                                = this.context.typeDefinition.getFieldDefinition(columnIdentifier);
                        final Constant constant = Constant.class.cast(operandTwo);
                        if (fieldDefinition.isPrimaryField()) {
                            return FilterBuilders.notFilter(
                                    FilterBuilders
                                            .idsFilter(this.context.typeDefinition.getIdentifier().toString())
                                            .ids(this.parseConstantValue(constant, reference).toString())
                            );
                        } else {
                            return FilterBuilders.notFilter(
                                    FilterBuilders.termFilter(
                                            columnIdentifier.toString(),
                                            this.parseConstantValue(constant, reference)
                                    )
                            );
                        }
                    }
                    throw new SQL4ESException("Unsupported expression[" + realExpression + "]");

                } else if (expression instanceof BetweenExpression) {
                    final BetweenExpression realExpression = (BetweenExpression) expression;
                    Expression operandOne = realExpression.getOperandExpression(0);
                    Expression operandTwo = realExpression.getOperandExpression(1);
                    Expression operandThree = realExpression.getOperandExpression(2);
                    if (!(operandOne instanceof Reference)) {
                        throw new SQL4ESException("Unsupported expression[" + operandOne +
                                "] in BetweenExpression[" + realExpression + "].");
                    }
                    if (operandTwo instanceof Argument) {
                        operandTwo = new Constant(
                                this.context.argumentValue((Argument) operandTwo)
                        );
                    }
                    if (operandThree instanceof Argument) {
                        operandThree = new Constant(
                                this.context.argumentValue((Argument) operandThree)
                        );
                    }
                    if (!(operandTwo instanceof Constant && operandThree instanceof Constant)) {
                        throw new SQL4ESException("Unsupported expression[" + realExpression + "]");
                    }
                    final Reference reference = Reference.class.cast(operandOne);
                    final Constant constantOne = Constant.class.cast(operandTwo);
                    final Constant constantTwo = Constant.class.cast(operandThree);
                    return FilterBuilders
                            .rangeFilter(reference.columnIdentifier.toString())
                            .from(this.parseConstantValue(constantOne, reference))
                            .to(this.parseConstantValue(constantTwo, reference));
                } else if (expression instanceof InExpression) {
                    final InExpression realExpression = InExpression.class.cast(expression);
                    Expression operandOne = realExpression.getOperandExpression(0);
                    if (!(operandOne instanceof Reference)) {
                        throw new SQL4ESException("Unsupported expression[" + expression +
                                "] in InExpression[" + realExpression + "]");
                    }
                    final Reference reference = Reference.class.cast(operandOne);
                    final int size = realExpression.expressionCountInSet;
                    final Object[] values = new Object[size];
                    Expression operandExpression;
                    for (int index = 0; index < size; index++) {
                        operandExpression = realExpression.getOperandExpression(index + 1);
                        if (operandExpression instanceof Argument) {
                            values[index] = this.parseConstantValue(
                                    new Constant(this.context.argumentValue((Argument) operandExpression)),
                                    reference
                            );
                        } else if (operandExpression instanceof Constant) {
                            values[index] = this.parseConstantValue(
                                    (Constant) operandExpression,
                                    reference
                            );
                        } else {
                            throw new SQL4ESException("Unsupported expression[" + operandExpression +
                                    "] in InExpression[" + realExpression + "]");
                        }
                    }
                    final FieldDefinition fieldDefinition
                            = this.context.typeDefinition.getFieldDefinition(reference.columnIdentifier);
                    if (fieldDefinition.isPrimaryField()) {
                        final String[] ids = new String[size];
                        for (int index = 0; index < size; index++) {
                            ids[index] = values[index].toString();
                        }
                        return FilterBuilders
                                .idsFilter(this.context.tableIdentifier.toString())
                                .ids(ids);
                    } else {
                        return FilterBuilders.termsFilter(reference.columnIdentifier.toString(), values);
                    }
                } else if (expression instanceof LikeExpression) {
                    final LikeExpression realExpression = (LikeExpression) expression;
                    final Expression operandOne = realExpression.getOperandExpression(0);
                    Expression operandTwo = realExpression.getOperandExpression(1);
                    if (!(operandOne instanceof Reference)) {
                        throw new SQL4ESException("Unsupported expression[" + operandOne +
                                "] in LikeExpression[" + realExpression + "]");
                    }
                    final String value;
                    if (operandTwo instanceof Argument) {
                        value = this.context.argumentValue((Argument) operandTwo).toString();
                    } else if (operandTwo instanceof Constant) {
                        value = ((Constant) operandTwo).value.toString();
                    } else {
                        throw new SQL4ESException("Unsupported expression[" + operandTwo +
                                "] in LikeExpression[" + realExpression + "]");
                    }
                    final char[] characters = value.toCharArray();
                    final int size = characters.length;
                    final StringBuilder builder = new StringBuilder(size * 2);
                    char character;
                    int percentCount = 0;
                    int percentIndex = 0;
                    for (int index = 0; index < size; index++) {
                        character = characters[index];
                        switch (character) {
                            case '%':
                                percentCount++;
                                percentIndex = index;
                                builder.append(".*");
                                for (int length = size - 1; index < length; ) {
                                    if (characters[++index] != '%') {
                                        index--;
                                        break;
                                    }
                                }
                                break;
                            case '\\':
                                if (characters[++index] == '%') {
                                    builder.append("%");
                                } else {
                                    builder.append("\\\\");
                                    index--;
                                }
                                break;
                            case '.':
                                builder.append("\\.");
                                break;
                            case '?':
                                builder.append("\\?");
                                break;
                            case '+':
                                builder.append("\\+");
                                break;
                            case '*':
                                builder.append("\\*");
                                break;
                            case '|':
                                builder.append("\\|");
                                break;
                            case '{':
                                builder.append("\\{");
                                break;
                            case '}':
                                builder.append("\\}");
                                break;
                            case '[':
                                builder.append("\\[");
                                break;
                            case ']':
                                builder.append("\\]");
                                break;
                            case '(':
                                builder.append("\\(");
                                break;
                            case ')':
                                builder.append("\\)");
                                break;
                            case '"':
                                builder.append("\\\"");
                                break;
                            case '^':
                                builder.append("\\^");
                                break;
                            case '$':
                                builder.append("\\$");
                                break;
                            default:
                                builder.append(character);
                        }
                    }
                    if (percentCount == 1 && value.endsWith("%")) {
                        final char[] destination = new char[percentIndex];
                        System.arraycopy(characters, 0, destination, 0, percentIndex);
                        return FilterBuilders.prefixFilter(
                                ((Reference) operandOne).columnIdentifier.toString(),
                                new String(destination)
                        );
                    } else {
                        return FilterBuilders
                                .regexpFilter(
                                        ((Reference) operandOne).columnIdentifier.toString(),
                                        builder.toString()
                                ).flags(RegexpFlag.NONE);
                    }
                } else {
                    throw new SQL4ESException("Unsupported expression[" + expression + "]");
                }
            }
        }

        private static long SRC_TIME_ZONE_RAW_OFFSET = TimeZone.getDefault().getRawOffset();

        private static long TARGET_TIME_ZONE_RAW_OFFSET = TimeZone.getTimeZone("UTC").getRawOffset();

        private Object parseConstantValue(final Constant constant,
                                          final Reference reference) throws SQL4ESException {
            final Identifier columnIdentifier = reference.columnIdentifier;
            final FieldDefinition columnDefinition
                    = this.context.typeDefinition.getFieldDefinition(columnIdentifier);
            if (columnDefinition.getDataType() == DataType.DATE) {
                final String pattern = columnDefinition.getPattern();
                switch (constant.dataType) {
                    case STRING:
                        final SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
                        final String value = constant.value.toString();
                        try {
                            dateFormat.parse(value);
                        } catch (ParseException e) {
                            throw new SQL4ESException("Failed to parse constant[" +
                                    value + "] to date, expect its format[" +
                                    pattern + "]");
                        }
                        return value;
                    case DATE:
                        return ((Date) constant.value).getTime()
                                + SRC_TIME_ZONE_RAW_OFFSET - TARGET_TIME_ZONE_RAW_OFFSET;
                    default:
                        throw new SQL4ESException("Failed to parse constant[" +
                                constant.value + "] to date, expect date or date format string[" +
                                pattern + "]");
                }
            }
            return constant.value;
        }

        private static void registerStatementExecutePlan(final Class<? extends SelectStatementExecutePlan> clazz) {
            synchronized (SelectStatementExecutor.class) {
                if (REGISTERED_EXECUTE_PLAN_CLASS_SET.contains(clazz)) {
                    return;
                }
                try {
                    clazz.getDeclaredConstructor(SearchExecuteContext.class);
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException(
                            "Invalid select statement execute plan, " +
                                    "it must contains a constructor with only one parameter " +
                                    "and the parameter type is " + SearchExecuteContext.class +
                                    " or its child class."
                    );
                }
                REGISTERED_EXECUTE_PLAN_CLASSES.add(clazz);
                REGISTERED_EXECUTE_PLAN_CLASS_SET.add(clazz);
            }
        }
    }

    private static abstract class SearchResponseCallback<ResultSet extends InternalResultSet>
            implements ResponseCallback<SearchResponse> {

        public abstract ResultSet getResultSet() throws SQL4ESException;

        @Override
        public abstract void callback(SearchResponse response) throws SQL4ESException;
    }

    private static class SearchExecuteContext {

        final SelectStatement statement;

        final ExecuteEnvironment environment;

        final int maxRowCount;

        final IndexDefinition indexDefinition;

        private final ArgumentValues argumentValues;

        Identifier tableIdentifier;

        Identifier tableAlias;

        TypeDefinition typeDefinition;

        Expression finallyWhereConditionExpression;

        protected SearchExecuteContext(final SelectStatement statement,
                                       final ExecuteEnvironment environment,
                                       final Object[] argumentValues) throws SQL4ESException {
            this(statement, environment, new ArgumentValues(argumentValues));
        }

        protected SearchExecuteContext(final SearchExecuteContext context) throws SQL4ESException {
            this(context.statement, context.environment, context.argumentValues);
        }

        private SearchExecuteContext(final SelectStatement statement,
                                     final ExecuteEnvironment environment,
                                     final ArgumentValues values) throws SQL4ESException {
            this.statement = statement;
            this.environment = environment;
            this.argumentValues = values;
            this.maxRowCount = Integer.parseInt(
                    this.environment.getProperty(
                            PROPERTY_ENTRY$SCAN_SIZE.identifier,
                            PROPERTY_ENTRY$SCAN_SIZE.defaultValue
                    )
            );
            this.indexDefinition = this.environment.getIndexDefinition();
        }

        final Object argumentValue(final Argument argument) throws SQL4ESException {
            return this.argumentValues.argumentValue(argument);
        }

        private static final class ArgumentValues {

            private final Object[] values;

            private final int argumentValueCount;

            ArgumentValues(final Object[] values) {
                this.values = values;
                this.argumentValueCount = values.length;
            }

            final Object argumentValue(final Argument argument) throws SQL4ESException {
                this.checkArgumentIndex(argument);
                return this.values[argument.index];
            }

            private void checkArgumentIndex(final Argument argument) throws SQL4ESException {
                final int index = argument.index;
                if (index >= this.argumentValueCount) {
                    throw new SQL4ESException("There's no value assigned to the argument[" + index + "]");
                }
            }

        }

    }

    private static abstract class SelectStatementSemanticAnalyzer<Context extends SearchExecuteContext>
            extends SemanticAnalyzer {

        protected final Context context;

        protected final SelectStatement statement;

        protected SelectStatementSemanticAnalyzer(final Context context) throws SQL4ESException {
            this.context = context;
            this.statement = this.context.statement;
            // TODO 当前只考虑单表场景
            final SimpleTableDeclare tableDeclare
                    = (SimpleTableDeclare) this.statement.fromClause.tableDeclareList.get(0);
            try {
                this.context.typeDefinition
                        = this.context.environment.getTypeDefinition(tableDeclare.tableIdentifier);
            } catch (SQL4ESException e) {
                final Throwable cause = e.getCause();
                if (cause instanceof TypeNotExistsException) {
                    throw new SQL4ESException("Table[" + tableDeclare.tableIdentifier +
                            "] is not found in Database[" +
                            this.context.indexDefinition.getIdentifier() + "]", cause);
                }
                if (cause instanceof IndexNotExistsException) {
                    throw new SQL4ESException("Database[" + this.context.indexDefinition.getIdentifier() +
                            "] is not exists.", cause);
                }
                throw e;
            }
            this.context.tableIdentifier = this.context.typeDefinition.getIdentifier();
            this.context.tableAlias = tableDeclare.alias;
        }

        public abstract void analyzeStatement() throws SQL4ESException;

        protected void analyzeWhereClause() throws SQL4ESException {
            final WhereClause whereClause = this.statement.whereClause;
            if (whereClause == null) {
                this.context.finallyWhereConditionExpression = null;
                return;
            }
            final Expression originalConditionExpression = whereClause.conditionExpression;
            if (originalConditionExpression.getResultType() != DataType.BOOLEAN) {
                throw new SQL4ESException(
                        "Invalid expression in where clause, only allow boolean expression."
                );
            }
            this.context.finallyWhereConditionExpression
                    = this.analyzeExpressionInWhereClause(originalConditionExpression);
        }

        private Expression analyzeExpressionInWhereClause(final Expression expression) throws SQL4ESException {
            if (expression instanceof Reference) {
                final Reference reference = Reference.class.cast(expression);
                if (reference.columnIdentifier.equals(Reference.ALL_COLUMN_IDENTIFIER)) {
                    throw new SQL4ESException("Can not contain * reference in where clause.");
                }
                try {
                    this.context.typeDefinition.getFieldDefinition(reference.columnIdentifier);
                } catch (FieldNotExistsException e) {
                    throw new SQL4ESException(
                            "Column[" + reference.columnIdentifier + "] is not found in table[" +
                                    this.context.tableIdentifier + "]"
                    );
                }
                if (!(reference.setIdentifier == null
                        || reference.setIdentifier.equals(this.context.tableAlias)
                        || reference.setIdentifier.equals(this.context.tableIdentifier))) {
                    throw new SQL4ESException(
                            "Unknown data source identifier[" + reference.setIdentifier + "]"
                    );
                }
                return expression;
            } else if (expression instanceof NonAggregation) {
                final ReadonlyList<Expression> operandExpressionList = expression.getOperandExpressionList();
                if (!operandExpressionList.isEmpty()) {
                    for (int index = 0, size = operandExpressionList.size(); index < size; index++) {
                        analyzeExpressionInWhereClause(operandExpressionList.get(index));
                    }
                } else {
                    return expression;
                }
            } else if (expression == Null.INSTANCE) {
                throw new SQL4ESException("Can not contain null in where clause.");
            } else if (expression instanceof Aggregation) {
                throw new SQL4ESException("Can not contain aggregation in where clause.");
            }
            return expression;
        }

    }

    static final class SearchResultSet extends InternalResultSet {

        static final class SearchResultSetMetaData extends InternalMetaData {

            static final class ColumnInformation {

                ColumnInformation(final Identifier identifier,
                                  final String label,
                                  final DataType dataType,
                                  final int displaySize) {
                    this.identifier = identifier;
                    this.label = label;
                    this.dataType = dataType;
                    this.displaySize = displaySize;
                }

                final Identifier identifier;

                final String label;

                final DataType dataType;

                final int displaySize;

                @Override
                public final boolean equals(final Object obj) {
                    if (obj != null && obj instanceof ColumnInformation) {
                        final ColumnInformation that = (ColumnInformation) obj;
                        return this.identifier.equals(that.identifier)
                                && this.label.equals(that.label)
                                && this.dataType == that.dataType
                                && this.displaySize == that.displaySize;
                    }
                    return false;
                }
            }

            SearchResultSetMetaData(final ColumnInformation[] columnInformations) {
                final HashMap<Identifier, Integer> columnIndexMap = new HashMap<Identifier, Integer>();
                for (int i = 0; i < columnInformations.length; i++) {
                    final ColumnInformation columnInformation = columnInformations[i];
                    columnIndexMap.put(columnInformation.identifier, i);
                }
                this.columnInformations = columnInformations;
                this.columnIndexMap = columnIndexMap;
            }

            private final ColumnInformation[] columnInformations;

            private final HashMap<Identifier, Integer> columnIndexMap;

            @Override
            public final int getColumnCount() {
                return this.columnInformations.length;
            }

            @Override
            public final int getColumnIndex(final Identifier columnIdentifier) {
                if (columnIdentifier == null) {
                    throw new IllegalArgumentException("Argument[columnIdentifier] is null.");
                }
                final Integer columnIndex = this.columnIndexMap.get(columnIdentifier);
                return columnIndex == null ? -1 : columnIndex;
            }

            @Override
            public final Identifier getColumnIdentifier(final int columnIndex) {
                return this.columnInformations[columnIndex].identifier;
            }

            @Override
            public final String getColumnLabel(final int columnIndex) {
                return this.columnInformations[columnIndex].label;
            }

            @Override
            public final DataType getColumnValueType(final int columnIndex) {
                return this.columnInformations[columnIndex].dataType;
            }

            @Override
            public final int getColumnDisplaySize(final int columnIndex) {
                return this.columnInformations[columnIndex].displaySize;
            }

            @Override
            public final boolean equals(final Object obj) {
                if (obj != null && obj instanceof SearchResultSetMetaData) {
                    final SearchResultSetMetaData that = (SearchResultSetMetaData) obj;
                    if (this.getColumnCount() == that.getColumnCount()) {
                        for (int index = 0, count = this.getColumnCount(); index < count; index++) {
                            if (!this.columnInformations[index].equals(that.columnInformations[index])) {
                                return false;
                            }
                        }
                        return true;
                    }
                    return false;
                }
                return false;
            }
        }

        SearchResultSet(final SearchResultSetMetaData metaData,
                        final SearchResultSetIterator searchResultSetIterator) {
            this.metaData = metaData;
            this.searchResultSetIterator = searchResultSetIterator;
        }

        private final SearchResultSetMetaData metaData;

        private final SearchResultSetIterator searchResultSetIterator;

        private Object[] resultValues;

        @Override
        public final InternalMetaData getMetaData() {
            return this.metaData;
        }

        @Override
        public final Object getColumnValue(final Identifier columnIdentifier) {
            return this.resultValues[this.metaData.getColumnIndex(columnIdentifier)];
        }

        @Override
        public final Object getColumnValue(final int index) {
            return this.resultValues[index];
        }

        @Override
        public final boolean next() throws SQL4ESException {
            final boolean haveNext = this.searchResultSetIterator.next();
            if (haveNext) {
                final SearchResultSetIterator searchResultSetIterator = this.searchResultSetIterator;
                Object[] resultValues = this.resultValues;
                if (resultValues == null) {
                    resultValues = this.resultValues = new Object[this.metaData.getColumnCount()];
                }
                if (searchResultSetIterator.getResultValueCount() != resultValues.length) {
                    throw new UnknownError();
                }
                for (int i = 0, resultValueCount = resultValues.length; i < resultValueCount; i++) {
                    resultValues[i] = searchResultSetIterator.getResultValue(i);
                }
            } else {
                this.resultValues = null;
            }
            return haveNext;
        }

        @Override
        public final void close() throws IOException {
            this.searchResultSetIterator.close();
        }

    }

    private static abstract class SearchResultSetIterator implements Closeable {

        abstract boolean next() throws SQL4ESException;

        abstract int getResultValueCount();

        abstract Object getResultValue(final int valueIndex);

    }

    private static final class NonAggregationNormalSearchExecutePlan extends
            SelectStatementExecutePlan<NonAggregationNormalSearchExecutePlan.NonAggregationNormalSearchRequestBuilder,
                    NonAggregationNormalSearchExecutePlan.NonAggregationNormalSearchCallback> {

        private static final String[] EMPTY_STRING_ARRAY = new String[0];

        protected NonAggregationNormalSearchExecutePlan(
                final SearchExecuteContext context) throws SQL4ESException {
            super(new NonAggregationNormalSearchExecuteContext(context));
        }

        @Override
        protected final boolean accept() throws SQL4ESException {
            final SelectStatement statement = this.context.statement;
            final ReadonlyList<ResultColumnDeclare> resultColumnDeclareList
                    = statement.selectClause.resultColumnDeclareList;
            for (int index = 0, size = resultColumnDeclareList.size(); index < size; index++) {
                final ResultColumnDeclare resultColumnDeclare = resultColumnDeclareList.get(index);
                final Expression expression = resultColumnDeclare.expression;
                if (!(expression instanceof Reference
                        || expression instanceof Constant
                        || expression instanceof Argument)) {
                    return false;
                }
            }
            final LimitClause limitClause = statement.limitClause;
            if (limitClause != null) {
                final Expression rowCountExpression = limitClause.rowCount;
                final int rowCount;
                if (rowCountExpression instanceof Argument) {
                    rowCount = Integer.parseInt(
                            this.context.argumentValue(
                                    Argument.class.cast(rowCountExpression)
                            ).toString()
                    );
                } else {
                    rowCount = Integer.parseInt(
                            Constant.class.cast(rowCountExpression).value.toString()
                    );
                }
                if (rowCount > this.context.maxRowCount) {
                    return false;
                }
            }
            // TODO 判断其他子句中是否包含算数运算类表达式，如果包含，则返回false
            return true;
        }

        private SearchSourceBuilder searchSourceBuilder;

        @Override
        protected final NonAggregationNormalSearchRequestBuilder doCreateRequestBuilder() throws SQL4ESException {
            final NonAggregationNormalSearchExecuteContext context
                    = (NonAggregationNormalSearchExecuteContext) this.context;
            // 语义分析
            final SelectStatementSemanticAnalyzer semanticAnalyzer
                    = new NonAggregationNormalSearchSemanticAnalyzer(context);
            semanticAnalyzer.analyzeStatement();

            // TODO 需要完善search template
            this.searchSourceBuilder = new SearchSourceBuilder();
            this
                    .fetchSource(context)
                    .query(context)
                    .postFilter(context)
                    .sort(context)
                    .from(context)
                    .size(context)
            ;
            return new NonAggregationNormalSearchRequestBuilder(this.searchSourceBuilder);
        }

        @Override
        protected final NonAggregationNormalSearchCallback doCallback() throws SQL4ESException {
            return new NonAggregationNormalSearchCallback();
        }

        private NonAggregationNormalSearchExecutePlan fetchSource(
                final NonAggregationNormalSearchExecuteContext context) {
            final ArrayList<Reference> resultColumnReferenceList = context.resultColumnReferenceList;
            final int size = resultColumnReferenceList.size();
            final String[] includes = new String[size];
            for (int index = 0; index < size; index++) {
                includes[index] = resultColumnReferenceList.get(index).columnIdentifier.toString();
            }
            this.searchSourceBuilder.fetchSource(includes, EMPTY_STRING_ARRAY);
            return this;
        }

        private NonAggregationNormalSearchExecutePlan query(
                final NonAggregationNormalSearchExecuteContext context) throws SQL4ESException {
            final Expression conditionExpression = context.finallyWhereConditionExpression;
            if (conditionExpression != null) {
                this.searchSourceBuilder.query(
                        new FilteredQueryBuilder(
                                QueryBuilders.matchAllQuery(),
                                this.buildFilterBuilder(conditionExpression)
                        )
                );
            }
            return this;
        }

        private NonAggregationNormalSearchExecutePlan postFilter(
                final NonAggregationNormalSearchExecuteContext context) throws SQL4ESException {
            final Expression conditionExpression = context.finallyHavingConditionExpression;
            if (conditionExpression != null) {
                this.searchSourceBuilder.postFilter(
                        this.buildFilterBuilder(conditionExpression)
                );
            }
            return this;
        }

        private NonAggregationNormalSearchExecutePlan sort(
                final NonAggregationNormalSearchExecuteContext context) throws SQL4ESException {
            final OrderSpecification[] orderSpecifications = context.finallyOrderSpecifications;
            if (orderSpecifications != null && orderSpecifications.length > 0) {
                OrderSpecification orderSpecification;
                for (int index = 0, length = orderSpecifications.length; index < length; index++) {
                    orderSpecification = orderSpecifications[index];
                    final String fieldName
                            = Reference.class.cast(orderSpecification.expression).columnIdentifier.toString();
                    this.searchSourceBuilder.sort(
                            SortBuilders
                                    .fieldSort(fieldName)
                                    .order(orderSpecification.ascendingOrder ? SortOrder.ASC : SortOrder.DESC)
                    );
                }
            }
            return this;
        }

        private NonAggregationNormalSearchExecutePlan from(
                final NonAggregationNormalSearchExecuteContext context) throws SQL4ESException {
            this.searchSourceBuilder.from(context.offset);
            return this;
        }

        private NonAggregationNormalSearchExecutePlan size(
                final NonAggregationNormalSearchExecuteContext context) throws SQL4ESException {
            this.searchSourceBuilder.size(context.rowCount);
            return this;
        }

        final class NonAggregationNormalSearchRequestBuilder implements
                InternalDocumentRequestBuilder<SearchRequest, SearchResponse, SearchRequestBuilder,
                        SearchAction, SearchExecuteContext> {

            private final SearchSourceBuilder searchSourceBuilder;

            NonAggregationNormalSearchRequestBuilder(final SearchSourceBuilder searchSourceBuilder) {
                this.searchSourceBuilder = searchSourceBuilder;
            }

            @Override
            public final SearchAction buildAction() {
                return SearchAction.INSTANCE;
            }

            @Override
            public final SearchRequest buildRequest(final Client client,
                                                    final SearchExecuteContext context) throws SQL4ESException {
                final SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);
                searchRequestBuilder.internalBuilder(this.searchSourceBuilder);
                searchRequestBuilder.setIndices(
                        context.environment.getIndexDefinition().getIdentifier().toString()
                );
                searchRequestBuilder.setTypes(
                        NonAggregationNormalSearchExecutePlan.this.context.tableIdentifier.toString()
                );
                if (NonAggregationNormalSearchExecutePlan.this.outputSQLToElasticsearchRequestMapping) {
                    LOGGER.info("SQL and request mapping [" + context.statement + " -> "
                            + searchRequestBuilder.toString() + "]");
                }

                return searchRequestBuilder.request();
            }

        }

        final class NonAggregationNormalSearchCallback extends SearchResponseCallback {

            private final NonAggregationNormalSearchExecuteContext context;

            private final int resultColumnCount;

            NonAggregationNormalSearchCallback() {
                this.context
                        = (NonAggregationNormalSearchExecuteContext) NonAggregationNormalSearchExecutePlan.this.context;
                this.resultColumnCount = this.context.resultColumnAliasList.size();
            }

            @Override
            public final InternalResultSet getResultSet() throws SQL4ESException {
                return new SearchResultSet(
                        this.buildResultSetMetaData(),
                        this.buildResultIterator()
                );
            }

            private Object[][] values;

            @Override
            public final void callback(final SearchResponse response) throws SQL4ESException {
                if (response.status() != RestStatus.OK) {
                    throw new SQL4ESException("Failed to execute query with elasticsearch, response status is: "
                            + response.status().getStatus());
                }
                final SearchHits searchHits = response.getHits();
                final SearchHit[] hits = searchHits.getHits();
                final int length = hits.length;
                this.values = new Object[length][];
                Map<String, Object> sourceMap;
                for (int index = 0; index < length; index++) {
                    sourceMap = hits[index].getSource();
                    Expression resultColumnExpression;
                    Object[] columnValues = new Object[this.resultColumnCount];
                    for (int columnIndex = 0; columnIndex < this.resultColumnCount; columnIndex++) {
                        resultColumnExpression = this.context.resultColumnExpressionList.get(columnIndex);
                        if (resultColumnExpression instanceof Reference) {
                            final Reference reference = Reference.class.cast(resultColumnExpression);
                            final Identifier columnIdentifier = reference.columnIdentifier;
                            final FieldDefinition fieldDefinition
                                    = this.context.typeDefinition.getFieldDefinition(columnIdentifier);
                            final DataType dataType
                                    = fieldDefinition.getDataType();
                            final Object columnValue = sourceMap.get(columnIdentifier.toString());
                            columnValues[columnIndex] = columnValue == null ? null : dataType.toValue(
                                    columnValue.toString(),
                                    fieldDefinition.getPattern()
                            );
                        } else {
                            columnValues[columnIndex] = Constant.class.cast(resultColumnExpression).value;
                        }
                    }
                    this.values[index] = columnValues;
                }
            }

            private SearchResultSet.SearchResultSetMetaData buildResultSetMetaData() throws SQL4ESException {
                SearchResultSet.SearchResultSetMetaData.ColumnInformation columnInformation;
                Identifier columnIdentifier;
                String columnLabel;
                DataType dataType;
                int columnDisplaySize;
                Expression columnValueExpression;
                ArrayList<SearchResultSet.SearchResultSetMetaData.ColumnInformation> columnInformationList
                        = new ArrayList<SearchResultSet.SearchResultSetMetaData.ColumnInformation>();
                for (int index = 0, size = this.context.resultColumnAliasList.size(); index < size; index++) {
                    columnIdentifier = this.context.resultColumnAliasList.get(index);
                    columnValueExpression = this.context.aliasExpressionMap.get(columnIdentifier);
                    if (columnValueExpression instanceof Reference) {
                        dataType = this.context.typeDefinition.getFieldDefinition(
                                Reference.class.cast(columnValueExpression).columnIdentifier
                        ).getDataType();
                    } else {
                        // constant
                        dataType = DataType.getDataType(Constant.class.cast(columnValueExpression).value.getClass());
                    }
                    columnLabel = columnIdentifier.toString();
                    columnDisplaySize = dataType.displaySize();
                    columnInformation = new SearchResultSet.SearchResultSetMetaData.ColumnInformation(
                            columnIdentifier,
                            columnLabel,
                            dataType,
                            columnDisplaySize
                    );
                    columnInformationList.add(columnInformation);
                }
                return new SearchResultSet.SearchResultSetMetaData(
                        columnInformationList.toArray(
                                new SearchResultSet.SearchResultSetMetaData.ColumnInformation[columnInformationList.size()]
                        )
                );
            }

            private SearchResultSetIterator buildResultIterator() throws SQL4ESException {
                return new SearchResultSetIteratorImpl();
            }

            private final class SearchResultSetIteratorImpl extends SearchResultSetIterator {

                private int index;

                private final int size;

                SearchResultSetIteratorImpl() {
                    this.index = -1;
                    this.size = NonAggregationNormalSearchCallback.this.values.length;
                }

                @Override
                final boolean next() throws SQL4ESException {
                    return ++this.index < this.size;
                }

                @Override
                final int getResultValueCount() {
                    return NonAggregationNormalSearchCallback.this.values[this.index].length;
                }

                @Override
                final Object getResultValue(int valueIndex) {
                    return NonAggregationNormalSearchCallback.this.values[this.index][valueIndex];
                }

                @Override
                public final void close() throws IOException {
                    // nothing to do.
                }
            }

        }

        private static final class NonAggregationNormalSearchSemanticAnalyzer extends
                SelectStatementSemanticAnalyzer<NonAggregationNormalSearchExecuteContext> {

            NonAggregationNormalSearchSemanticAnalyzer(
                    final NonAggregationNormalSearchExecuteContext context) throws SQL4ESException {
                super(context);
            }

            @Override
            public final void analyzeStatement() throws SQL4ESException {
                this.analyzeWhereClause();
                this.analyzeGroupByClause();
                this.analyzeSelectClause();
                this.analyzeHavingClause();
                this.analyzeOrderByClause();
                this.analyzeLimitClause();
            }

            private void analyzeSelectClause() throws SQL4ESException {
                final ReadonlyList<ResultColumnDeclare> originalResultColumnDeclareList
                        = this.statement.selectClause.resultColumnDeclareList;
                final int size = originalResultColumnDeclareList.size();
                ResultColumnDeclare resultColumnDeclare;
                Expression resultColumnDeclareExpression;
                for (int index = 0; index < size; index++) {
                    resultColumnDeclare = originalResultColumnDeclareList.get(index);
                    resultColumnDeclareExpression = resultColumnDeclare.expression;
                    if (resultColumnDeclareExpression instanceof Reference) {
                        final Reference reference = Reference.class.cast(resultColumnDeclareExpression);
                        if (!(reference.setIdentifier == null
                                || reference.setIdentifier.equals(this.context.tableAlias)
                                || reference.setIdentifier.equals(this.context.tableIdentifier))) {
                            throw new SQL4ESException("Unknown data source identifier[" + reference.setIdentifier +
                                    "] in group by clause.");
                        }

                        if (reference.columnIdentifier.equals(Reference.ALL_COLUMN_IDENTIFIER)) {
                            Reference finallyReference;
                            for (int fieldIndex = 0, fieldCount = this.context.typeDefinition.getFieldDefinitionCount();
                                 fieldIndex < fieldCount; fieldIndex++) {
                                final FieldDefinition fieldDefinition
                                        = this.context.typeDefinition.getFieldDefinition(fieldIndex);
                                final Identifier fieldIdentifier = fieldDefinition.getIdentifier();
                                final Identifier alias = fieldIdentifier;
                                if (this.context.resultColumnAliasList.contains(alias)) {
                                    throw new SQL4ESException(
                                            "There's multi columns with the same alias[" + alias + "] in select clause."
                                    );
                                }
                                finallyReference = new Reference(reference.setIdentifier, fieldIdentifier);
                                if (!this.context.resultColumnReferenceList.contains(finallyReference)) {
                                    this.context.resultColumnReferenceList.add(finallyReference);
                                }
                                this.context.aliasReferenceMap.put(alias, finallyReference);
                                this.context.resultColumnAliasList.add(alias);
                                this.context.aliasExpressionMap.put(alias, finallyReference);
                                this.context.resultColumnExpressionList.add(finallyReference);
                            }
                        } else {
                            try {
                                this.context.typeDefinition.getFieldDefinition(reference.columnIdentifier);
                            } catch (FieldNotExistsException e) {
                                throw new SQL4ESException(
                                        "Column[" + reference.columnIdentifier + "] is not found in table[" +
                                                this.context.tableIdentifier + "]"
                                );
                            }
                            final Identifier alias
                                    = resultColumnDeclare.alias == null ? reference.columnIdentifier : resultColumnDeclare.alias;
                            if (this.context.resultColumnAliasList.contains(alias)) {
                                throw new SQL4ESException(
                                        "There's multi columns with the same alias[" + alias + "] in select clause."
                                );
                            }
                            if (!this.context.resultColumnReferenceList.contains(reference)) {
                                this.context.resultColumnReferenceList.add(reference);
                            }
                            this.context.aliasReferenceMap.put(alias, reference);
                            this.context.resultColumnAliasList.add(alias);
                            this.context.aliasExpressionMap.put(alias, reference);
                            this.context.resultColumnExpressionList.add(reference);
                        }

                    } else if (resultColumnDeclareExpression instanceof Constant) {
                        final Constant constant = Constant.class.cast(resultColumnDeclareExpression);
                        final Identifier alias
                                = resultColumnDeclare.alias == null ? new Identifier(constant.value.toString()) : resultColumnDeclare.alias;
                        if (this.context.resultColumnAliasList.contains(alias)) {
                            throw new SQL4ESException(
                                    "There's multi columns with the same alias[" + alias + "] in select clause."
                            );
                        }
                        this.context.resultColumnAliasList.add(alias);
                        this.context.aliasExpressionMap.put(alias, constant);
                        this.context.resultColumnExpressionList.add(constant);

                    } else {
                        throw new SQL4ESException(
                                "Invalid expression in select clause, only support Reference and Constant."
                        );
                    }
                }
            }

            private void analyzeGroupByClause() throws SQL4ESException {
                if (this.statement.groupByClause != null) {
                    throw new SQL4ESException("Group by clause is not necessary.");
                }
            }

            private void analyzeHavingClause() throws SQL4ESException {
                final HavingClause originalHavingClause = this.statement.havingClause;
                if (originalHavingClause == null) {
                    return;
                }
                final Expression originalConditionExpression = originalHavingClause.conditionExpression;
                if (originalConditionExpression.getResultType() != DataType.BOOLEAN) {
                    throw new SQL4ESException(
                            "Invalid expression in having clause, only allow boolean expression."
                    );
                }
                this.context.finallyHavingConditionExpression
                        = analyzeExpressionInHavingClause(originalConditionExpression);
            }

            private void analyzeOrderByClause() throws SQL4ESException {
                final OrderByClause originalClause = this.statement.orderByClause;
                if (originalClause == null) {
                    return;
                }
                final ReadonlyList<OrderSpecification> originalOrderSpecificationList
                        = originalClause.orderSpecificationList;
                final int size = originalOrderSpecificationList.size();
                final OrderSpecification[] finallyOrderSpecifications = new OrderSpecification[size];
                OrderSpecification originalOrderSpecification;
                Expression originalExpression;
                for (int index = 0; index < size; index++) {
                    originalOrderSpecification = originalOrderSpecificationList.get(index);
                    originalExpression = originalOrderSpecification.expression;
                    if (!(originalExpression instanceof Reference)) {
                        throw new SQL4ESException("Invalid expression[" + originalExpression +
                                "] in order by clause, only allow reference.");
                    }
                    final Reference reference = Reference.class.cast(originalExpression);
                    if (reference.columnIdentifier.equals(Reference.ALL_COLUMN_IDENTIFIER)) {
                        throw new SQL4ESException("Can not have * in order by clause.");
                    }
                    final Reference resultColumnReference
                            = this.context.aliasReferenceMap.get(reference.columnIdentifier);
                    if (resultColumnReference == null) {
                        throw new SQL4ESException(
                                "Unknown column[" + reference.columnIdentifier +
                                        "], maybe it is not in select clause or it is a constant in select clause."
                        );
                    }
                    finallyOrderSpecifications[index] = new OrderSpecification(
                            resultColumnReference,
                            originalOrderSpecification.ascendingOrder
                    );
                }
                this.context.finallyOrderSpecifications = finallyOrderSpecifications;
            }

            private void analyzeLimitClause() throws SQL4ESException {
                final LimitClause limitClause = this.statement.limitClause;
                if (limitClause == null) {
                    this.context.offset = 0;
                    this.context.rowCount = this.context.maxRowCount;
                } else {
                    final Expression offsetExpression = limitClause.offset;
                    final Expression rowCountExpression = limitClause.rowCount;
                    if (offsetExpression instanceof Argument) {
                        this.context.offset
                                = Integer.parseInt(
                                this.context.argumentValue(Argument.class.cast(offsetExpression)).toString()
                        );
                    } else {
                        this.context.offset
                                = Integer.parseInt(
                                Constant.class.cast(offsetExpression).value.toString()
                        );
                    }
                    if (rowCountExpression instanceof Argument) {
                        this.context.rowCount
                                = Integer.parseInt(
                                this.context.argumentValue(Argument.class.cast(rowCountExpression)).toString()
                        );
                    } else {
                        this.context.rowCount
                                = Integer.parseInt(
                                Constant.class.cast(rowCountExpression).value.toString()
                        );
                    }
                }
            }

            private Expression analyzeExpressionInHavingClause(
                    final Expression originalExpression) throws SQL4ESException {
                if (originalExpression instanceof Reference) {
                    final Reference reference = Reference.class.cast(originalExpression);
                    if (reference.columnIdentifier.equals(Reference.ALL_COLUMN_IDENTIFIER)) {
                        throw new SQL4ESException("Can not contain * reference in having clause.");
                    }
                    Reference resultColumnReference = this.context.aliasReferenceMap.get(reference.columnIdentifier);
                    if (resultColumnReference == null) {
                        throw new SQL4ESException(
                                "Unknown column[" + reference + "], " +
                                        "it is not exist in select clause or maybe it is constant in select clause."
                        );
                    }
                    return resultColumnReference;
                } else if (originalExpression instanceof NonAggregation) {
                    final ReadonlyList<Expression> operandExpressionList
                            = originalExpression.getOperandExpressionList();
                    if (!operandExpressionList.isEmpty()) {
                        for (int index = 0, size = operandExpressionList.size(); index < size; index++) {
                            analyzeExpressionInHavingClause(operandExpressionList.get(index));
                        }
                        return originalExpression;
                    } else {
                        return originalExpression;
                    }
                } else if (originalExpression instanceof Argument) {
                    return originalExpression;
                } else if (originalExpression instanceof Constant) {
                    return originalExpression;
                }
                throw new SQL4ESException("Invalid expression[" + originalExpression +
                        "], which is not supported in having clause.");
            }

        }

        private static final class NonAggregationNormalSearchExecuteContext extends SearchExecuteContext {

            final HashMap<Identifier, Reference> aliasReferenceMap;

            final HashMap<Identifier, Expression> aliasExpressionMap;

            final ArrayList<Expression> resultColumnExpressionList;

            final ArrayList<Reference> resultColumnReferenceList;

            final ArrayList<Identifier> resultColumnAliasList;

            NonAggregationNormalSearchExecuteContext(final SearchExecuteContext context) throws SQL4ESException {
                super(context);
                this.aliasReferenceMap = new HashMap<Identifier, Reference>();
                this.aliasExpressionMap = new HashMap<Identifier, Expression>();
                this.resultColumnReferenceList = new ArrayList<Reference>();
                this.resultColumnAliasList = new ArrayList<Identifier>();
                this.resultColumnExpressionList = new ArrayList<Expression>();
            }

            Expression finallyHavingConditionExpression;

            OrderSpecification[] finallyOrderSpecifications;

            int offset;

            int rowCount;

        }

    }

    private static final class AggregationNormalSearchExecutePlan extends
            SelectStatementExecutePlan<AggregationNormalSearchExecutePlan.AggregationNormalSearchRequestBuilder,
                    AggregationNormalSearchExecutePlan.AggregationNormalSearchCallback> {

        protected AggregationNormalSearchExecutePlan(final SearchExecuteContext context) throws SQL4ESException {
            super(new AggregationNormalSearchExecuteContext(context));
        }

        @Override
        protected final boolean accept() {
            final SelectStatement statement = this.context.statement;
            final ReadonlyList<ResultColumnDeclare> resultColumnDeclareList
                    = statement.selectClause.resultColumnDeclareList;
            ResultColumnDeclare resultColumnDeclare;
            for (int index = 0, size = resultColumnDeclareList.size(); index < size; index++) {
                resultColumnDeclare = resultColumnDeclareList.get(index);
                if (resultColumnDeclare.expression instanceof Aggregation) {
                    return true;
                }
            }
            return false;
        }

        private SearchSourceBuilder searchSourceBuilder;

        @Override
        protected final AggregationNormalSearchRequestBuilder doCreateRequestBuilder() throws SQL4ESException {
            final AggregationNormalSearchExecuteContext context
                    = (AggregationNormalSearchExecuteContext) this.context;
            final SelectStatementSemanticAnalyzer semanticAnalyzer
                    = new AggregationNormalSearchSemanticAnalyzer(context);
            semanticAnalyzer.analyzeStatement();
            this.searchSourceBuilder = new SearchSourceBuilder();
            this
                    .query(context)
                    .agg(context);
            return new AggregationNormalSearchRequestBuilder(this.searchSourceBuilder);
        }

        @Override
        protected final AggregationNormalSearchCallback doCallback() throws SQL4ESException {
            return new AggregationNormalSearchCallback();
        }

        private AggregationNormalSearchExecutePlan query(
                final AggregationNormalSearchExecuteContext context) throws SQL4ESException {
            final Expression conditionExpression = context.finallyWhereConditionExpression;
            if (conditionExpression != null) {
                this.searchSourceBuilder.query(
                        new FilteredQueryBuilder(
                                QueryBuilders.matchAllQuery(),
                                this.buildFilterBuilder(conditionExpression)
                        )
                );
            }
            return this;
        }

        private AggregationNormalSearchExecutePlan agg(
                final AggregationNormalSearchExecuteContext context) throws SQL4ESException {
            if (context.existsGroupByClause) {
                final ArrayList<Identifier> groupByColumnIdentifierList = context.finallyGroupColumnIdentifierList;
                final int size = groupByColumnIdentifierList.size();
                String groupByColumnName = groupByColumnIdentifierList.get(size - 1).toString();
                TermsBuilder termsBuilder
                        = AggregationBuilders.terms(groupByColumnName).field(groupByColumnName).size(0);
                for (Map.Entry<Aggregation, Integer> entry : context.finallyResultColumnAggregationIndexMap.entrySet()) {
                    termsBuilder.subAggregation(this.agg(entry.getKey(), entry.getValue()));
                }
                for (int index = size - 2; index >= 0; index--) {
                    groupByColumnName = groupByColumnIdentifierList.get(index).toString();
                    termsBuilder = AggregationBuilders
                            .terms(groupByColumnName).field(groupByColumnName).size(0)
                            .subAggregation(termsBuilder);
                }
                this.searchSourceBuilder.aggregation(termsBuilder);
            } else {
                for (Map.Entry<Aggregation, Integer> entry : context.finallyResultColumnAggregationIndexMap.entrySet()) {
                    this.searchSourceBuilder.aggregation(
                            this.agg(entry.getKey(), entry.getValue())
                    );
                }
            }
            return this;
        }

        private AbstractAggregationBuilder agg(final Aggregation aggregation,
                                               final Integer index) throws SQL4ESException {
            final String name = index.toString();
            if (aggregation instanceof CountFunction) {
                final CountFunction count = CountFunction.class.cast(aggregation);
                final Reference operand = Reference.class.cast(count.getOperandExpression(0));
                if (operand.columnIdentifier.equals(Reference.ALL_COLUMN_IDENTIFIER)) {
                    return AggregationBuilders
                            .count(name)
                            .field(
                                    this.context.typeDefinition.getFieldDefinition(0).getIdentifier().toString()
                            );
                }
                return AggregationBuilders
                        .count(name)
                        .field(
                                operand.columnIdentifier.toString()
                        );
            } else if (aggregation instanceof SummaryFunction) {
                final SummaryFunction summary = SummaryFunction.class.cast(aggregation);
                return AggregationBuilders.sum(name)
                        .field(
                                Reference.class.cast(summary.getOperandExpression(0)).columnIdentifier.toString()
                        );
            } else if (aggregation instanceof AverageFunction) {
                final AverageFunction average = AverageFunction.class.cast(aggregation);
                return AggregationBuilders.avg(name)
                        .field(
                                Reference.class.cast(average.getOperandExpression(0)).columnIdentifier.toString()
                        );
            } else if (aggregation instanceof MaxinumFunction) {
                final MaxinumFunction maxinum = MaxinumFunction.class.cast(aggregation);
                return AggregationBuilders.max(name)
                        .field(
                                Reference.class.cast(maxinum.getOperandExpression(0)).columnIdentifier.toString()
                        );
            } else if (aggregation instanceof MininumFunction) {
                final MininumFunction mininum = MininumFunction.class.cast(aggregation);
                return AggregationBuilders.min(name)
                        .field(
                                Reference.class.cast(mininum.getOperandExpression(0)).columnIdentifier.toString()
                        );
            }
            throw new SQL4ESException("Unsupported aggregation[" + aggregation + "]");
        }

        final class AggregationNormalSearchRequestBuilder implements
                InternalDocumentRequestBuilder<SearchRequest, SearchResponse, SearchRequestBuilder,
                        SearchAction, SearchExecuteContext> {

            private final SearchSourceBuilder searchSourceBuilder;

            AggregationNormalSearchRequestBuilder(final SearchSourceBuilder searchSourceBuilder) {
                this.searchSourceBuilder = searchSourceBuilder;
            }

            @Override
            public final SearchAction buildAction() {
                return SearchAction.INSTANCE;
            }

            @Override
            public final SearchRequest buildRequest(final Client client,
                                                    final SearchExecuteContext context) throws SQL4ESException {
                final SearchRequestBuilder builder = new SearchRequestBuilder(client);
                builder.internalBuilder(this.searchSourceBuilder);
                builder.setIndices(
                        context.environment.getIndexDefinition().getIdentifier().toString()
                );
                builder.setTypes(
                        AggregationNormalSearchExecutePlan.this.context.tableIdentifier.toString()
                );
                builder.setSearchType(SearchType.COUNT);
                if (AggregationNormalSearchExecutePlan.this.outputSQLToElasticsearchRequestMapping) {
                    LOGGER.info("SQL and request mapping [" + context.statement + " -> "
                            + builder.toString() + "]");
                }

                return builder.request();
            }

        }

        final class AggregationNormalSearchCallback extends SearchResponseCallback {

            private final AggregationNormalSearchExecuteContext context;

            AggregationNormalSearchCallback() {
                this.context
                        = (AggregationNormalSearchExecuteContext) AggregationNormalSearchExecutePlan.this.context;
            }

            @Override
            public final InternalResultSet getResultSet() throws SQL4ESException {
                return new SearchResultSet(
                        this.buildResultSetMetaData(),
                        this.buildResultIterator()
                );
            }

            private ArrayList<Object[]> values;

            @Override
            public final void callback(final SearchResponse response) throws SQL4ESException {
                if (response.status() != RestStatus.OK) {
                    throw new SQL4ESException("Failed to execute query with elasticsearch, response status is: "
                            + response.status().getStatus());
                }
                this.values = new ArrayList<Object[]>();
                final int resultColumnCount = this.context.finallyResultColumnAliasList.size();
                final int groupColumnCount = this.context.finallyGroupColumnIdentifierList.size();
                final Object[] columnValues = new Object[resultColumnCount];
                this.parseAggregationResponse(
                        response.getAggregations(),
                        columnValues,
                        0,
                        groupColumnCount
                );
            }

            private void parseAggregationResponse(final Aggregations aggs,
                                                  final Object[] columnValues,
                                                  final int currentGroupColumnIndex,
                                                  final int groupColumnCount) throws SQL4ESException {
                if (currentGroupColumnIndex == groupColumnCount) {
                    // 需要解析聚合函数值以及常量值
                    for (Map.Entry<Integer, Constant> constantEntry : this.context.finallyResultColumnConstantIndexMap.entrySet()) {
                        columnValues[constantEntry.getKey()] = constantEntry.getValue().value;
                    }
                    Aggregation agg;
                    Integer index;
                    Object aggValue;
                    for (Map.Entry<Aggregation, Integer> aggEntry : this.context.finallyResultColumnAggregationIndexMap.entrySet()) {
                        agg = aggEntry.getKey();
                        index = aggEntry.getValue();
                        if (agg instanceof CountFunction) {
                            aggValue = ValueCount.class.cast(aggs.get(index.toString())).getValue();
                        } else if (agg instanceof SummaryFunction) {
                            aggValue = Sum.class.cast(aggs.get(index.toString())).getValue();
                        } else if (agg instanceof AverageFunction) {
                            aggValue = Avg.class.cast(aggs.get(index.toString())).getValue();
                        } else if (agg instanceof MaxinumFunction) {
                            aggValue = Max.class.cast(aggs.get(index.toString())).getValue();
                        } else if (agg instanceof MininumFunction) {
                            aggValue = Min.class.cast(aggs.get(index.toString())).getValue();
                        } else {
                            throw new SQL4ESException("Unsupported expression[" + agg + "]");
                        }
                        columnValues[index] = aggValue;
                    }
                    final Object[] finallyResultColumnValues = new Object[columnValues.length];
                    System.arraycopy(columnValues, 0, finallyResultColumnValues, 0, columnValues.length);
                    this.values.add(finallyResultColumnValues);
                    return;
                }
                final Identifier groupColumnIdentifier
                        = this.context.finallyGroupColumnIdentifierList.get(currentGroupColumnIndex);
                final int nextGroupColumnIndex = currentGroupColumnIndex + 1;
                final Terms terms = aggs.get(groupColumnIdentifier.toString());
                final Integer groupColumnInResultColumnsIndex
                        = this.context.finallyGroupColumnResultSetIndexMap.get(groupColumnIdentifier);
                final FieldDefinition groupColumnDefinition
                        = this.context.typeDefinition.getFieldDefinition(groupColumnIdentifier);
                final DataType currentGroupColumnDataType = groupColumnDefinition.getDataType();
                final boolean groupColumnExistsResultColumns = groupColumnInResultColumnsIndex != null;
                String groupColumnValue;
                for (Terms.Bucket bucket : terms.getBuckets()) {
                    if (groupColumnExistsResultColumns) {
                        groupColumnValue = bucket.getKey();
                        columnValues[groupColumnInResultColumnsIndex] = currentGroupColumnDataType.toValue(
                                groupColumnValue,
                                groupColumnDefinition.getPattern()
                        );
                    }
                    this.parseAggregationResponse(
                            bucket.getAggregations(),
                            columnValues,
                            nextGroupColumnIndex,
                            groupColumnCount
                    );
                }
            }

            private SearchResultSetIterator buildResultIterator() throws SQL4ESException {
                return new SearchResultSetIteratorImpl();
            }

            private SearchResultSet.SearchResultSetMetaData buildResultSetMetaData() throws SQL4ESException {
                SearchResultSet.SearchResultSetMetaData.ColumnInformation columnInformation;
                Identifier columnIdentifier;
                String columnLabel;
                DataType dataType;
                int columnDisplaySize;
                Expression columnValueExpression;
                ArrayList<SearchResultSet.SearchResultSetMetaData.ColumnInformation> columnInformationList
                        = new ArrayList<SearchResultSet.SearchResultSetMetaData.ColumnInformation>();
                for (int index = 0, size = this.context.finallyResultColumnAliasList.size(); index < size; index++) {
                    columnIdentifier = this.context.finallyResultColumnAliasList.get(index);
                    columnValueExpression = this.context.finallyResultColumnExpressionIndexMap.get(index);
                    if (columnValueExpression instanceof Reference) {
                        dataType = this.context.typeDefinition.getFieldDefinition(
                                Reference.class.cast(columnValueExpression).columnIdentifier
                        ).getDataType();
                    } else if (columnValueExpression instanceof Aggregation) {
                        dataType = columnValueExpression.getResultType();
                    } else {
                        // constant
                        dataType = DataType.getDataType(Constant.class.cast(columnValueExpression).value.getClass());
                    }
                    columnLabel = columnIdentifier.toString();
                    columnDisplaySize = dataType.displaySize();
                    columnInformation = new SearchResultSet.SearchResultSetMetaData.ColumnInformation(
                            columnIdentifier,
                            columnLabel,
                            dataType,
                            columnDisplaySize
                    );
                    columnInformationList.add(columnInformation);
                }
                return new SearchResultSet.SearchResultSetMetaData(
                        columnInformationList.toArray(
                                new SearchResultSet.SearchResultSetMetaData.ColumnInformation[columnInformationList.size()]
                        )
                );
            }

            private final class SearchResultSetIteratorImpl extends SearchResultSetIterator {

                private int index;

                private final int size;

                SearchResultSetIteratorImpl() {
                    this.index = -1;
                    this.size = AggregationNormalSearchCallback.this.values.size();
                }

                @Override
                final boolean next() throws SQL4ESException {
                    return ++this.index < this.size;
                }

                @Override
                final int getResultValueCount() {
                    return AggregationNormalSearchCallback.this.values.get(this.index).length;
                }

                @Override
                final Object getResultValue(int valueIndex) {
                    return AggregationNormalSearchCallback.this.values.get(this.index)[valueIndex];
                }

                @Override
                public final void close() throws IOException {
                    // nothing to do.
                }
            }

        }

        private static final class AggregationNormalSearchExecuteContext extends SearchExecuteContext {

            AggregationNormalSearchExecuteContext(final SearchExecuteContext context) throws SQL4ESException {
                super(context);
                this.finallyGroupColumnIdentifierList = new ArrayList<Identifier>();
                this.finallyResultColumnAliasList = new ArrayList<Identifier>();
                this.finallyResultColumnExpressionIndexMap = new HashMap<Integer, Expression>();
                this.finallyResultColumnAggregationIndexMap = new HashMap<Aggregation, Integer>();
                this.finallyGroupColumnResultSetIndexMap = new HashMap<Identifier, Integer>();
                this.finallyResultColumnConstantIndexMap = new HashMap<Integer, Constant>();
            }

            boolean existsGroupByClause = false;

            private final ArrayList<Identifier> finallyGroupColumnIdentifierList;

            private final ArrayList<Identifier> finallyResultColumnAliasList;

            private final HashMap<Aggregation, Integer> finallyResultColumnAggregationIndexMap;

            // 结果列表达式与其在SELECT子句中索引位置的映射关系，用于解析元数据
            private final HashMap<Integer, Expression> finallyResultColumnExpressionIndexMap;

            // group column在SELECT子句中的索引位置
            private final HashMap<Identifier, Integer> finallyGroupColumnResultSetIndexMap;

            private final HashMap<Integer, Constant> finallyResultColumnConstantIndexMap;

        }

        private static final class AggregationNormalSearchSemanticAnalyzer
                extends SelectStatementSemanticAnalyzer<AggregationNormalSearchExecuteContext> {

            protected AggregationNormalSearchSemanticAnalyzer(
                    final AggregationNormalSearchExecuteContext context) throws SQL4ESException {
                super(context);
            }

            @Override
            public final void analyzeStatement() throws SQL4ESException {
                this.analyzeWhereClause();
                this.analyzeGroupByClause();
                this.analyzeSelectClause();
                this.analyzeHavingClause();
                this.analyzeOrderByClause();
                this.analyzeLimitClause();
            }

            private void analyzeGroupByClause() throws SQL4ESException {
                final GroupByClause originalGroupClause = this.context.statement.groupByClause;
                if (originalGroupClause == null) {
                    return;
                }
                this.context.existsGroupByClause = true;
                final ReadonlyList<Expression> originalGroupExpressionList = originalGroupClause.groupExpressionList;
                Expression originalExpression;
                Reference reference;
                Identifier identifier;
                for (int index = 0, size = originalGroupExpressionList.size(); index < size; index++) {
                    originalExpression = originalGroupExpressionList.get(index);
                    if (!(originalExpression instanceof Reference)) {
                        throw new SQL4ESException("Invalid expression in where clause, only allow column name.");
                    }
                    reference = Reference.class.cast(originalExpression);
                    if (reference.columnIdentifier.equals(Reference.ALL_COLUMN_IDENTIFIER)) {
                        throw new SQL4ESException("Can not contain * reference in where clause.");
                    }
                    if (!(reference.setIdentifier == null
                            || reference.setIdentifier.equals(this.context.tableAlias)
                            || reference.setIdentifier.equals(this.context.tableIdentifier))) {
                        throw new SQL4ESException("Unknown data source identifier[" +
                                reference.setIdentifier + "] in group by clause.");
                    }
                    identifier = reference.columnIdentifier;
                    try {
                        context.typeDefinition.getFieldDefinition(identifier);
                    } catch (FieldNotExistsException e) {
                        throw new SQL4ESException(
                                "Column[" + reference + "] is not found in table[" +
                                        context.tableIdentifier + "]"
                        );
                    }
                    this.context.finallyGroupColumnIdentifierList.add(identifier);
                }
            }

            private void analyzeSelectClause() throws SQL4ESException {
                final ReadonlyList<ResultColumnDeclare> resultColumnDeclareList
                        = this.context.statement.selectClause.resultColumnDeclareList;
                ResultColumnDeclare resultColumnDeclare;
                Expression resultColumnExpression;
                Identifier resultColumnAlias;
                for (int index = 0, size = resultColumnDeclareList.size(); index < size; index++) {
                    resultColumnDeclare = resultColumnDeclareList.get(index);
                    resultColumnAlias = resultColumnDeclare.alias;
                    resultColumnExpression = resultColumnDeclare.expression;
                    if (resultColumnExpression instanceof Reference) {
                        final Reference reference = Reference.class.cast(resultColumnExpression);
                        if (!(reference.setIdentifier == null
                                || reference.setIdentifier.equals(this.context.tableAlias)
                                || reference.setIdentifier.equals(this.context.tableIdentifier))) {
                            throw new SQL4ESException("Unknown data source identifier[" + reference.setIdentifier +
                                    "] in group by clause.");
                        }
                        if (reference.columnIdentifier.equals(Reference.ALL_COLUMN_IDENTIFIER)) {
                            throw new SQL4ESException("Can not contains '*' in select clause for having aggregation expression.");
                        }
                        if (!this.context.existsGroupByClause
                                || !this.context.finallyGroupColumnIdentifierList.contains(reference.columnIdentifier)) {
                            throw new SQL4ESException(
                                    "Invalid column[" + reference.columnIdentifier + "] in select clause, it must in group by clause."
                            );
                        }
                        this.context.finallyResultColumnAliasList.add(
                                resultColumnAlias == null ? reference.columnIdentifier : resultColumnAlias
                        );
                        this.context.finallyResultColumnExpressionIndexMap.put(index, reference);
                        this.context.finallyGroupColumnResultSetIndexMap.put(reference.columnIdentifier, index);

                    } else if (resultColumnExpression instanceof Aggregation) {
                        final Aggregation aggregation;
                        if (resultColumnExpression instanceof CountFunction) {
                            aggregation = CountFunction.class.cast(resultColumnExpression);
                            final Expression operandExpression = aggregation.getOperandExpression(0);
                            if (!(operandExpression instanceof Reference)) {
                                throw new SQL4ESException(
                                        "Unsupported expression[" + operandExpression + "] in " +
                                                CountFunction.IDENTIFIER + "()."
                                );
                            }
                            final Reference reference = Reference.class.cast(operandExpression);
                            if (!reference.columnIdentifier.equals(Reference.ALL_COLUMN_IDENTIFIER)) {
                                try {
                                    this.context.typeDefinition.getFieldDefinition(reference.columnIdentifier);
                                } catch (FieldNotExistsException e) {
                                    throw new SQL4ESException(
                                            "Unknown column[" + reference.columnIdentifier + "] in " +
                                                    CountFunction.IDENTIFIER + "()."
                                    );
                                }
                            }
                        } else if (resultColumnExpression instanceof SummaryFunction) {
                            aggregation = SummaryFunction.class.cast(resultColumnExpression);
                            final Expression operandExpression = aggregation.getOperandExpression(0);
                            if (!(operandExpression instanceof Reference)) {
                                throw new SQL4ESException(
                                        "Unsupported expression[" + operandExpression + "] in " +
                                                SummaryFunction.IDENTIFIER + "()."
                                );
                            }
                            final Reference reference = Reference.class.cast(operandExpression);
                            if (reference.columnIdentifier.equals(Reference.ALL_COLUMN_IDENTIFIER)) {
                                throw new SQL4ESException(
                                        "Can not contains '*' in " +
                                                SummaryFunction.IDENTIFIER + "()."
                                );
                            }
                            try {
                                this.context.typeDefinition.getFieldDefinition(reference.columnIdentifier);
                            } catch (FieldNotExistsException e) {
                                throw new SQL4ESException(
                                        "Unknown column[" + reference.columnIdentifier + "] in " +
                                                SummaryFunction.IDENTIFIER + "()."
                                );
                            }

                        } else if (resultColumnExpression instanceof MaxinumFunction) {
                            aggregation = MaxinumFunction.class.cast(resultColumnExpression);
                            final Expression operandExpression = aggregation.getOperandExpression(0);
                            if (!(operandExpression instanceof Reference)) {
                                throw new SQL4ESException(
                                        "Unsupported expression[" + operandExpression + "] in " +
                                                MaxinumFunction.IDENTIFIER + "()."
                                );
                            }
                            final Reference reference = Reference.class.cast(operandExpression);
                            if (reference.columnIdentifier.equals(Reference.ALL_COLUMN_IDENTIFIER)) {
                                throw new SQL4ESException(
                                        "Can not contains '*' in " +
                                                MaxinumFunction.IDENTIFIER + "()."
                                );
                            }
                            try {
                                this.context.typeDefinition.getFieldDefinition(reference.columnIdentifier);
                            } catch (FieldNotExistsException e) {
                                throw new SQL4ESException(
                                        "Unknown column[" + reference.columnIdentifier + "]" +
                                                MaxinumFunction.IDENTIFIER + "()."
                                );
                            }
                        } else if (resultColumnExpression instanceof MininumFunction) {
                            aggregation = MininumFunction.class.cast(resultColumnExpression);
                            final Expression operandExpression = aggregation.getOperandExpression(0);
                            if (!(operandExpression instanceof Reference)) {
                                throw new SQL4ESException(
                                        "Unsupported expression[" + operandExpression + "] in " +
                                                MininumFunction.IDENTIFIER + "()."
                                );
                            }
                            final Reference reference = Reference.class.cast(operandExpression);
                            if (reference.columnIdentifier.equals(Reference.ALL_COLUMN_IDENTIFIER)) {
                                throw new SQL4ESException(
                                        "Can not contains '*' in " +
                                                MininumFunction.IDENTIFIER + "()."
                                );
                            }
                            try {
                                this.context.typeDefinition.getFieldDefinition(reference.columnIdentifier);
                            } catch (FieldNotExistsException e) {
                                throw new SQL4ESException(
                                        "Unknown column[" + reference.columnIdentifier + "]" +
                                                MininumFunction.IDENTIFIER + "()."
                                );
                            }
                        } else if (resultColumnExpression instanceof AverageFunction) {
                            aggregation = AverageFunction.class.cast(resultColumnExpression);
                            final Expression operandExpression = aggregation.getOperandExpression(0);
                            if (!(operandExpression instanceof Reference)) {
                                throw new SQL4ESException(
                                        "Unsupported expression[" + operandExpression + "] in " +
                                                AverageFunction.IDENTIFIER + "()."
                                );
                            }
                            final Reference reference = Reference.class.cast(operandExpression);
                            if (reference.columnIdentifier.equals(Reference.ALL_COLUMN_IDENTIFIER)) {
                                throw new SQL4ESException(
                                        "Can not contains '*' in " +
                                                AverageFunction.IDENTIFIER + "()."
                                );
                            }
                            try {
                                this.context.typeDefinition.getFieldDefinition(reference.columnIdentifier);
                            } catch (FieldNotExistsException e) {
                                throw new SQL4ESException(
                                        "Unknown column[" + reference.columnIdentifier + "]" +
                                                AverageFunction.IDENTIFIER + "()."
                                );
                            }
                        } else {
                            throw new SQL4ESException("Unsupported aggregation[" + resultColumnExpression + "]");
                        }
                        this.context.finallyResultColumnAliasList.add(
                                resultColumnAlias == null
                                        ? new Identifier(resultColumnExpression.toString()) : resultColumnAlias
                        );
                        this.context.finallyResultColumnExpressionIndexMap.put(index, aggregation);
                        this.context.finallyResultColumnAggregationIndexMap.put(aggregation, index);

                    } else if (resultColumnExpression instanceof Constant) {
                        final Constant constant = Constant.class.cast(resultColumnExpression);
                        this.context.finallyResultColumnAliasList.add(
                                resultColumnAlias == null ? new Identifier(constant.value.toString()) : resultColumnAlias
                        );
                        this.context.finallyResultColumnExpressionIndexMap.put(index, constant);
                        this.context.finallyResultColumnConstantIndexMap.put(index, constant);

                    } else {
                        throw new SQL4ESException("Unsupported expression[" + resultColumnExpression +
                                "] in select clause.");
                    }
                }
            }

            private void analyzeHavingClause() throws SQL4ESException {
                if (this.context.statement.havingClause != null) {
                    throw new SQL4ESException(
                            "For aggregation, having clause is not supported."
                    );
                }
            }

            private void analyzeOrderByClause() throws SQL4ESException {
                if (this.context.statement.orderByClause != null) {
                    throw new SQL4ESException(
                            "For aggregation, order by clause is not supported."
                    );
                }
            }

            private void analyzeLimitClause() throws SQL4ESException {
                if (this.context.statement.limitClause != null) {
                    throw new SQL4ESException(
                            "For aggregation, limit clause is not supported."
                    );
                }
            }

        }

    }

}
