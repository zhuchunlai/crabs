package org.codefamily.crabs.jdbc.lang.expression.util;

import org.codefamily.crabs.common.util.ReadonlyList;
import org.codefamily.crabs.exception.SQL4ESException;
import org.codefamily.crabs.jdbc.lang.Expression;
import org.codefamily.crabs.jdbc.lang.expression.Aggregation;
import org.codefamily.crabs.jdbc.lang.expression.Argument;
import org.codefamily.crabs.jdbc.lang.expression.Reference;

import java.util.ArrayList;

public final class ExpressionHelper {

    public static Expression[] concatExpressionArrays(
            final Expression[]... arrays) {
        if (arrays == null) {
            return null;
        }
        int length = 0;
        for (Expression[] array : arrays) {
            if (array != null) {
                length += array.length;
            }
        }
        int position = 0;
        final Expression[] resultArray = new Expression[length];
        for (Expression[] array : arrays) {
            if (array != null && array.length > 0) {
                System.arraycopy(array, 0, resultArray, position, array.length);
                position += array.length;
            }
        }
        return resultArray;
    }

    /**
     * Get reference expressions in specified expression.
     *
     * @throws SQL4ESException
     */
    public static Reference[] getReferences(final Expression expression) throws SQL4ESException {
        if (expression == null) {
            throw new IllegalArgumentException("Argument [expession] is null.");
        }
        final ArrayList<Reference> referenceList = getEmptyReferenceList();
        doGetReference(referenceList, expression);
        return referenceList.toArray(new Reference[referenceList.size()]);
    }

    /**
     * Get reference expressions in specified expression.
     *
     * @throws SQL4ESException
     */
    public static void getReferences(final Expression expression,
                                     final ArrayList<Reference> referenceList) throws SQL4ESException {
        if (expression == null) {
            throw new IllegalArgumentException("Argument [expession] is null.");
        }
        if (referenceList == null) {
            throw new IllegalArgumentException(
                    "Argument [referenceList] is null.");
        }
        doGetReference(referenceList, expression);
    }

    /**
     * Get reference expressions in specified expression.
     *
     * @throws SQL4ESException
     */
    public static Aggregation[] getAggregations(final Expression expression) throws SQL4ESException {
        if (expression == null) {
            throw new IllegalArgumentException("Argument[expression] is null.");
        }
        final ArrayList<Aggregation> aggregationList = getEmptyAggregationList();
        doGetAggregation(aggregationList, expression);
        return aggregationList.toArray(new Aggregation[aggregationList.size()]);
    }

    /**
     * Get reference expressions in specified expression.
     *
     * @throws SQL4ESException
     */
    public static void getAggregations(final Expression expression,
                                       final ArrayList<Aggregation> aggregationList) throws SQL4ESException {
        if (expression == null) {
            throw new IllegalArgumentException("Argument[expression] is null.");
        }
        if (aggregationList == null) {
            throw new IllegalArgumentException(
                    "Argument [referenceList] is null.");
        }
        doGetAggregation(aggregationList, expression);
    }

    public static int getArgumentCount(final Expression expression) throws SQL4ESException {
        if (expression == null) {
            throw new IllegalArgumentException("Argument[expression] is null.");
        }
        return doGetArgumentCount(expression);
    }

    private static void doGetReference(final ArrayList<Reference> referenceList,
                                       final Expression expression) throws SQL4ESException {
        final Class<? extends Expression> expressionClass = expression.getClass();
        if (expressionClass == Reference.class) {
            referenceList.add((Reference) expression);
        } else {
            final ReadonlyList<Expression> operandExpressionList = expression.getOperandExpressionList();
            if (!operandExpressionList.isEmpty()) {
                for (int i = 0, operandExpressionCount = operandExpressionList.size(); i < operandExpressionCount; i++) {
                    doGetReference(referenceList, operandExpressionList.get(i));
                }
            }
        }
    }

    private static void doGetAggregation(final ArrayList<Aggregation> aggregationList,
                                         final Expression expression) throws SQL4ESException {
        if (expression instanceof Aggregation) {
            aggregationList.add((Aggregation) expression);
        } else {
            final ReadonlyList<Expression> operandExpressionList = expression.getOperandExpressionList();
            if (!operandExpressionList.isEmpty()) {
                for (int i = 0, operandExpressionCount = operandExpressionList.size(); i < operandExpressionCount; i++) {
                    doGetAggregation(aggregationList, operandExpressionList.get(i));
                }
            }
        }
    }

    private static int doGetArgumentCount(final Expression expression) throws SQL4ESException {
        if (expression == null) {
            throw new IllegalArgumentException("Argument[expression] is null.");
        }
        if (expression instanceof Argument) {
            return 1;
        } else {
            final ReadonlyList<Expression> operandExpressionList = expression.getOperandExpressionList();
            int argumentCount = 0;
            if (!operandExpressionList.isEmpty()) {
                for (int i = 0, operandExpressionCount = operandExpressionList
                        .size(); i < operandExpressionCount; i++) {
                    argumentCount += doGetArgumentCount(operandExpressionList
                            .get(i));
                }
            }
            return argumentCount;
        }
    }

    private static ArrayList<Reference> getEmptyReferenceList() {
        final ArrayList<Reference> referenceList = FACTORY$REFERENCE_LIST.get();
        referenceList.clear();
        return referenceList;
    }

    private static ArrayList<Aggregation> getEmptyAggregationList() {
        final ArrayList<Aggregation> aggregationList = FACTORY$AGGREGATION_LIST.get();
        aggregationList.clear();
        return aggregationList;
    }

    private static final ThreadLocal<ArrayList<Reference>> FACTORY$REFERENCE_LIST
            = new ThreadLocal<ArrayList<Reference>>() {

        @Override
        protected final ArrayList<Reference> initialValue() {
            return new ArrayList<Reference>();
        }

    };

    private static final ThreadLocal<ArrayList<Aggregation>> FACTORY$AGGREGATION_LIST
            = new ThreadLocal<ArrayList<Aggregation>>() {

        @Override
        protected final ArrayList<Aggregation> initialValue() {
            return new ArrayList<Aggregation>();
        }

    };

    private ExpressionHelper() {
        // to do nothing.
    }

}
