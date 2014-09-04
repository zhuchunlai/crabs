package org.codefamily.crabs.jdbc.lang.extension.clause;

import org.codefamily.crabs.common.util.ReadonlyList;
import org.codefamily.crabs.jdbc.lang.Clause;
import org.codefamily.crabs.jdbc.lang.Expression;
import org.codefamily.crabs.jdbc.lang.Keyword;
import org.codefamily.crabs.jdbc.lang.extension.ReservedKeyword;

public final class OrderByClause extends Clause {

    public static final ReadonlyList<Keyword> PREFIX_KEYWORD_LIST
            = ReadonlyList.newInstance((Keyword) ReservedKeyword.ORDER, (Keyword) ReservedKeyword.BY);

    public OrderByClause(final OrderSpecification... specifications) {
        super(PREFIX_KEYWORD_LIST);
        if (specifications == null) {
            throw new IllegalArgumentException("Argument[specifications] is null.");
        }
        for (int i = 0; i < specifications.length; i++) {
            if (specifications[i] == null) {
                throw new IllegalArgumentException("Argument[specifications[" + i + "]] is null.");
            }
        }
        this.orderSpecificationList = ReadonlyList.newInstance(specifications.clone());
    }

    public final ReadonlyList<OrderSpecification> orderSpecificationList;

    private ReadonlyList<Expression> orderExpressionList;

    public final ReadonlyList<Expression> getOrderExpressionList() {
        final ReadonlyList<Expression> orderExpressionList = this.orderExpressionList;
        if (orderExpressionList == null) {
            final ReadonlyList<OrderSpecification> resultColumnDeclareList = this.orderSpecificationList;
            final int resultColumnCount = resultColumnDeclareList.size();
            final Expression[] orderExpressions = new Expression[resultColumnCount];
            for (int i = 0; i < resultColumnCount; i++) {
                orderExpressions[i] = resultColumnDeclareList.get(i).expression;
            }
            this.orderExpressionList = ReadonlyList.newInstance(orderExpressions);
        }
        return orderExpressionList;
    }

    @Override
    public final String toString() {
        final ReadonlyList<OrderSpecification> orderSpecificationList = this.orderSpecificationList;
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(this.getPrefixKeywordsString());
        if (orderSpecificationList.size() > 0) {
            stringBuilder.append(' ');
            stringBuilder.append(orderSpecificationList.get(0).toString());
            for (int i = 1, orderSpecificationCount = orderSpecificationList.size();
                 i < orderSpecificationCount; i++) {
                stringBuilder.append(", ");
                stringBuilder.append(orderSpecificationList.get(i).toString());
            }
        }
        return stringBuilder.toString();
    }

    @Override
    public final boolean equals(final Object object) {
        if (object != null && object instanceof OrderByClause) {
            if (object == this) {
                return true;
            }
            final OrderByClause that = (OrderByClause) object;
            final ReadonlyList<OrderSpecification> thisOrderSpecificationList = this.orderSpecificationList;
            final ReadonlyList<OrderSpecification> thatOrderSpecificationList = that.orderSpecificationList;
            if (thisOrderSpecificationList.size() == thatOrderSpecificationList.size()) {
                for (int i = 0, thisOrderSpecificationCount = thisOrderSpecificationList.size();
                     i < thisOrderSpecificationCount; i++) {
                    if (!thisOrderSpecificationList.get(i).equals(thatOrderSpecificationList.get(i))) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    public static final class OrderSpecification {

        public OrderSpecification(final Expression expression,
                                  final boolean ascendingOrder) {
            if (expression == null) {
                throw new IllegalArgumentException("Argument[expression] is null.");
            }
            this.expression = expression;
            this.ascendingOrder = ascendingOrder;
        }

        public final Expression expression;

        public final boolean ascendingOrder;

        @Override
        public final String toString() {
            return this.expression.toString() + " "
                    + (this.ascendingOrder ? "ASC" : "DESC");
        }

        @Override
        public final boolean equals(final Object object) {
            if (object != null && object instanceof OrderSpecification) {
                final OrderSpecification that = (OrderSpecification) object;
                return this.expression.equals(that.expression)
                        && this.ascendingOrder == that.ascendingOrder;
            }
            return false;
        }

    }

}
