package org.codefamily.crabs.jdbc.lang.expression.util;

import org.codefamily.crabs.util.ExtensionClassCollector;
import org.codefamily.crabs.jdbc.lang.Expression;
import org.codefamily.crabs.jdbc.lang.expression.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.*;

public final class ExtensionExpressionFactory {

    public static final class FunctionInformation {

        FunctionInformation(final String identifier,
                            final Class<? extends Expression> functionClass) {
            this.identifier = identifier;
            this.functionClass = functionClass;
        }

        public final String identifier;

        public final Class<? extends Expression> functionClass;

    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ExtensionExpressionFactory.class);

    private static final HashMap<Class<? extends Expression>, ExpressionInformation> EXTENSION_EXPRESSION_CLASS_INFORMATION_MAP
            = new HashMap<Class<? extends Expression>, ExpressionInformation>();

    private static final ArrayList<Class<? extends Expression>> EXTENSION_EXPRESSION_CLASS_LIST
            = new ArrayList<Class<? extends Expression>>();

    private static final ArrayList<FunctionInformation> FUNCTION_INFORMATION_LIST
            = new ArrayList<FunctionInformation>();

    static {
        recollectExtensionExpressionClasses();
    }

    @SuppressWarnings("unchecked")
    public static Class<? extends Expression>[] getExtensionExpressionClasses() {
        return EXTENSION_EXPRESSION_CLASS_LIST
                .toArray((Class<? extends Expression>[]) new Class<?>[EXTENSION_EXPRESSION_CLASS_INFORMATION_MAP
                        .size()]);
    }

    public static FunctionInformation[] getFunctionInformations() {
        return FUNCTION_INFORMATION_LIST
                .toArray(new FunctionInformation[FUNCTION_INFORMATION_LIST
                        .size()]);
    }

    public static <TExpression extends Expression> TExpression newExtensionExpression(
            final Class<TExpression> expressionClass,
            final Expression... operandExpressions) {
        if (expressionClass == null) {
            throw new IllegalArgumentException("Argument[expressionClass] is null.");
        }
        if (operandExpressions == null) {
            throw new IllegalArgumentException("Argument[operandExpressions] is null.");
        }
        final ExpressionInformation expressionInformation
                = EXTENSION_EXPRESSION_CLASS_INFORMATION_MAP.get(expressionClass);
        if (expressionInformation == null) {
            throw new IllegalArgumentException("Extension expression ["
                    + expressionClass.getName() + "] is unregistered.");
        }
        return expressionClass.cast(expressionInformation.constructExpression(operandExpressions));
    }

    public static void recollectExtensionExpressionClasses() {
        recollectExtensionExpressionClasses(null);
    }

    public static void recollectExtensionExpressionClasses(final ClassLoader classLoader) {
        synchronized (ExtensionExpressionFactory.class) {
            final Iterator<Class<? extends Expression>> expressionClassIterator;
            expressionClassIterator
                    = classLoader == null ? ExtensionClassCollector.getExtensionClasses(Expression.class)
                    : ExtensionClassCollector.getExtensionClasses(Expression.class, classLoader);
            while (expressionClassIterator.hasNext()) {
                final Class<? extends Expression> expressionClass = expressionClassIterator.next();
                if (EXTENSION_EXPRESSION_CLASS_INFORMATION_MAP.containsKey(expressionClass)) {
                    continue;
                }
                if (Function.class.isAssignableFrom(expressionClass)) {
                    try {
                        registerFunctionClass(
                                (String) (expressionClass.getDeclaredField("IDENTIFIER").get(null)),
                                expressionClass);
                    } catch (Throwable t) {
                        LOGGER.error("Can not register function class.", t);
                    }
                } else {
                    registerExpressionClass(expressionClass);
                }
            }
        }
    }

    private static void registerExpressionClass(final Class<? extends Expression> expressionClass) {
        if (!EXTENSION_EXPRESSION_CLASS_INFORMATION_MAP.containsKey(expressionClass)) {
            if (Modifier.isAbstract(expressionClass.getModifiers())) {
                throw new IllegalArgumentException(
                        "Expression class can not be an abstract class. ["
                                + expressionClass.getName() + "]"
                );
            }
            EXTENSION_EXPRESSION_CLASS_LIST.add(expressionClass);
            EXTENSION_EXPRESSION_CLASS_INFORMATION_MAP.put(
                    expressionClass,
                    new ExpressionInformation(expressionClass)
            );
        }
    }

    private static void registerFunctionClass(final String functionIdentifier,
                                              final Class<? extends Expression> expressionClass) {
        registerExpressionClass(expressionClass);
        FUNCTION_INFORMATION_LIST.add(new FunctionInformation(functionIdentifier, expressionClass));
    }

    private ExtensionExpressionFactory() {
        // to do nothing.
    }

    private static final class ExpressionInformation {

        @SuppressWarnings("unchecked")
        ExpressionInformation(final Class<? extends Expression> javaClass) {
            Constructor<? extends Expression> javaConstructor = null;
            int javaConstructorArgumentCount = 0;
            try {
                javaConstructor = javaClass.getConstructor(Expression[].class);
                javaConstructorArgumentCount = -1;
            } catch (SecurityException exception) {
                throw new RuntimeException(exception.getMessage(), exception);
            } catch (NoSuchMethodException exception) {
                for (Constructor<? extends Expression> constructor
                        : (Constructor<? extends Expression>[]) (javaClass.getConstructors())) {
                    final Class<?>[] parameterTypes = constructor.getParameterTypes();
                    for (Class<?> parameterType : parameterTypes) {
                        if (!Expression.class.isAssignableFrom(parameterType)) {
                            constructor = null;
                            break;
                        }
                    }
                    if (constructor != null) {
                        if (javaConstructor != null) {
                            throw new RuntimeException("There are to many constructors in expression class.");
                        } else {
                            javaConstructor = constructor;
                            javaConstructorArgumentCount = parameterTypes.length;
                        }
                    }
                }
                if (javaConstructor == null) {
                    throw new RuntimeException("There is no legal constructor in expression class.");
                }
            }
            this.javaConstructor = javaConstructor;
            this.javaConstructorArgumentCount = javaConstructorArgumentCount;
        }

        private final Constructor<? extends Expression> javaConstructor;

        private final int javaConstructorArgumentCount;

        final Expression constructExpression(final Expression[] operandExpressions) {
            try {
                if (this.javaConstructorArgumentCount >= 0) {
                    if (this.javaConstructorArgumentCount != operandExpressions.length) {
                        throw new IllegalArgumentException("Expect "
                                + this.javaConstructorArgumentCount
                                + " arguments. "
                                + this.javaConstructor.getDeclaringClass()
                                .getName());
                    }
                    return this.javaConstructor.newInstance((Object[]) operandExpressions);
                } else {
                    return this.javaConstructor.newInstance(new Object[]{operandExpressions});
                }
            } catch (IllegalArgumentException e) {
                throw new RuntimeException(e.getMessage(), e);
            } catch (InstantiationException e) {
                throw new RuntimeException(e.getMessage(), e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e.getMessage(), e);
            } catch (InvocationTargetException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

    }

}
