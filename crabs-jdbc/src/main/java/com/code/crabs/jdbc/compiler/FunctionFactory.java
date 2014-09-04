package com.code.crabs.jdbc.compiler;

import com.code.crabs.jdbc.lang.Expression;
import com.code.crabs.jdbc.lang.expression.util.ExtensionExpressionFactory;

import java.util.ArrayList;
import java.util.HashSet;

public final class FunctionFactory {

    private static final int FUNCTION_MAP_CAPACITY = 'Z' - 'A' + 1;

    private static final FunctionMapEntry[][] FUNCTION_MAP = new FunctionMapEntry[FUNCTION_MAP_CAPACITY][];

    private static final HashSet<Class<?>> FUNCTION_CLASS_SET = new HashSet<Class<?>>();

    static {
        refreshFunctionClasses();
    }

    public static void refreshFunctionClasses() {
        synchronized (FunctionFactory.class) {
            @SuppressWarnings("unchecked")
            final ArrayList<FunctionMapEntry>[] functionMap = new ArrayList[FUNCTION_MAP_CAPACITY];
            for (ExtensionExpressionFactory.FunctionInformation functionInformation
                    : ExtensionExpressionFactory.getFunctionInformations()) {
                final Class<?> functionClass = functionInformation.functionClass;
                if (FUNCTION_CLASS_SET.contains(functionClass)) {
                    continue;
                }
                final FunctionMapEntry functionMapEntry = new FunctionMapEntry(functionInformation);
                final char[] key = functionMapEntry.key;
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
                    throw new RuntimeException(
                            "Function identifier contain illegal character. "
                                    + functionInformation.identifier);
                }
                final int mapIndex = (key[0] - 'A') & (FUNCTION_MAP_CAPACITY - 1);
                ArrayList<FunctionMapEntry> functionMapEntryList = functionMap[mapIndex];
                if (functionMapEntryList == null) {
                    functionMapEntryList = new ArrayList<FunctionMapEntry>();
                    functionMap[mapIndex] = functionMapEntryList;
                }
                functionMapEntryList.add(functionMapEntry);
                FUNCTION_CLASS_SET.add(functionClass);
            }
            for (int i = 0; i < FUNCTION_MAP_CAPACITY; i++) {
                final ArrayList<FunctionMapEntry> functionMapEntryList = functionMap[i];
                if (functionMapEntryList != null) {
                    FUNCTION_MAP[i] = functionMapEntryList.toArray(
                            new FunctionMapEntry[functionMapEntryList.size()]
                    );
                }
            }
        }
    }

    static Class<? extends Expression> tryGetFunctionClass(
            final String identifier) {
        final int identifierLength = identifier.length();
        if (identifierLength != 0) {
            final int mapIndex = (Character.toUpperCase(identifier.charAt(0)) - 'A') & (FUNCTION_MAP_CAPACITY - 1);
            if (mapIndex < FUNCTION_MAP_CAPACITY) {
                final FunctionMapEntry[] functionMapEntries = FUNCTION_MAP[mapIndex];
                if (functionMapEntries != null) {
                    for (int i = 0; i < functionMapEntries.length; i++) {
                        final FunctionMapEntry functionMapEntry = functionMapEntries[i];
                        final char[] key = functionMapEntry.key;
                        if (key.length == identifierLength) {
                            compare:
                            {
                                for (int j = 0; j < identifierLength; j++) {
                                    char currentCharacter = identifier.charAt(j);
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
                                    if (key[j] != currentCharacter) {
                                        break compare;
                                    }
                                }
                                return functionMapEntry.functionInformation.functionClass;
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    private FunctionFactory() {
        // to do nothing.
    }

    private static final class FunctionMapEntry {

        FunctionMapEntry(final ExtensionExpressionFactory.FunctionInformation functionInformation) {
            this.key = functionInformation.identifier.toUpperCase().toCharArray();
            this.functionInformation = functionInformation;
        }

        final char[] key;

        final ExtensionExpressionFactory.FunctionInformation functionInformation;

    }

}
