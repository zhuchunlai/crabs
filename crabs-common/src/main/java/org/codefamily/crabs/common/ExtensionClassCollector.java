package org.codefamily.crabs.common;

import org.codefamily.crabs.exception.SQL4ESError;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.*;

public final class ExtensionClassCollector {

    public static final String REGISTER_FILE_PATH = "META-INF/services/";

    public static <TSuperClass> Iterator<Class<? extends TSuperClass>> getExtensionClasses(
            final Class<TSuperClass> superClass) throws SQL4ESError {
        return new LazyIterator<TSuperClass>(
                superClass,
                Thread.currentThread().getContextClassLoader()
        );
    }

    public static <TSuperClass> Iterator<Class<? extends TSuperClass>> getExtensionClasses(
            final Class<TSuperClass> superClass, final ClassLoader loader) throws SQL4ESError {
        return new LazyIterator<TSuperClass>(superClass, loader);
    }

    private static Iterator<String> parse(final Class<?> superClass,
                                          final URL URL,
                                          final TreeSet<String> classNameSet) throws SQL4ESError {
        final ArrayList<String> classNameList = new ArrayList<String>();
        try {
            final InputStream inputStream = URL.openStream();
            try {
                final BufferedReader buffer = new BufferedReader(
                        new InputStreamReader(inputStream, "utf-8"));
                try {
                    int lineNumber = 1;
                    while ((lineNumber = parseLine(superClass, URL, buffer,
                            lineNumber, classNameList, classNameSet)) >= 0) {
                        // to do nothing.
                    }
                } finally {
                    buffer.close();
                }
            } finally {
                inputStream.close();
            }
        } catch (IOException exception) {
            throw new SQL4ESError(superClass.getName() + ": " + exception);
        }
        return classNameList.iterator();
    }

    private static int parseLine(final Class<?> superClass,
                                 final URL URL,
                                 final BufferedReader buffer,
                                 final int lineNumber,
                                 final ArrayList<String> classNameList,
                                 final TreeSet<String> classNameSet) throws SQL4ESError {
        String line;
        try {
            line = buffer.readLine();
        } catch (IOException exception) {
            throw new SQL4ESError(superClass.getName() + ": "
                    + exception.getMessage());
        }
        if (line == null) {
            return -1;
        }
        int index = line.indexOf('#');
        if (index >= 0) {
            line = line.substring(0, index);
        }
        line = line.trim();
        final int lineLength = line.length();
        if (lineLength != 0) {
            if ((line.indexOf(' ') >= 0) || (line.indexOf('\t') >= 0)) {
                throw new SQL4ESError(superClass.getName() + ": "
                        + URL.toString() + ": " + lineNumber
                        + ": Illegal configuration-file syntax");
            }
            int codePoint = line.codePointAt(0);
            if (!Character.isJavaIdentifierStart(codePoint)) {
                throw new SQL4ESError(superClass.getName() + ": "
                        + URL.toString() + ": " + lineNumber
                        + ": Illegal extension-class name: " + line);
            }
            for (int i = Character.charCount(codePoint); i < lineLength; i += Character
                    .charCount(codePoint)) {
                codePoint = line.codePointAt(i);
                if (!Character.isJavaIdentifierPart(codePoint)
                        && (codePoint != '.')) {
                    throw new SQL4ESError(superClass.getName() + ": "
                            + URL.toString() + ": " + lineNumber
                            + ": Illegal extension-class name: " + line);
                }
            }
            if (!classNameSet.contains(line)) {
                classNameList.add(line);
                classNameSet.add(line);
            }
        }
        return lineNumber + 1;
    }

    private ExtensionClassCollector() {
        // to do nothing.
    }

    private static final class LazyIterator<TSuperClass> implements Iterator<Class<? extends TSuperClass>> {

        LazyIterator(final Class<TSuperClass> superClass,
                     final ClassLoader classLoader) {
            this.superClass = superClass;
            this.classLoader = classLoader;
        }

        private final Class<TSuperClass> superClass;

        private final ClassLoader classLoader;

        private final TreeSet<String> classNameSet = new TreeSet<String>();

        private Iterator<String> classNameIterator = null;

        private String nextClassName = null;

        private Enumeration<URL> configurationFiles = null;

        public final boolean hasNext() throws SQL4ESError {
            if (this.nextClassName != null) {
                return true;
            }
            if (this.configurationFiles == null) {
                try {
                    final String fullName = REGISTER_FILE_PATH + this.superClass.getName();
                    if (this.classLoader == null) {
                        this.configurationFiles = ClassLoader
                                .getSystemResources(fullName);
                    } else {
                        this.configurationFiles = this.classLoader
                                .getResources(fullName);
                    }
                } catch (IOException exception) {
                    throw new SQL4ESError(this.superClass.getName() + ": "
                            + exception.getMessage());
                }
            }
            while ((this.classNameIterator == null)
                    || !this.classNameIterator.hasNext()) {
                if (!this.configurationFiles.hasMoreElements()) {
                    return false;
                }
                this.classNameIterator = parse(
                        this.superClass,
                        this.configurationFiles.nextElement(),
                        this.classNameSet
                );
            }
            this.nextClassName = this.classNameIterator.next();
            return true;
        }

        @SuppressWarnings("unchecked")
        public final Class<? extends TSuperClass> next() throws SQL4ESError {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final String className = this.nextClassName;
            this.nextClassName = null;
            final Class<?> clazz;
            try {
                clazz = Class.forName(className, true, this.classLoader);
            } catch (ClassNotFoundException exception) {
                throw new SQL4ESError(this.superClass.getName() + ": "
                        + "Extension " + className + " not found");
            }
            if (this.superClass.isAssignableFrom(clazz)) {
                return (Class<? extends TSuperClass>) clazz;
            } else {
                throw new SQL4ESError(this.superClass.getName() + ": "
                        + "Extension " + className
                        + " is not a subclass for superclass. ");
            }
        }

        public final void remove() {
            throw new UnsupportedOperationException();
        }

    }

}
