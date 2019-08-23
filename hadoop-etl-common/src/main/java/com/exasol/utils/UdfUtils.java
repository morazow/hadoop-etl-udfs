package com.exasol.utils;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.stream.Stream;
import javax.xml.bind.DatatypeConverter;

import com.exasol.ExaDataTypeException;
import com.exasol.ExaIterationException;
import com.exasol.ExaIterator;
import com.exasol.ExaMetadata;

public class UdfUtils {

    /**
     * Forward stdout to the an debug output service.
     */
    public static void attachToOutputService(String ip, int port) {
        // Start before: python udf_debug.py
        try {
            Socket socket = new Socket(ip, port);
            PrintStream out = new PrintStream(socket.getOutputStream(), true);
            System.setOut(out);
            System.out.println("\n\n\nAttached to outputservice");
        } catch (Exception ignored) {
            // could not start output server}
        }
    }

    /**
     * Convenience method to attach to output service listening on same
     * host as database node.
     */
    public static void attachToOutputServiceLocalHost() {
        try {
            String ip = InetAddress.getLocalHost().toString();
            ip = ip.substring(ip.indexOf("/") + 1);
            int port = 3000;
            attachToOutputService(ip, port);
        } catch (Exception ignored) {
            // could not start output server
        }
    }

    public static String traceToString(Exception ex) {
        StringWriter errors = new StringWriter();
        ex.printStackTrace(new PrintWriter(errors));
        return errors.toString();
    }

    public static byte[] base64ToByteArray(String base64Str) {
        return DatatypeConverter.parseBase64Binary(base64Str);
    }

    public static String writeTempFile(byte[] data, String path, String prefix, String suffix)
        throws Exception {
        File file = File.createTempFile(prefix, suffix, new File(path));
        file.deleteOnExit();
        FileOutputStream out = new FileOutputStream(file);
        out.write(data);
        out.close();
        return file.getCanonicalPath();
    }

    public static void printClassPath() {
        System.out.println("Classpath:");
        ClassLoader cl = ClassLoader.getSystemClassLoader();

        URL[] urls = urlsFromClassLoader(cl);

        for (URL url : urls) {
            System.out.println(". " + url.getFile());
        }
    }

    private static URL[] urlsFromClassLoader(ClassLoader classLoader) {
        if (classLoader instanceof URLClassLoader) {
            return ((URLClassLoader) classLoader).getURLs();
        }

        return Stream
            .of(ManagementFactory.getRuntimeMXBean().getClassPath().split(File.pathSeparator))
            .map(UdfUtils::toURL).toArray(URL[]::new);
    }

    private static URL toURL(String path) {
        try {
            return new File(path).toURI().toURL();
        } catch (MalformedURLException exception) {
            throw new IllegalArgumentException("URL could not be created from path "
                    + path, exception);
        }
    }

    public static Object getInstanceByName(String className)
        throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        Class<?> clazz = Class.forName(className);
        return clazz.newInstance();
    }

    public static String getOptionalStringParameter(
            ExaMetadata meta, ExaIterator iter, int paramIndex, String defaultValue)
        throws ExaIterationException, ExaDataTypeException {
        String val = defaultValue;
        if (meta.getInputColumnCount() > paramIndex) {
            if (iter.getString(paramIndex) != null && iter.getString(paramIndex).length() > 0) {
                val = iter.getString(paramIndex);
            }
        }
        return val;
    }
}
