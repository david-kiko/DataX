package com.alibaba.datax.core.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class FileUtil {

    private static final Logger log = LoggerFactory.getLogger(FileUtil.class);

    /**
     * read file content to string.
     *
     * @param fileName str.
     * @return filecontent file content.
     */
    public static String readToString(String fileName) throws Exception {
        if (!fileExists(fileName)) {
            log.error(String.format("The file: %s not exists.", fileName));
            throw new Exception(String.format("The file: %s not exists.", fileName));
        }
        String encoding = "UTF-8";
        File file = new File(fileName);
        Long filelength = file.length();
        byte[] filecontent = new byte[filelength.intValue()];
        FileInputStream in = null;
        try {
            in = new FileInputStream(file);
            int bytes = in.read(filecontent);
            if (bytes <= 0) {
                log.error("there is no data in " + fileName);
            }
        } catch (IOException e) {
            log.error("close filestream has error.", e);
        } finally {
            if (null != in) {
                try {
                    in.close();
                } catch (IOException e) {
                    log.error("close filestream has error.", e);
                }
            }
        }
        try {
            return new String(filecontent, encoding);
        } catch (UnsupportedEncodingException e) {
            log.error("The OS does not support " + encoding, e);
            throw new Exception(String.format("The OS does not support %s%s", encoding, e.getMessage()));
        }
    }

    /**
     * given a file name with a boolean
     * indicating whether or not to append the data written.
     *
     * @param fileName str.
     * @param content  str.
     * @param append   Boolean.
     */
    public static void writeFile(String fileName, String content, Boolean append) throws Exception {
        FileWriter writer = null;
        try {
            File file = new File(fileName);
            if (!file.exists()) {
                boolean result = file.createNewFile();
                if (!result) {
                    file = new File(fileName);
                    if (!file.exists()) {
                        log.error(String.format("createNewFile %s failed", fileName));
                        throw new Exception(String.format("createNewFile %s failed", fileName));
                    }
                }
                writer = new FileWriter(fileName);
            } else {
                writer = new FileWriter(fileName, append);
            }
            writer.write(content);
        } catch (IOException e) {
            String message = String.format("write file: %s has error, content: %s", fileName, content);
            log.error(message, e);
            throw new Exception(message);
        } finally {
            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (IOException e) {
                log.error("close filestream has error.", e);
            }
        }
    }

    /**
     * judge file exists.
     *
     * @param fileName str.
     * @return fileExist true if file exists ;
     */
    public static boolean fileExists(String fileName) {
        if (fileName == null) {
            return false;
        } else {
            File file = new File(fileName);
            return file.isFile();
        }
    }
}
