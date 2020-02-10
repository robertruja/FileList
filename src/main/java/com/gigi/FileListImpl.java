package com.gigi;

import java.io.*;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class FileListImpl implements FileList {

    private static final int MEGABYTES = 1048576;
    private long maxBytes;
    private String chunk = "chunk_";

    private static final String tempPath = "C:\\tmp\\";

    public FileListImpl(int maxMemory) { // megs
        maxBytes = MEGABYTES * maxMemory;
    }

    public File intersect(File leftFile, File rightFile) {

        fileExists(leftFile);
        fileExists(rightFile);

        File leftChunksDir = splitFile(leftFile);
        File rightChunksDir = splitFile(rightFile);
        File destination = new File(tempPath + "\\" + leftFile.getName() + "_" + rightFile.getName() + ".out");
        try(BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(destination)))) {
            for (File leftChunk : leftChunksDir.listFiles()) {
                Set<String> leftLines = loadFromFile(leftChunk);
                for (File rightChunk : rightChunksDir.listFiles()) {
                    Set<String> rightLines = loadFromFile(rightChunk);
                    leftLines.parallelStream().forEach(left -> {
                        if(rightLines.contains(left)) {
                            try {
                                synchronized (this) {
                                    writer.write(left);
                                    writer.newLine();
                                }
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
                }
            }

        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        deleteDir(leftChunksDir); // cleanup
        deleteDir(rightChunksDir); // cleanup
        return destination;
    }

    public File diff(File input, File destination) {
        return null;
    }

    private void fileExists(File file) {
        if(!file.exists()) {
            throw new RuntimeException("File " + file.getAbsolutePath() + " does not exist");
        }
    }

    // returns temp dir
    private File splitFile(File file) {
        System.out.println("Preparing file ...");
        new File(tempPath).mkdir();
        String filename = file.getName();
        String tmpFileDirPath = tempPath + "\\" + filename + "_tmp";
        String tmpChunkFilePathPrefix = tmpFileDirPath + "\\" + chunk;
        File dir = new File(tmpFileDirPath);
        dir.mkdir();

        try(BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
            String line;
            long readBytes = 0;
            int chunkNr = 0;

            BufferedWriter writer = newChunk(tmpChunkFilePathPrefix + chunkNr);
            while((line = reader.readLine()) != null) {
                writer.write(line);
                writer.newLine();
                readBytes = readBytes + line.getBytes().length;
                if(readBytes > maxBytes) {
                    writer.close();
                    writer = newChunk(tmpChunkFilePathPrefix + ++chunkNr);
                    readBytes = 0;
                }
            }
            writer.close();
            System.out.println("Done preparing file");
            return dir;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private boolean deleteDir(File dir) {
        File[] allContents = dir.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDir(file);
            }
        }
        return dir.delete();
    }

    private BufferedWriter newChunk(String path) throws FileNotFoundException {
        File chunk = new File(path);
        return new BufferedWriter(new OutputStreamWriter(new FileOutputStream(chunk)));
    }

    private Set<String> loadFromFile(File file) {
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
            Set<String> res = new HashSet<>();
            String line;
            while((line = reader.readLine()) != null) {
                res.add(line);
            }
            return res;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }


    public static void main(String[] args) throws Exception {
        File file1 = new File("C:\\tmp\\test.txt");
        File file2 = new File("C:\\tmp\\test2.txt");
//        try(BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))){
//            for (int i = 500000; i < 1500000; i++) {
//                if(i%100 == 0) {
//                    writer.write("test_word_" + i);
//                    writer.newLine();
//                }
//            }
//        } catch (Exception ex) {
//            throw new RuntimeException(ex);
//        }

        FileList fileList = new FileListImpl(1);
        fileList.intersect(file2, file1);
    }
}
