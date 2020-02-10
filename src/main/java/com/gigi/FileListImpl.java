package com.gigi;

import java.io.*;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class FileListImpl implements FileList {

    private static final int MEGABYTES = 1048576;
    private long maxBytes;
    private String chunk = "chunk_";

    private String tempPath;

    public FileListImpl(int maxMemory, String tempPath) { // megs
        maxBytes = MEGABYTES * maxMemory;
        this.tempPath = tempPath;
    }

    public File intersect(File leftFile, File rightFile) {

        fileExists(leftFile);
        fileExists(rightFile);

        File leftChunksDir = splitFile(leftFile);
        File rightChunksDir = splitFile(rightFile);
        File destination = new File(tempPath + "\\" + leftFile.getName() + "_" + rightFile.getName() + ".intersect.out");
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

    public File uniqueRows(File input) {
        fileExists(input);
        File chunksDir = splitFile(input);
        File destination = new File(tempPath + "\\" + input.getName() + ".unique.out");
        try(BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(destination)))) {
            File[] allFiles = chunksDir.listFiles();
            Set<String> left = null;
            for (int i = 0; i < allFiles.length; i++) {
                left = loadFromFile(allFiles[i]);
                for (int j = i + 1; j < allFiles.length; j++) {
                    Set<String> right = loadFromFile(allFiles[j]);
                    right.forEach(left::remove);
                }
                writeLines(left, writer);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        deleteDir(chunksDir); // cleanup
        return destination;
    }

    public void writeLines(Collection<String> lines, BufferedWriter writer) {
        lines.forEach(line -> {
            try {
                writer.write(line);
                writer.newLine();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
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
        FileList fileList = new FileListImpl(250, "E:\\words\\dict\\out\\tmp");
        fileList.intersect(new File("E:\\words\\dict\\out\\raw.txt"), new File("E:\\words\\dict\\out\\tmp\\sub_rockyou.txt.unique.out"));
    }
}
