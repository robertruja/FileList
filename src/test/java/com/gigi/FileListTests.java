package com.gigi;

import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.Random;

public class FileListTests {

    FileList fileList;

    @Before
    public void init() {
        fileList = new FileListImpl(50, "C:\\tmp");
    }

    @Test
    public void TestUnique() {
//        File tmpDir = new File("C:\\tmp\\");
//        tmpDir.mkdir();
//
        File input = new File("C:\\tmp\\test_unique_input.txt");
//        Random rand = new Random(47);
//        try(BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(input)))) {
//            for (int i = 0; i < 20000000; i++) {
//                writer.write("word_" + rand.nextInt(10));
//                writer.newLine();
//            }
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }

        fileList.uniqueRows(input);

    }
}
