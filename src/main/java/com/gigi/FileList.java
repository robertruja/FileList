package com.gigi;

import java.io.File;

public interface FileList {

    File intersect(File input, File destination);
    File uniqueRows(File input);
    File diff(File input, File destination);

}
