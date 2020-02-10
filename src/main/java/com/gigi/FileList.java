package com.gigi;

import java.io.File;

public interface FileList {

    File intersect(File input, File destination);

    File diff(File input, File destination);

}
