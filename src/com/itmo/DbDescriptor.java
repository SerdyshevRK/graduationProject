package com.itmo;

import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.file.Path;

public class DbDescriptor {
    Path filePath;
    Path keyFilePath;
    RandomAccessFile dbFile;
    RandomAccessFile keyFile;
    Field[] fields;

    public DbDescriptor(Path filePath, RandomAccessFile dbFile, Path keyFilePath, RandomAccessFile keyFile, Field[] fields) {
        this.filePath = filePath;
        this.dbFile = dbFile;
        this.fields = fields;
        this.keyFilePath = keyFilePath;
        this.keyFile = keyFile;
    }
}
