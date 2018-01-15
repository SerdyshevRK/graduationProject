package com.itmo;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DbDescriptor {
    Lock lock = new ReentrantLock();
    Path filePath;
    Path keyFilePath;
    Reader[] readers = new Reader[10];
    RandomAccessFile dbFileWriter;
    RandomAccessFile keyFileWriter;
    Field[] fields;

    public DbDescriptor(Path filePath,
                        RandomAccessFile dbFile,
                        Path keyFilePath,
                        RandomAccessFile keyFile,
                        Field[] fields) throws FileNotFoundException {
        this.filePath = filePath;
        this.dbFileWriter = dbFile;
        this.fields = fields;
        this.keyFilePath = keyFilePath;
        this.keyFileWriter = keyFile;
        openReaders();
    }

    private void openReaders() throws FileNotFoundException {
        for (int i = 0; i < readers.length; i++) {
            readers[i] = new Reader(new RandomAccessFile(filePath.toFile(), "r"));
        }
    }

    public Reader getReader() {
        lock.lock();
        for (Reader reader : readers) {
            if (!reader.isBusy()) {
                reader.lock();
                lock.unlock();
                return reader;
            }
        }
        lock.unlock();
        return null;
    }

    public void releaseReader(Reader reader) {
        reader.release();
    }
}
