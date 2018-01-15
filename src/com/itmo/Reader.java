package com.itmo;

import java.io.RandomAccessFile;

public class Reader {
    private boolean busy;
    private RandomAccessFile reader;

    public Reader(RandomAccessFile reader) {
        busy = false;
        this.reader = reader;
    }

    public RandomAccessFile getReader() {
        return reader;
    }

    public boolean isBusy() {
        return busy;
    }

    public void lock() {
        this.busy = true;
    }

    public void release() {
        this.busy = false;
    }
}
