package com.itmo.exceptions;
import java.io.EOFException;

public class EndOfFileException extends EOFException {
    public EndOfFileException(){super();}
    public EndOfFileException(String message) {
        super(message);
    }
}
