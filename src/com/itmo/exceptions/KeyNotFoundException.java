package com.itmo.exceptions;

public class KeyNotFoundException extends RuntimeException {
    private String message = "The specified key can not be found.";

    public KeyNotFoundException() {super();}
    public KeyNotFoundException(String message) {super(message);}

    @Override
    public String getMessage() {
        return message;
    }
}
