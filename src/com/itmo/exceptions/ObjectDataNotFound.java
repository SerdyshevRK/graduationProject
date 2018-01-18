package com.itmo.exceptions;

public class ObjectDataNotFound extends RuntimeException {
    private String message = "Can not find the specified data.";

    public ObjectDataNotFound() {super();}
    public ObjectDataNotFound(String message) {super(message);}

    @Override
    public String getMessage() {
        return message;
    }
}
