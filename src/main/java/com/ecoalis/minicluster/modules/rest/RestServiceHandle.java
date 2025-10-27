package com.ecoalis.minicluster.modules.rest;


import io.javalin.Javalin;

public final class RestServiceHandle implements AutoCloseable {

    private final Javalin app;
    private final int port;

    public RestServiceHandle(Javalin app, int port) {
        this.app = app;
        this.port = port;
    }

    public int getPort() {
        return port;
    }

    public void stop() {
        app.stop();
    }

    @Override
    public void close() {
        stop();
    }
}