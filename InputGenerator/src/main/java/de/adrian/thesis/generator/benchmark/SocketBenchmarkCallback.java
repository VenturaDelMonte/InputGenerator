package de.adrian.thesis.generator.benchmark;


public interface SocketBenchmarkCallback {
    void finishApplication(String reason);

    void waitingForReconnect();
}
