package de.adrian.thesis.generator.benchmark.javaio;


public interface SocketBenchmarkCallback {
    void finishApplication(String reason);

    void waitingForReconnect();
}
