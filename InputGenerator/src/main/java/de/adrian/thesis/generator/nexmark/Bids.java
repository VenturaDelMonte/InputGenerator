package de.adrian.thesis.generator.nexmark;

import java.util.Random;

public class Bids {
    private static int BIDS_DISTRIBUTION_SIZE = 100;

    // Bids don't go away so lowChunk is always 0
    private int highChunk = 1;
    private int currentId = 0;
    private Random random = new Random(28349123);

    // creates the open auction instance as well as returning the new id
    public int getNewId() {
        int newId = currentId;
        currentId++;
        if (newId == highChunk * BIDS_DISTRIBUTION_SIZE) {
            highChunk++;
        }
        return newId;
    }

    public int getExistingId() {
        int id = random.nextInt(BIDS_DISTRIBUTION_SIZE);
        id += getRandomChunkOffset();
        return id % currentId;
    }

    private int getRandomChunkOffset() {
        int chunkId = random.nextInt(highChunk);
        return chunkId * BIDS_DISTRIBUTION_SIZE;
    }
}
