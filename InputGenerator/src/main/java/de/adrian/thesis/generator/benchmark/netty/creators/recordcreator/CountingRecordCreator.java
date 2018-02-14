package de.adrian.thesis.generator.benchmark.netty.creators.recordcreator;

public class CountingRecordCreator implements RecordCreator {
    @Override
    public String createRecord(long number) {
        return "Element#" + number;
    }
}
