package com.github.raftimpl.raft.storage;

import com.github.raftimpl.raft.proto.RaftProto;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a single segment of the Raft log.
 * Each segment is stored in a separate file and contains a contiguous range of log entries.
 */
public class Segment {

    /**
     * Represents a single record within the segment, containing a log entry and its file offset.
     */
    public static class Record {
        /** The offset of this record in the segment file. */
        public long offset;
        /** The actual log entry. */
        public RaftProto.LogEntry entry;

        /**
         * Constructs a new Record.
         * @param offset The offset of the record in the file.
         * @param entry The log entry.
         */
        public Record(long offset, RaftProto.LogEntry entry) {
            this.offset = offset;
            this.entry = entry;
        }
    }

    /** Indicates whether this segment can be written to. */
    private boolean canWrite;
    /** The index of the first log entry in this segment. */
    private long startIndex;
    /** The index of the last log entry in this segment. */
    private long endIndex;
    /** The current size of the segment file in bytes. */
    private long fileSize;
    /** The name of the file storing this segment. */
    private String fileName;
    /** The file handle for reading/writing this segment. */
    private RandomAccessFile randomAccessFile;
    /** List of all records in this segment. */
    private List<Record> entries = new ArrayList<>();

    /**
     * Retrieves a specific log entry from this segment.
     * @param index The index of the log entry to retrieve.
     * @return The log entry if found, null otherwise.
     */
    public RaftProto.LogEntry getEntry(long index) {
        if (startIndex == 0 || endIndex == 0) {
            return null;
        }
        if (index < startIndex || index > endIndex) {
            return null;
        }
        int indexInList = (int) (index - startIndex);
        return entries.get(indexInList).entry;
    }

    // Getters and setters

    public boolean isCanWrite() {
        return canWrite;
    }

    public void setCanWrite(boolean canWrite) {
        this.canWrite = canWrite;
    }

    public long getStartIndex() {
        return startIndex;
    }

    public void setStartIndex(long startIndex) {
        this.startIndex = startIndex;
    }

    public long getEndIndex() {
        return endIndex;
    }

    public void setEndIndex(long endIndex) {
        this.endIndex = endIndex;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public RandomAccessFile getRandomAccessFile() {
        return randomAccessFile;
    }

    public void setRandomAccessFile(RandomAccessFile randomAccessFile) {
        this.randomAccessFile = randomAccessFile;
    }

    public List<Record> getEntries() {
        return entries;
    }

    public void setEntries(List<Record> entries) {
        this.entries = entries;
    }
}