package com.github.raftimpl.raft.storage;

import com.github.raftimpl.raft.proto.RaftProto;
import com.github.raftimpl.raft.util.RaftFileUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The Snapshot class manages the snapshot functionality for the Raft consensus algorithm. It
 * handles snapshot metadata, data files, and operations related to taking and installing
 * snapshots.
 */
public class Snapshot {

    /**
     * Represents a snapshot data file with its file name and RandomAccessFile.
     */
    public class SnapshotDataFile {

        public String fileName;
        public RandomAccessFile randomAccessFile;
    }

    private static final Logger LOG = LoggerFactory.getLogger(Snapshot.class);
    private String snapshotDir;
    private RaftProto.SnapshotMetaData metaData;

    // Indicates whether a snapshot is currently being installed (leader to follower)
    private AtomicBoolean isInstallSnapshot = new AtomicBoolean(false);

    // Indicates whether the node is currently taking a snapshot of its state machine
    private AtomicBoolean isTakeSnapshot = new AtomicBoolean(false);

    private Lock lock = new ReentrantLock();

    /**
     * Constructs a new Snapshot instance.
     *
     * @param raftDataDir The base directory for Raft data
     */
    public Snapshot(String raftDataDir) {
        this.snapshotDir = raftDataDir + File.separator + "snapshot";
        String snapshotDataDir = snapshotDir + File.separator + "data";
        // Create the snapshot directory if it doesn't exist
        File file = new File(snapshotDataDir);
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    /**
     * Reloads the snapshot metadata from disk.
     */
    public void reload() {
        metaData = this.readMetaData();
        if (metaData == null) {
            metaData = RaftProto.SnapshotMetaData.newBuilder().build();
        }
    }

    /**
     * Opens all snapshot data files in the snapshot data directory. If a file is a symlink, it
     * opens the actual file handle.
     *
     * @return A TreeMap of file names to SnapshotDataFile objects
     */
    public TreeMap<String, SnapshotDataFile> openSnapshotDataFiles() {
        TreeMap<String, SnapshotDataFile> snapshotDataFileMap = new TreeMap<>();
        String snapshotDataDir = snapshotDir + File.separator + "data";
        try {
            Path snapshotDataPath = FileSystems.getDefault().getPath(snapshotDataDir);
            snapshotDataPath = snapshotDataPath.toRealPath();
            snapshotDataDir = snapshotDataPath.toString();
            List<String> fileNames = RaftFileUtils.getSortedFilesInDirectory(snapshotDataDir,
                snapshotDataDir);
            for (String fileName : fileNames) {
                RandomAccessFile randomAccessFile = RaftFileUtils.openFile(snapshotDataDir,
                    fileName, "r");
                SnapshotDataFile snapshotFile = new SnapshotDataFile();
                snapshotFile.fileName = fileName;
                snapshotFile.randomAccessFile = randomAccessFile;
                snapshotDataFileMap.put(fileName, snapshotFile);
            }
        } catch (IOException ex) {
            LOG.warn("readSnapshotDataFiles exception:", ex);
            throw new RuntimeException(ex);
        }
        return snapshotDataFileMap;
    }

    /**
     * Closes all opened snapshot data files.
     *
     * @param snapshotDataFileMap A TreeMap of file names to SnapshotDataFile objects
     */
    public void closeSnapshotDataFiles(TreeMap<String, SnapshotDataFile> snapshotDataFileMap) {
        for (Map.Entry<String, SnapshotDataFile> entry : snapshotDataFileMap.entrySet()) {
            try {
                entry.getValue().randomAccessFile.close();
            } catch (IOException ex) {
                LOG.warn("close snapshot files exception:", ex);
            }
        }
    }

    /**
     * Reads the snapshot metadata from disk.
     *
     * @return The SnapshotMetaData object, or null if the file doesn't exist
     */
    public RaftProto.SnapshotMetaData readMetaData() {
        String fileName = snapshotDir + File.separator + "metadata";
        File file = new File(fileName);
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            return RaftFileUtils.readProtoFromFile(randomAccessFile,
                RaftProto.SnapshotMetaData.class);
        } catch (IOException ex) {
            LOG.warn("meta file not exist, name={}", fileName);
            return null;
        }
    }

    /**
     * Updates the snapshot metadata on disk.
     *
     * @param dir               The directory to write the metadata file
     * @param lastIncludedIndex The last log index included in the snapshot
     * @param lastIncludedTerm  The term of the last included log entry
     * @param configuration     The current cluster configuration
     */
    public void updateMetaData(String dir,
        Long lastIncludedIndex,
        Long lastIncludedTerm,
        RaftProto.Configuration configuration) {
        RaftProto.SnapshotMetaData snapshotMetaData = RaftProto.SnapshotMetaData.newBuilder()
            .setLastIncludedIndex(lastIncludedIndex)
            .setLastIncludedTerm(lastIncludedTerm)
            .setConfiguration(configuration).build();
        String snapshotMetaFile = dir + File.separator + "metadata";
        RandomAccessFile randomAccessFile = null;
        try {
            File dirFile = new File(dir);
            if (!dirFile.exists()) {
                dirFile.mkdirs();
            }

            File file = new File(snapshotMetaFile);
            if (file.exists()) {
                FileUtils.forceDelete(file);
            }
            file.createNewFile();
            randomAccessFile = new RandomAccessFile(file, "rw");
            RaftFileUtils.writeProtoToFile(randomAccessFile, snapshotMetaData);
        } catch (IOException ex) {
            LOG.warn("meta file not exist, name={}", snapshotMetaFile);
        } finally {
            RaftFileUtils.closeFile(randomAccessFile);
        }
    }

    // Getter methods

    /**
     * @return The current snapshot metadata
     */
    public RaftProto.SnapshotMetaData getMetaData() {
        return metaData;
    }

    /**
     * @return The snapshot directory path
     */
    public String getSnapshotDir() {
        return snapshotDir;
    }

    /**
     * @return The AtomicBoolean indicating whether a snapshot is being installed
     */
    public AtomicBoolean getIsInstallSnapshot() {
        return isInstallSnapshot;
    }

    /**
     * @return The AtomicBoolean indicating whether a snapshot is being taken
     */
    public AtomicBoolean getIsTakeSnapshot() {
        return isTakeSnapshot;
    }

    /**
     * @return The lock used for synchronizing snapshot operations
     */
    public Lock getLock() {
        return lock;
    }
}