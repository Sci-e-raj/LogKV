#pragma once
#include <string>
#include <fstream>
#include <vector>
#include <mutex>
#include "store.h"

struct LogEntry {
    int index;
    int term;
    std::string key;
    std::string value;
    std::string operation; // "PUT", "DELETE", etc.
    
    LogEntry() : index(-1), term(-1) {}
    LogEntry(int idx, int t, const std::string& k, const std::string& v, const std::string& op = "PUT")
        : index(idx), term(t), key(k), value(v), operation(op) {}
};

class WriteAheadLog {
public:
    explicit WriteAheadLog(const std::string& filename);
    
    // Append a log entry
    void appendEntry(const LogEntry& entry);
    
    // Get entry at index (1-indexed)
    bool getEntry(int index, LogEntry& entry) const;
    
    // Get the last log entry
    bool getLastEntry(LogEntry& entry) const;
    
    // Get last log index and term
    void getLastLogInfo(int& last_index, int& last_term) const;
    
    // Truncate log from index onwards (for conflict resolution)
    void truncateFrom(int index);
    
    // Replay all entries to rebuild state
    void replay(KVStore& store);
    
    // Get all entries from start_index onwards
    std::vector<LogEntry> getEntriesFrom(int start_index) const;
    
    // Persist metadata (current_term, voted_for)
    void saveMetadata(int current_term, int voted_for);
    
    // Load metadata
    void loadMetadata(int& current_term, int& voted_for);
    
    int size() const;
    
    // ============================================================================
    // SNAPSHOT INTEGRATION METHODS
    // ============================================================================
    
    /**
     * Discard log entries up to and including snapshot_index
     * 
     * Called after creating a snapshot to free up disk space.
     * This is LOG COMPACTION - the core benefit of snapshots.
     * 
     * @param snapshot_index: Discard all entries <= this index
     * 
     * SAFETY: Only call this after snapshot is successfully created!
     * If snapshot fails, we must keep the log entries.
     * 
     * EXAMPLE:
     * Log: [1,2,3,4,5,6,7]
     * Snapshot at index 5
     * After compaction: [6,7]
     * Saved: 5 log entries worth of disk space
     */
    void discardEntriesBefore(int snapshot_index);
    
    /**
     * Get the index of the first entry in the log
     * 
     * After log compaction, the first entry might not be index 1.
     * Returns 0 if log is empty.
     * 
     * WHY NEEDED:
     * - Leader needs to know if it can replicate to a follower
     * - If follower needs entry at index 100 but leader's first entry is 200,
     *   leader must send a snapshot instead
     */
    int getFirstLogIndex() const;
    
    /**
     * Install a snapshot (called by follower receiving InstallSnapshot RPC)
     * 
     * This completely replaces the log with snapshot metadata.
     * All existing log entries are discarded.
     * 
     * @param last_included_index: Last log index in the snapshot
     * @param last_included_term: Term of that entry
     * 
     * WHEN TO USE:
     * - Follower receives InstallSnapshot from leader
     * - Follower is so far behind that leader has already discarded
     *   the log entries it needs
     * 
     * EFFECT:
     * - Clears all log entries
     * - Sets first_log_index = last_included_index + 1
     * - Next append will start from that index
     */
    void installSnapshot(int last_included_index, int last_included_term);

private:
    std::string filename_;
    std::string metadata_filename_;
    mutable std::vector<LogEntry> log_cache_;  // In-memory cache
    mutable std::mutex mutex_;
    
    // Track the first log index (for log compaction)
    // After compaction, first index might be > 1
    mutable int first_log_index_ = 1;
    
    void rebuildCache();
    void persistCache();
};