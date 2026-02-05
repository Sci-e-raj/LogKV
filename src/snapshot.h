#pragma once
#include <string>
#include <fstream>
#include <vector>
#include <mutex>
#include <unordered_map>

/**
 * Snapshot Metadata
 * 
 * Contains information about a snapshot's coverage:
 * - last_included_index: The highest log index included in this snapshot
 * - last_included_term: The term of the last_included_index entry
 * - data_size: Number of key-value pairs in the snapshot
 * 
 * Purpose: Allows us to know what portion of the log this snapshot covers,
 * so we can safely discard older log entries.
 */
struct SnapshotMetadata {
    int last_included_index;  // Highest log index covered by this snapshot
    int last_included_term;   // Term of that log entry
    size_t data_size;         // Number of KV pairs in snapshot
    
    SnapshotMetadata() 
        : last_included_index(0), last_included_term(0), data_size(0) {}
    
    SnapshotMetadata(int idx, int term, size_t size)
        : last_included_index(idx), last_included_term(term), data_size(size) {}
};

/**
 * SnapshotManager
 * 
 * Manages snapshot creation, loading, and log compaction for a Raft node.
 * 
 * WHY SNAPSHOTS ARE CRITICAL:
 * -------------------------
 * 1. LOG UNBOUNDED GROWTH: Without snapshots, the WAL grows forever. After millions
 *    of operations, nodes would need gigabytes of disk and minutes to replay on restart.
 * 
 * 2. NEW NODES SLOW: When adding a new node to the cluster, sending it the entire
 *    log history is extremely slow. Snapshots let us send just the current state.
 * 
 * 3. RECOVERY TIME: Crash recovery requires replaying the entire log. With snapshots,
 *    we only replay entries after the last snapshot (much faster).
 * 
 * HOW IT WORKS:
 * -------------
 * 1. Periodically, we save the entire KV store to disk (snapshot)
 * 2. We record which log index this snapshot covers
 * 3. We can safely delete all log entries up to that index (log compaction)
 * 4. On recovery: Load snapshot + replay remaining log entries
 * 
 * RAFT SNAPSHOT REQUIREMENTS:
 * ---------------------------
 * - Snapshots must be taken at committed log entries only (never uncommitted)
 * - Must preserve last_included_index and last_included_term for log matching
 * - Leader can send snapshots to followers who are too far behind
 * - Followers must handle receiving snapshots (InstallSnapshot RPC)
 */
class SnapshotManager {
public:
    explicit SnapshotManager(const std::string& snapshot_dir, int server_id);
    
    /**
     * Create a snapshot of the current state
     * 
     * @param data: The complete KV store state
     * @param last_index: The highest log index applied to this state
     * @param last_term: The term of that log entry
     * @return: True if snapshot created successfully
     * 
     * IMPLEMENTATION NOTES:
     * - Atomic write: Write to temp file, then rename (crash-safe)
     * - Format: Simple text format for readability (production might use protobuf)
     * - Thread-safe: Can be called while server is running
     */
    bool createSnapshot(const std::unordered_map<std::string, std::string>& data,
                       int last_index,
                       int last_term);
    
    /**
     * Load the most recent snapshot
     * 
     * @param data: Output - will be filled with snapshot data
     * @param metadata: Output - will be filled with snapshot metadata
     * @return: True if snapshot loaded successfully
     * 
     * WHEN TO USE:
     * - On server startup (before replaying WAL)
     * - When receiving InstallSnapshot from leader
     */
    bool loadSnapshot(std::unordered_map<std::string, std::string>& data,
                     SnapshotMetadata& metadata);
    
    /**
     * Get metadata of the most recent snapshot
     * 
     * Useful for determining if a snapshot exists and what it covers
     * without loading the entire snapshot data.
     */
    bool getSnapshotMetadata(SnapshotMetadata& metadata);
    
    /**
     * Check if a snapshot exists
     */
    bool hasSnapshot() const;
    
    /**
     * Get the path to the current snapshot file
     */
    std::string getSnapshotPath() const;
    
    /**
     * Serialize snapshot data to send to a follower (for InstallSnapshot RPC)
     * 
     * @param offset: Byte offset to start from (for chunked transfer)
     * @param chunk_size: Maximum bytes to read
     * @param data: Output buffer
     * @return: Actual bytes read
     * 
     * CHUNKED TRANSFER:
     * - Snapshots can be large (GBs), so we send them in chunks
     * - Leader tracks offset for each follower
     * - Follower reassembles chunks into complete snapshot
     */
    size_t readSnapshotChunk(size_t offset, size_t chunk_size, std::vector<char>& data);
    
    /**
     * Write a chunk of snapshot data (follower receiving InstallSnapshot)
     * 
     * @param offset: Where to write this chunk
     * @param data: The chunk data
     * @param is_last: True if this is the final chunk
     * @return: True if write successful
     */
    bool writeSnapshotChunk(size_t offset, const std::vector<char>& data, bool is_last);
    
    /**
     * Delete old snapshots (keep only the most recent)
     * 
     * MAINTENANCE:
     * - Prevent disk from filling with old snapshots
     * - Usually keep 1-2 most recent snapshots for safety
     */
    void cleanupOldSnapshots(int keep_count = 1);

private:
    std::string snapshot_dir_;      // Directory to store snapshots
    int server_id_;                 // Server ID for naming
    mutable std::mutex mutex_;      // Thread safety
    
    std::string temp_snapshot_path_;  // Temporary file during creation
    
    // Generate snapshot filename based on index
    std::string generateSnapshotFilename(int last_index) const;
    
    // Find the most recent snapshot file
    std::string findLatestSnapshot() const;
    
    // Parse snapshot filename to extract index
    int parseSnapshotIndex(const std::string& filename) const;
};
