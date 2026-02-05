#include "snapshot.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <sys/stat.h>
#include <dirent.h>
#include <cstring>
#include <unistd.h>

SnapshotManager::SnapshotManager(const std::string& snapshot_dir, int server_id)
    : snapshot_dir_(snapshot_dir), server_id_(server_id) {
    
    // Create snapshot directory if it doesn't exist
    struct stat st;
    if (stat(snapshot_dir_.c_str(), &st) != 0) {
        mkdir(snapshot_dir_.c_str(), 0755);
        std::cout << "[INFO] Created snapshot directory: " << snapshot_dir_ << std::endl;
    }
    
    temp_snapshot_path_ = snapshot_dir_ + "/temp_" + std::to_string(server_id_) + ".snap";
}

std::string SnapshotManager::generateSnapshotFilename(int last_index) const {
    std::ostringstream oss;
    oss << snapshot_dir_ << "/snapshot_" << server_id_ 
        << "_idx_" << last_index << ".snap";
    return oss.str();
}

bool SnapshotManager::createSnapshot(
    const std::unordered_map<std::string, std::string>& data,
    int last_index,
    int last_term) {
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::cout << "[INFO] Creating snapshot at index " << last_index 
              << ", term " << last_term << " (" << data.size() << " entries)" << std::endl;
    
    // Step 1: Write to temporary file
    // WHY TEMP FILE? If we crash during write, we don't corrupt the last good snapshot
    std::ofstream temp_out(temp_snapshot_path_, std::ios::binary);
    if (!temp_out) {
        std::cerr << "[ERROR] Failed to create temp snapshot file: " 
                  << temp_snapshot_path_ << std::endl;
        return false;
    }
    
    // Step 2: Write metadata header
    // Format: SNAPSHOT_MAGIC last_index last_term data_size\n
    // Magic number helps detect file corruption
    temp_out << "LOGKV_SNAPSHOT_V1\n";
    temp_out << last_index << " " << last_term << " " << data.size() << "\n";
    
    // Step 3: Write all key-value pairs
    // Format: key_length value_length key value
    // This format handles keys/values with spaces or newlines
    for (const auto& [key, value] : data) {
        temp_out << key.size() << " " << value.size() << "\n";
        temp_out << key << "\n";
        temp_out << value << "\n";
    }
    
    // Step 4: Flush to ensure data is on disk
    temp_out.flush();
    temp_out.close();
    
    if (!temp_out) {
        std::cerr << "[ERROR] Failed to write snapshot data" << std::endl;
        return false;
    }
    
    // Step 5: Atomic rename
    // This is the critical step! Rename is atomic on POSIX systems.
    // Even if we crash here, we either have the old snapshot or the new one,
    // never a partially written file.
    std::string final_path = generateSnapshotFilename(last_index);
    if (rename(temp_snapshot_path_.c_str(), final_path.c_str()) != 0) {
        std::cerr << "[ERROR] Failed to rename snapshot: " << strerror(errno) << std::endl;
        return false;
    }
    
    std::cout << "[SUCCESS] Snapshot created: " << final_path << std::endl;
    
    // Step 6: Clean up old snapshots
    cleanupOldSnapshots(2);  // Keep last 2 snapshots for safety
    
    return true;
}

bool SnapshotManager::loadSnapshot(
    std::unordered_map<std::string, std::string>& data,
    SnapshotMetadata& metadata) {
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::string snapshot_path = findLatestSnapshot();
    if (snapshot_path.empty()) {
        std::cout << "[INFO] No snapshot found" << std::endl;
        return false;
    }
    
    std::cout << "[INFO] Loading snapshot: " << snapshot_path << std::endl;
    
    std::ifstream in(snapshot_path, std::ios::binary);
    if (!in) {
        std::cerr << "[ERROR] Failed to open snapshot: " << snapshot_path << std::endl;
        return false;
    }
    
    // Read and verify magic header
    std::string magic;
    std::getline(in, magic);
    if (magic != "LOGKV_SNAPSHOT_V1") {
        std::cerr << "[ERROR] Invalid snapshot format: " << magic << std::endl;
        return false;
    }
    
    // Read metadata
    in >> metadata.last_included_index 
       >> metadata.last_included_term 
       >> metadata.data_size;
    in.ignore();  // Skip newline
    
    std::cout << "[INFO] Snapshot metadata: index=" << metadata.last_included_index
              << ", term=" << metadata.last_included_term
              << ", entries=" << metadata.data_size << std::endl;
    
    // Read all key-value pairs
    data.clear();
    for (size_t i = 0; i < metadata.data_size; i++) {
        size_t key_len, value_len;
        in >> key_len >> value_len;
        in.ignore();  // Skip newline
        
        std::string key(key_len, '\0');
        in.read(&key[0], key_len);
        in.ignore();  // Skip newline
        
        std::string value(value_len, '\0');
        in.read(&value[0], value_len);
        in.ignore();  // Skip newline
        
        data[key] = value;
    }
    
    std::cout << "[SUCCESS] Loaded " << data.size() << " entries from snapshot" << std::endl;
    
    return true;
}

bool SnapshotManager::getSnapshotMetadata(SnapshotMetadata& metadata) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::string snapshot_path = findLatestSnapshot();
    if (snapshot_path.empty()) {
        return false;
    }
    
    std::ifstream in(snapshot_path, std::ios::binary);
    if (!in) {
        return false;
    }
    
    // Skip magic
    std::string magic;
    std::getline(in, magic);
    
    // Read metadata
    in >> metadata.last_included_index 
       >> metadata.last_included_term 
       >> metadata.data_size;
    
    return true;
}

bool SnapshotManager::hasSnapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return !findLatestSnapshot().empty();
}

std::string SnapshotManager::getSnapshotPath() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return findLatestSnapshot();
}

std::string SnapshotManager::findLatestSnapshot() const {
    // Don't lock here - caller should lock
    
    DIR* dir = opendir(snapshot_dir_.c_str());
    if (!dir) {
        return "";
    }
    
    int max_index = -1;
    std::string latest_snapshot;
    
    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        std::string filename = entry->d_name;
        
        // Check if this is a snapshot file for our server
        std::string prefix = "snapshot_" + std::to_string(server_id_) + "_idx_";
        if (filename.find(prefix) == 0) {
            int index = parseSnapshotIndex(filename);
            if (index > max_index) {
                max_index = index;
                latest_snapshot = snapshot_dir_ + "/" + filename;
            }
        }
    }
    
    closedir(dir);
    return latest_snapshot;
}

int SnapshotManager::parseSnapshotIndex(const std::string& filename) const {
    // Extract index from filename: snapshot_<server_id>_idx_<index>.snap
    size_t idx_pos = filename.find("_idx_");
    if (idx_pos == std::string::npos) {
        return -1;
    }
    
    size_t start = idx_pos + 5;  // Length of "_idx_"
    size_t end = filename.find(".snap", start);
    if (end == std::string::npos) {
        return -1;
    }
    
    std::string index_str = filename.substr(start, end - start);
    try {
        return std::stoi(index_str);
    } catch (...) {
        return -1;
    }
}

size_t SnapshotManager::readSnapshotChunk(
    size_t offset, 
    size_t chunk_size, 
    std::vector<char>& data) {
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::string snapshot_path = findLatestSnapshot();
    if (snapshot_path.empty()) {
        return 0;
    }
    
    std::ifstream in(snapshot_path, std::ios::binary);
    if (!in) {
        return 0;
    }
    
    // Seek to offset
    in.seekg(offset);
    if (!in) {
        return 0;
    }
    
    // Read chunk
    data.resize(chunk_size);
    in.read(data.data(), chunk_size);
    
    size_t bytes_read = in.gcount();
    data.resize(bytes_read);
    
    return bytes_read;
}

bool SnapshotManager::writeSnapshotChunk(
    size_t offset,
    const std::vector<char>& data,
    bool is_last) {
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Open in append mode for first chunk, update mode for subsequent chunks
    std::ios::openmode mode = std::ios::binary;
    if (offset == 0) {
        mode |= std::ios::trunc;  // Truncate if starting fresh
    } else {
        mode |= std::ios::in | std::ios::out;  // Read-write for seeking
    }
    
    std::fstream out(temp_snapshot_path_, mode);
    if (!out) {
        std::cerr << "[ERROR] Failed to open temp snapshot for chunk write" << std::endl;
        return false;
    }
    
    // Seek to offset
    out.seekp(offset);
    if (!out) {
        std::cerr << "[ERROR] Failed to seek to offset " << offset << std::endl;
        return false;
    }
    
    // Write chunk
    out.write(data.data(), data.size());
    out.flush();
    
    if (!out) {
        std::cerr << "[ERROR] Failed to write snapshot chunk" << std::endl;
        return false;
    }
    
    // If this is the last chunk, read metadata and finalize
    if (is_last) {
        out.close();
        
        // Read metadata to determine final filename
        std::ifstream in(temp_snapshot_path_, std::ios::binary);
        std::string magic;
        std::getline(in, magic);
        
        int last_index, last_term;
        size_t data_size;
        in >> last_index >> last_term >> data_size;
        in.close();
        
        // Rename to final location
        std::string final_path = generateSnapshotFilename(last_index);
        if (rename(temp_snapshot_path_.c_str(), final_path.c_str()) != 0) {
            std::cerr << "[ERROR] Failed to finalize snapshot" << std::endl;
            return false;
        }
        
        std::cout << "[SUCCESS] Received and installed snapshot at index " 
                  << last_index << std::endl;
    }
    
    return true;
}

void SnapshotManager::cleanupOldSnapshots(int keep_count) {
    // Don't lock here - caller should lock
    
    DIR* dir = opendir(snapshot_dir_.c_str());
    if (!dir) {
        return;
    }
    
    // Find all snapshots for this server
    std::vector<std::pair<int, std::string>> snapshots;  // (index, path)
    
    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        std::string filename = entry->d_name;
        
        std::string prefix = "snapshot_" + std::to_string(server_id_) + "_idx_";
        if (filename.find(prefix) == 0) {
            int index = parseSnapshotIndex(filename);
            if (index >= 0) {
                snapshots.push_back({index, snapshot_dir_ + "/" + filename});
            }
        }
    }
    
    closedir(dir);
    
    // Sort by index (descending)
    std::sort(snapshots.begin(), snapshots.end(), 
              [](const auto& a, const auto& b) { return a.first > b.first; });
    
    // Delete all except the most recent keep_count
    for (size_t i = keep_count; i < snapshots.size(); i++) {
        std::cout << "[INFO] Deleting old snapshot: " << snapshots[i].second << std::endl;
        unlink(snapshots[i].second.c_str());
    }
}
