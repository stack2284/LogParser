#include <iostream>
#include <fstream>
#include <string>
#include <chrono>
#include <vector>
#include <string_view>
#include <unordered_map>
#include <iomanip>
#include <re2/re2.h>

// Step 2: Precompiled Regex Patterns
re2::RE2 REGEX_IP(R"(\d+\.\d+\.\d+\.\d+)");
re2::RE2 REGEX_ID(R"([a-zA-Z]+_-?\d+)");
re2::RE2 REGEX_NUM(R"(\b\d+\b)");
re2::RE2 REGEX_PATH(R"(\/[^\s]+)");

void preprocess_log(std::string& log_message) {
    re2::RE2::GlobalReplace(&log_message, REGEX_IP, "<IP>");
    re2::RE2::GlobalReplace(&log_message, REGEX_ID, "<ID>");
    re2::RE2::GlobalReplace(&log_message, REGEX_PATH, "<PATH>");
    re2::RE2::GlobalReplace(&log_message, REGEX_NUM, "<NUM>");
}

// Step 3: Zero-Copy Tokenization
std::vector<std::string_view> tokenize_log(std::string_view log) {
    std::vector<std::string_view> tokens;
    tokens.reserve(32); 
    const char* delimiters = " :,=";
    
    size_t start = log.find_first_not_of(delimiters);
    while (start != std::string_view::npos) {
        size_t end = log.find_first_of(delimiters, start);
        if (end == std::string_view::npos) {
            tokens.push_back(log.substr(start));
            break;
        }
        tokens.push_back(log.substr(start, end - start));
        start = log.find_first_not_of(delimiters, end);
    }
    return tokens;
}

// Step 4: High-Speed FNV-1a Hashing Algorithm
uint64_t generate_hash(const std::vector<std::string_view>& tokens) {
    uint64_t hash = 14695981039346656037ull; // FNV offset basis
    for (auto token : tokens) {
        // Skip variables (anything wrapped in < >) to only hash constants 
        if (!token.empty() && token.front() == '<' && token.back() == '>') {
            continue;
        }
        for (char c : token) {
            hash ^= static_cast<uint64_t>(c);
            hash *= 1099511628211ull; // FNV prime
        }
        // Add a space equivalent to prevent collisions
        hash ^= ' ';
        hash *= 1099511628211ull;
    }
    return hash;
}

int main() {
    std::string filename = "HDFS_2k.log";
    std::ifstream file(filename);

    if (!file.is_open()) {
        std::cerr << "Error: Could not open " << filename << "\n";
        return 1;
    }

    // Our ultra-fast in-memory template database
    std::unordered_map<uint64_t, std::string> template_cache;
    int template_counter = 0;

    std::string line;
    int line_count = 0;

    std::cout << "Starting full high-performance parsing pipeline...\n";
    auto start_time = std::chrono::high_resolution_clock::now();

    while (std::getline(file, line)) {
        // Fix the Windows '\r' formatting bug
        if (!line.empty() && line.back() == '\r') line.pop_back();

        if (!line.empty()) {
            preprocess_log(line);
            std::vector<std::string_view> tokens = tokenize_log(line);
            
            // Generate the $O(1)$ lookup hash [cite: 98, 152]
            uint64_t signature = generate_hash(tokens);
            
            // Check if we have seen this template before
            auto it = template_cache.find(signature);
            std::string template_id;
            
            if (it == template_cache.end()) {
                // New template discovered!
                template_counter++;
                // Format ID as T0001, T0002...
                char id_buf[16];
                snprintf(id_buf, sizeof(id_buf), "T%04d", template_counter);
                template_id = id_buf;
                template_cache[signature] = template_id;
            } else {
                // Matched existing template!
                template_id = it->second;
            }

            line_count++;
            
            // Print the first 5 logs to verify
            if (line_count <= 5) {
                std::cout << "[" << template_id << "] " << line << "\n";
            }
        }
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

    std::cout << "------------------------------------------\n";
    std::cout << "Processed " << line_count << " lines.\n";
    std::cout << "Unique Templates Discovered: " << template_cache.size() << "\n";
    std::cout << "Time taken: " << duration.count() << " microseconds.\n";
    
    double seconds = duration.count() / 1000000.0;
    if (seconds > 0) {
        std::cout << "Estimated Throughput: " << (line_count / seconds) << " logs/sec\n";
    }

    file.close();
    return 0;
}