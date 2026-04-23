#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <string>
#include <vector>
#include <string_view>
#include <unordered_map>
#include <re2/re2.h>
#include <omp.h>
#include <mutex>
#include <cstring>
#include <bitset>
#include <array>

namespace py = pybind11;

// =====================================================
// B9: Bloom Filter for fast template cache membership test
// Avoids hash map lookup for ~95% of logs (known templates)
// =====================================================
class BloomFilter {
    static constexpr size_t BLOOM_SIZE = 65536;  // 64K bits = 8KB memory
    std::bitset<BLOOM_SIZE> bits;
    
    // Use 3 independent hash functions for low false positive rate
    size_t hash1(uint64_t key) const { return key % BLOOM_SIZE; }
    size_t hash2(uint64_t key) const { return (key * 2654435761ull) % BLOOM_SIZE; }
    size_t hash3(uint64_t key) const { return (key * 40503ull) % BLOOM_SIZE; }

public:
    void insert(uint64_t key) {
        bits.set(hash1(key));
        bits.set(hash2(key));
        bits.set(hash3(key));
    }
    
    bool possibly_contains(uint64_t key) const {
        return bits.test(hash1(key)) && 
               bits.test(hash2(key)) && 
               bits.test(hash3(key));
    }
};

class FastParser {
private:
    re2::RE2 REGEX_IP;
    re2::RE2 REGEX_ID;
    re2::RE2 REGEX_NUM;
    re2::RE2 REGEX_PATH;
    re2::RE2 REGEX_BLOCK;
    
    re2::RE2 REGEX_MAC;
    re2::RE2 REGEX_IPV6;
    re2::RE2 REGEX_HEX;
    re2::RE2 REGEX_UUID;
    re2::RE2 REGEX_JAVA;
    re2::RE2 REGEX_ZK_ENV;
    re2::RE2 REGEX_THREAD;
    re2::RE2 REGEX_NUM_MS;

    bool is_specialized;

    std::unordered_map<uint64_t, std::string> template_cache;
    int template_counter;
    std::mutex cache_mutex;
    
    BloomFilter bloom;

    uint64_t generate_hash(const std::vector<std::string_view>& tokens) {
        uint64_t hash = 14695981039346656037ull;
        for (const auto& token : tokens) {
            if (!token.empty() && token.front() == '<' && token.back() == '>') continue;
            for (char c : token) {
                hash ^= static_cast<uint64_t>(c);
                hash *= 1099511628211ull;
            }
            hash ^= ' ';
            hash *= 1099511628211ull;
        }
        return hash;
    }

    std::vector<double> extract_numbers(const std::string& raw_log) {
        std::vector<double> numbers;
        numbers.reserve(16);
        const char* p = raw_log.c_str();
        const char* end = p + raw_log.size();
        while (p < end) {
            while (p < end && (*p < '0' || *p > '9')) p++;
            if (p >= end) break;
            if (p > raw_log.c_str()) {
                char prev = *(p - 1);
                if ((prev >= 'a' && prev <= 'z') || (prev >= 'A' && prev <= 'Z') || prev == '_') {
                    while (p < end && *p >= '0' && *p <= '9') p++;
                    continue;
                }
            }
            double num = 0;
            while (p < end && *p >= '0' && *p <= '9') {
                num = num * 10 + (*p - '0');
                p++;
            }
            if (p < end) {
                char next = *p;
                if ((next >= 'a' && next <= 'z') || (next >= 'A' && next <= 'Z') || next == '_') {
                    continue;
                }
            }
            numbers.push_back(num);
        }
        return numbers;
    }

    std::string extract_block_id(const std::string& raw_log) {
        std::string block_id;
        if (is_specialized) {
            re2::RE2::PartialMatch(raw_log, REGEX_BLOCK, &block_id);
        }
        return block_id;
    }

    bool is_low_priority(const std::string& log) {
        size_t check_len = std::min(log.size(), (size_t)80);
        for (size_t i = 0; i < check_len; i++) {
            if (log[i] == 'D' && check_len - i >= 5 &&
                log[i+1] == 'E' && log[i+2] == 'B' && log[i+3] == 'U' && log[i+4] == 'G') {
                return true;
            }
            if (log[i] == 'T' && check_len - i >= 5 &&
                log[i+1] == 'R' && log[i+2] == 'A' && log[i+3] == 'C' && log[i+4] == 'E') {
                return true;
            }
        }
        return false;
    }

    struct ParseResult {
        std::string template_id;
        std::string clean_log;
        std::string block_id;
        std::vector<double> params;
        bool is_new;
        bool skip_anomaly;
    };

    ParseResult parse_line_core(std::string log_message, const std::string& raw_log) {
        ParseResult result;
        result.skip_anomaly = false;
        result.is_new = false;
        
        if (!log_message.empty() && log_message.back() == '\r') log_message.pop_back();

        if (is_low_priority(log_message)) {
            result.skip_anomaly = true;
        }

        result.params = extract_numbers(raw_log);
        result.block_id = extract_block_id(raw_log);

        if (!is_specialized) {
            re2::RE2::GlobalReplace(&log_message, REGEX_IPV6, "<IPV6>");
            re2::RE2::GlobalReplace(&log_message, REGEX_MAC, "<MAC>");
            re2::RE2::GlobalReplace(&log_message, REGEX_UUID, "<UUID>");
            re2::RE2::GlobalReplace(&log_message, REGEX_ZK_ENV, "Server environment: <ENV>");
            re2::RE2::GlobalReplace(&log_message, REGEX_THREAD, "<THREAD>");
            re2::RE2::GlobalReplace(&log_message, REGEX_JAVA, "<JAVA>");
            re2::RE2::GlobalReplace(&log_message, REGEX_HEX, "<HEX>");
        }
        
        re2::RE2::GlobalReplace(&log_message, REGEX_IP, "<IP>");
        re2::RE2::GlobalReplace(&log_message, REGEX_ID, "<ID>");
        re2::RE2::GlobalReplace(&log_message, REGEX_PATH, "<PATH>");
        re2::RE2::GlobalReplace(&log_message, REGEX_NUM_MS, "<NUM>ms");
        re2::RE2::GlobalReplace(&log_message, REGEX_NUM, "<NUM>");

        // Tokenize
        std::vector<std::string_view> tokens;
        tokens.reserve(32);
        const char* delimiters = " :,=";
        size_t start = log_message.find_first_not_of(delimiters);
        while (start != std::string_view::npos) {
            size_t end_pos = log_message.find_first_of(delimiters, start);
            if (end_pos == std::string_view::npos) {
                tokens.push_back(std::string_view(log_message).substr(start));
                break;
            }
            tokens.push_back(std::string_view(log_message).substr(start, end_pos - start));
            start = log_message.find_first_not_of(delimiters, end_pos);
        }

        uint64_t signature = generate_hash(tokens);

        {
            std::lock_guard<std::mutex> lock(cache_mutex);
            
            if (bloom.possibly_contains(signature)) {
                auto it = template_cache.find(signature);
                if (it != template_cache.end()) {
                    result.template_id = it->second;
                    result.is_new = false;
                } else {
                    template_counter++;
                    char id_buf[16];
                    snprintf(id_buf, sizeof(id_buf), "T%04d", template_counter);
                    result.template_id = id_buf;
                    template_cache[signature] = result.template_id;
                    bloom.insert(signature);
                    result.is_new = true;
                }
            } else {
                template_counter++;
                char id_buf[16];
                snprintf(id_buf, sizeof(id_buf), "T%04d", template_counter);
                result.template_id = id_buf;
                template_cache[signature] = result.template_id;
                bloom.insert(signature);
                result.is_new = true;
            }
        }

        result.clean_log = std::move(log_message);
        return result;
    }

public:
    FastParser(bool specialized = true) : 
        REGEX_MAC(R"(([0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2})"),
        REGEX_IPV6(R"((?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4})"),
        REGEX_HEX(R"(0[xX][0-9a-fA-F]+)"),
        REGEX_UUID(R"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"),
        REGEX_JAVA(R"([a-zA-Z_][a-zA-Z0-9_$@-]*\.(?:[a-zA-Z0-9_$@-]+\.)+[a-zA-Z0-9_$@-]+)"),
        REGEX_ZK_ENV(R"(Server environment:.*)"),
        REGEX_THREAD(R"(\[(?:[^\[\]]|\[[^\]]*\])+\])"),
        is_specialized(specialized),
        REGEX_IP(R"(\d+\.\d+\.\d+\.\d+(?::\d+)?)"),
        REGEX_ID(R"([a-zA-Z]+_(?:[-a-zA-Z0-9]+_)*-?\d+)"),
        REGEX_NUM(R"(\b\d+\b)"),
        REGEX_NUM_MS(R"(\b\d+ms\b)"),
        REGEX_PATH(R"(\/[^\s]+)"),
        REGEX_BLOCK(R"((blk_-?\d+))"),
        template_counter(0) {}

    py::dict parse_line(std::string log_message) {
        std::string raw = log_message;
        auto result = parse_line_core(std::move(log_message), raw);
        
        py::dict py_result;
        py_result["template_id"] = result.template_id;
        py_result["clean_log"] = result.clean_log;
        py_result["is_new"] = result.is_new;
        py_result["block_id"] = result.block_id;
        py_result["params"] = result.params;
        py_result["skip_anomaly"] = result.skip_anomaly;
        return py_result;
    }

    py::list parse_batch(std::vector<std::string> lines) {
        size_t n = lines.size();
        std::vector<ParseResult> cpp_results(n);
        std::vector<std::string> raw_copies(lines);
        
        {
            py::gil_scoped_release release;
            
            #pragma omp parallel for schedule(dynamic, 64)
            for (size_t i = 0; i < n; i++) {
                cpp_results[i] = parse_line_core(std::move(lines[i]), raw_copies[i]);
            }
        }
        
        py::list py_results(n);
        for (size_t i = 0; i < n; i++) {
            auto& r = cpp_results[i];
            py::dict d;
            d["template_id"] = std::move(r.template_id);
            d["clean_log"] = std::move(r.clean_log);
            d["is_new"] = r.is_new;
            d["block_id"] = std::move(r.block_id);
            d["params"] = std::move(r.params);
            d["skip_anomaly"] = r.skip_anomaly;
            py_results[i] = std::move(d);
        }
        return py_results;
    }
};

PYBIND11_MODULE(fast_log_parser, m) {
    m.doc() = "High-performance C++ log parsing engine with OpenMP + Bloom filter";
    
    py::class_<FastParser>(m, "FastParser")
        .def(py::init<bool>(), py::arg("specialized") = true)
        .def("parse_line", &FastParser::parse_line)
        .def("parse_batch", &FastParser::parse_batch);
}