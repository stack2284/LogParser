#include <pybind11/pybind11.h>
#include <pybind11/stl.h> // Allows automatic conversion between C++ vectors/maps and Python lists/dicts
#include <string>
#include <vector>
#include <string_view>
#include <unordered_map>
#include <re2/re2.h>

namespace py = pybind11;

class FastParser {
private:
    re2::RE2 REGEX_IP;
    re2::RE2 REGEX_ID;
    re2::RE2 REGEX_NUM;
    re2::RE2 REGEX_PATH;
    
    std::unordered_map<uint64_t, std::string> template_cache;
    int template_counter;

    uint64_t generate_hash(const std::vector<std::string_view>& tokens) {
        uint64_t hash = 14695981039346656037ull;
        for (auto token : tokens) {
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

public:
    FastParser() : 
        REGEX_IP(R"(\d+\.\d+\.\d+\.\d+)"),
        REGEX_ID(R"([a-zA-Z]+_-?\d+)"),
        REGEX_NUM(R"(\b\d+\b)"),
        REGEX_PATH(R"(\/[^\s]+)"),
        template_counter(0) {}

    // This is the main function we will call from Python
    py::dict parse_line(std::string log_message) {
        // Fix Windows line endings
        if (!log_message.empty() && log_message.back() == '\r') log_message.pop_back();

        // 1. Preprocess
        re2::RE2::GlobalReplace(&log_message, REGEX_IP, "<IP>");
        re2::RE2::GlobalReplace(&log_message, REGEX_ID, "<ID>");
        re2::RE2::GlobalReplace(&log_message, REGEX_PATH, "<PATH>");
        re2::RE2::GlobalReplace(&log_message, REGEX_NUM, "<NUM>");

        // 2. Tokenize
        std::vector<std::string_view> tokens;
        tokens.reserve(32); 
        const char* delimiters = " :,=";
        size_t start = log_message.find_first_not_of(delimiters);
        while (start != std::string_view::npos) {
            size_t end = log_message.find_first_of(delimiters, start);
            if (end == std::string_view::npos) {
                tokens.push_back(log_message.substr(start));
                break;
            }
            tokens.push_back(log_message.substr(start, end - start));
            start = log_message.find_first_not_of(delimiters, end);
        }

        // 3. Hash & Identify
        uint64_t signature = generate_hash(tokens);
        auto it = template_cache.find(signature);
        std::string template_id;
        bool is_new = false;
        
        if (it == template_cache.end()) {
            template_counter++;
            char id_buf[16];
            snprintf(id_buf, sizeof(id_buf), "T%04d", template_counter);
            template_id = id_buf;
            template_cache[signature] = template_id;
            is_new = true;
        } else {
            template_id = it->second;
        }

        // Return a Python dictionary containing the results!
        py::dict result;
        result["template_id"] = template_id;
        result["clean_log"] = log_message;
        result["is_new"] = is_new;
        return result;
    }
};

// This macro creates the actual Python module called 'fast_log_parser'
PYBIND11_MODULE(fast_log_parser, m) {
    m.doc() = "High-performance C++ log parsing engine";
    
    py::class_<FastParser>(m, "FastParser")
        .def(py::init<>())
        .def("parse_line", &FastParser::parse_line);
}