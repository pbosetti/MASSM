#pragma once

#include <cerrno>
#include <cstdlib>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace Mads {

/**
 * @brief Stream CSV rows into JSON objects.
 *
 * The loader reads the header row once to obtain column names, then converts
 * each subsequent line into a JSON object without preloading the whole file
 * into memory. Fields whose value is exactly `NA` are omitted.
 */
class CsvLoader {
public:
  /**
   * @brief Open a CSV file and read its header row.
   *
   * @param file_path Path to the CSV file.
   *
   * @throw std::runtime_error If the file cannot be opened or is empty.
   */
  explicit CsvLoader(std::string file_path)
      : _file_path(std::move(file_path)) {
    open_and_read_header();
  }

  /**
   * @brief Close the underlying file stream.
   */
  ~CsvLoader() { close(); }

  CsvLoader(const CsvLoader &) = delete;
  CsvLoader &operator=(const CsvLoader &) = delete;
  CsvLoader(CsvLoader &&) = delete;
  CsvLoader &operator=(CsvLoader &&) = delete;

  /**
   * @brief Read the next CSV row into a JSON document.
   *
   * Column names are taken from the first line of the file. Values equal to
   * `NA` are omitted from @p doc. Integer and floating-point values are stored
   * as JSON numbers; all other values are stored as strings.
   *
   * @param doc Output JSON object filled with the next row.
   * @return true If a row was read successfully.
   * @return false If the end of file was reached.
   *
   * @throw std::runtime_error If the file is not open.
   */
  bool load_line(json &doc) {
    ensure_open();
    doc = json::object();

    std::string raw_line;
    while (std::getline(_stream, raw_line)) {
      if (!raw_line.empty() && raw_line.back() == '\r') {
        raw_line.pop_back();
      }

      if (raw_line.empty()) {
        continue;
      }

      const auto values = parse_csv_line(raw_line);
      for (std::size_t i = 0; i < _columns.size(); ++i) {
        const std::string value = i < values.size() ? values[i] : std::string{};
        const std::string trimmed = trim(value);
        if (trimmed == "NA") {
          continue;
        }

        doc[_columns[i]] = to_json_value(trimmed);
      }

      return true;
    }

    return false;
  }

  /**
   * @brief Rewind the file and reread the header row.
   *
   * After reset, the next call to load_line() returns the first data row again.
   *
   * @throw std::runtime_error If the file cannot be reopened or is empty.
   */
  void reset() {
    close();
    open_and_read_header();
  }

private:
  static std::vector<std::string> parse_csv_line(const std::string &line) {
    std::vector<std::string> fields;
    std::string current;
    bool in_quotes = false;

    for (std::size_t i = 0; i < line.size(); ++i) {
      const char ch = line[i];
      if (ch == '"') {
        if (in_quotes && i + 1 < line.size() && line[i + 1] == '"') {
          current.push_back('"');
          ++i;
        } else {
          in_quotes = !in_quotes;
        }
      } else if (ch == ',' && !in_quotes) {
        fields.push_back(current);
        current.clear();
      } else {
        current.push_back(ch);
      }
    }

    fields.push_back(current);
    return fields;
  }

  static std::string trim(std::string_view value) {
    std::size_t begin = 0;
    std::size_t end = value.size();

    while (begin < end && is_space(value[begin])) {
      ++begin;
    }
    while (end > begin && is_space(value[end - 1])) {
      --end;
    }

    return std::string(value.substr(begin, end - begin));
  }

  static bool is_space(char ch) {
    return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\f' ||
           ch == '\v';
  }

  static json to_json_value(const std::string &value) {
    long long integer_value = 0;
    if (try_parse_integer(value, integer_value)) {
      return integer_value;
    }

    double double_value = 0.0;
    if (try_parse_double(value, double_value)) {
      return double_value;
    }

    return value;
  }

  static bool try_parse_integer(const std::string &value, long long &result) {
    if (value.empty()) {
      return false;
    }

    char *end = nullptr;
    errno = 0;
    const long long parsed = std::strtoll(value.c_str(), &end, 10);
    if (errno != 0 || end != value.c_str() + value.size()) {
      return false;
    }

    result = parsed;
    return true;
  }

  static bool try_parse_double(const std::string &value, double &result) {
    if (value.empty()) {
      return false;
    }

    char *end = nullptr;
    errno = 0;
    const double parsed = std::strtod(value.c_str(), &end);
    if (errno != 0 || end != value.c_str() + value.size()) {
      return false;
    }

    result = parsed;
    return true;
  }

  void ensure_open() const {
    if (!_stream.is_open()) {
      throw std::runtime_error("CSV file is not open: " + _file_path);
    }
  }

  void open_and_read_header() {
    _stream.open(_file_path);
    if (!_stream.is_open()) {
      throw std::runtime_error("Unable to open CSV file: " + _file_path);
    }

    std::string header_line;
    if (!std::getline(_stream, header_line)) {
      throw std::runtime_error("CSV file is empty: " + _file_path);
    }

    if (!header_line.empty() && header_line.back() == '\r') {
      header_line.pop_back();
    }

    _columns = parse_csv_line(header_line);
    for (auto &column : _columns) {
      column = trim(column);
    }
  }

  void close() {
    if (_stream.is_open()) {
      _stream.close();
    }
  }

  std::string _file_path;
  std::ifstream _stream;
  std::vector<std::string> _columns;
};

} // namespace Mads
