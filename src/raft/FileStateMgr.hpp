// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <libnuraft/nuraft.hxx>
#include "in_memory_log_store.hxx"
#include <filesystem>
#include <fstream>
#include <string>

namespace hamsaz::raft {

// File-backed state manager: persists state/config to disk; log store remains in-memory.
// On restart, the node retains its server ID, term, vote, and config; the leader will
// catch it up by log replication.
class FileStateMgr : public nuraft::state_mgr {
public:
  FileStateMgr(int srv_id, const std::string& endpoint, const std::string& base_dir)
      : my_id_(srv_id),
        my_endpoint_(endpoint),
        base_dir_(base_dir),
        cur_log_store_(nuraft::cs_new<nuraft::inmem_log_store>()) {
    std::filesystem::create_directories(base_dir_);
    my_srv_config_ = nuraft::cs_new<nuraft::srv_config>(srv_id, endpoint);
    saved_config_ = load_config_internal();
    if (!saved_config_) {
      saved_config_ = nuraft::cs_new<nuraft::cluster_config>();
      saved_config_->get_servers().push_back(my_srv_config_);
      save_config(*saved_config_);
    }
    saved_state_ = load_state_internal();
  }

  nuraft::ptr<nuraft::cluster_config> load_config() override { return saved_config_; }

  void save_config(const nuraft::cluster_config& config) override {
    auto buf = config.serialize();
    write_buf(config_path(), *buf);
    saved_config_ = nuraft::cluster_config::deserialize(*buf);
  }

  void save_state(const nuraft::srv_state& state) override {
    auto buf = state.serialize();
    write_buf(state_path(), *buf);
    saved_state_ = nuraft::srv_state::deserialize(*buf);
  }

  nuraft::ptr<nuraft::srv_state> read_state() override { return saved_state_; }

  nuraft::ptr<nuraft::log_store> load_log_store() override { return cur_log_store_; }

  int32_t server_id() override { return my_id_; }

  void system_exit(const int) override {}

  nuraft::ptr<nuraft::srv_config> get_srv_config() const { return my_srv_config_; }

private:
  std::filesystem::path config_path() const { return std::filesystem::path(base_dir_) / "config.bin"; }
  std::filesystem::path state_path() const { return std::filesystem::path(base_dir_) / "state.bin"; }

  static void write_buf(const std::filesystem::path& p, nuraft::buffer& buf) {
    std::ofstream out(p, std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<char*>(buf.data_begin()), buf.size());
  }

  static nuraft::ptr<nuraft::buffer> read_buf(const std::filesystem::path& p) {
    if (!std::filesystem::exists(p)) return nullptr;
    std::ifstream in(p, std::ios::binary);
    in.seekg(0, std::ios::end);
    std::streamsize sz = in.tellg();
    in.seekg(0, std::ios::beg);
    auto buf = nuraft::buffer::alloc(static_cast<size_t>(sz));
    in.read(reinterpret_cast<char*>(buf->data_begin()), sz);
    return buf;
  }

  nuraft::ptr<nuraft::cluster_config> load_config_internal() {
    auto buf = read_buf(config_path());
    if (!buf) return nullptr;
    return nuraft::cluster_config::deserialize(*buf);
  }

  nuraft::ptr<nuraft::srv_state> load_state_internal() {
    auto buf = read_buf(state_path());
    if (!buf) return nullptr;
    return nuraft::srv_state::deserialize(*buf);
  }

  int my_id_;
  std::string my_endpoint_;
  std::string base_dir_;
  nuraft::ptr<nuraft::inmem_log_store> cur_log_store_;
  nuraft::ptr<nuraft::srv_config> my_srv_config_;
  nuraft::ptr<nuraft::cluster_config> saved_config_;
  nuraft::ptr<nuraft::srv_state> saved_state_;
};

} // namespace hamsaz::raft
