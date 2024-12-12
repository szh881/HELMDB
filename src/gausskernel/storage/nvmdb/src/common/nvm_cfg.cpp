//
// Created by pzs1997 on 5/10/24.
//

#include "common/nvm_cfg.h"
#include <sstream>
#include <experimental/filesystem>
#include "glog/logging.h"

namespace NVMDB {

std::shared_ptr<NVMDB::DirectoryConfig> g_dir_config = nullptr;

DirectoryConfig::DirectoryConfig(const std::string &dirPathsString, bool init) {
    const auto endWithFunc = [](const std::string& str, const std::string& suffix) {
        if (str.size() < suffix.size()) return false;
        return str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0;
    };
    // init dirPaths
    if (endWithFunc(dirPathsString, "pg_nvm")) {
        m_dirPaths.emplace_back("/mnt/pmem0/test_folder");
        m_dirPaths.emplace_back("/mnt/pmem1/test_folder");
    } else {
        std::stringstream ss(dirPathsString);
        for (std::string dirPath; std::getline(ss, dirPath, ';'); m_dirPaths.push_back(dirPath));
    }
    // for (const auto& it: m_dirPaths) {
    //     LOG(INFO) << "NVM dir paths: " << it;
    // }
    CHECK(!m_dirPaths.empty() && m_dirPaths.size() <= NVMDB_MAX_GROUP);
    // 初始化时, 删除目录内所有文件
    if (init) {
        for (const auto &it : m_dirPaths) {
            std::experimental::filesystem::remove_all(it);
        }
    }
    // 创建文件夹
    for (const auto &it : m_dirPaths) {
        auto ret = std::experimental::filesystem::create_directories(it);
        if (!ret && init) {
            LOG(WARNING) << "Create " << it << " failed, directory may already exists!";
        }
    }
}

}