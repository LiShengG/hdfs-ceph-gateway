// src/tools/gwctl.cc
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "rpc/internal/internal_rpc_client.h"
#include "gateway_internal.pb.h"
#include "common/logging.h"

using hcg::internal_rpc::InternalRpcClient;

static void usage() {
    std::fprintf(stderr,
        "Usage:\n"
        "  gwctl [--host HOST] [--port PORT] <command> [args...]\n"
        "\n"
        "Commands:\n"
        "  create <path>\n"
        "  stat   <path>\n"
        "  ls     <path>\n"
        "  rm     <path>\n"
        "  put    <local_file> <remote_path>\n"
        "  cat    <remote_path>\n"
        "\n"
        "Default host=127.0.0.1, port=19000\n");
}

// 简单 argv 解析
int main(int argc, char** argv) {
    std::string host = "127.0.0.1";
    uint16_t port = 19000;

    int idx = 1;
    while (idx < argc && std::strncmp(argv[idx], "--", 2) == 0) {
        std::string opt = argv[idx];
        if (opt == "--host" && idx + 1 < argc) {
            host = argv[++idx];
        } else if (opt == "--port" && idx + 1 < argc) {
            port = static_cast<uint16_t>(std::stoi(argv[++idx]));
        } else {
            usage();
            return 1;
        }
        ++idx;
    }

    if (idx >= argc) {
        usage();
        return 1;
    }

    std::string cmd = argv[idx++];

    InternalRpcClient client(host, port);
    if (!client.connect()) {
        std::fprintf(stderr, "failed to connect to %s:%u\n", host.c_str(), port);
        return 1;
    }

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    if (cmd == "create") {
        if (idx >= argc) {
            usage();
            return 1;
        }
        std::string path = argv[idx++];

        hcg::internal::CreateFileRequest req;
        req.set_path(path);
        req.set_mode(0644);
        req.set_replication(1);
        req.set_block_size(128ULL * 1024 * 1024);

        hcg::internal::CreateFileResponse rsp;
        if (!client.CreateFile(req, rsp)) {
            std::fprintf(stderr, "CreateFile RPC failed\n");
            return 1;
        }
        if (rsp.status().code() != 0) {
            std::fprintf(stderr, "CreateFile error: code=%d msg=%s\n",
                         rsp.status().code(), rsp.status().message().c_str());
            return 1;
        }
        std::printf("Created file: file_id=%lu path=%s\n",
                    (unsigned long)rsp.handle().file_id(),
                    rsp.handle().path().c_str());
    } else if (cmd == "stat") {
        if (idx >= argc) {
            usage();
            return 1;
        }
        std::string path = argv[idx++];

        hcg::internal::GetFileInfoRequest req;
        req.set_path(path);
        hcg::internal::GetFileInfoResponse rsp;
        if (!client.GetFileInfo(req, rsp)) {
            std::fprintf(stderr, "GetFileInfo RPC failed\n");
            return 1;
        }
        if (rsp.status().code() != 0) {
            std::fprintf(stderr, "GetFileInfo error: code=%d msg=%s\n",
                         rsp.status().code(), rsp.status().message().c_str());
            return 1;
        }
        const auto& st = rsp.status_info();
        std::printf("Path: %s\n", st.path().c_str());
        std::printf("Type: %s\n", st.is_dir() ? "directory" : "file");
        std::printf("Length: %lu\n", (unsigned long)st.length());
        std::printf("Block size: %lu\n", (unsigned long)st.block_size());
        std::printf("Replication: %u\n", st.replication());
        std::printf("Mode: %o\n", st.mode());
        std::printf("Owner: %s\n", st.owner().c_str());
        std::printf("Group: %s\n", st.group().c_str());
    } else if (cmd == "ls") {
        if (idx >= argc) {
            usage();
            return 1;
        }
        std::string path = argv[idx++];

        hcg::internal::ListStatusRequest req;
        req.set_path(path);
        hcg::internal::ListStatusResponse rsp;
        if (!client.ListStatus(req, rsp)) {
            std::fprintf(stderr, "ListStatus RPC failed\n");
            return 1;
        }
        if (rsp.status().code() != 0) {
            std::fprintf(stderr, "ListStatus error: code=%d msg=%s\n",
                         rsp.status().code(), rsp.status().message().c_str());
            return 1;
        }
        for (const auto& e : rsp.entries()) {
            std::printf("%c %10lu %s\n",
                        e.is_dir() ? 'd' : '-',
                        (unsigned long)e.length(),
                        e.path().c_str());
        }
    } else if (cmd == "rm") {
        if (idx >= argc) {
            usage();
            return 1;
        }
        std::string path = argv[idx++];

        hcg::internal::DeleteRequest req;
        req.set_path(path);
        req.set_recursive(false); // 可以后续支持 -r
        hcg::internal::DeleteResponse rsp;
        if (!client.DeletePath(req, rsp)) {
            std::fprintf(stderr, "Delete RPC failed\n");
            return 1;
        }
        if (rsp.status().code() != 0) {
            std::fprintf(stderr, "Delete error: code=%d msg=%s\n",
                         rsp.status().code(), rsp.status().message().c_str());
            return 1;
        }
        std::printf("Deleted %s\n", path.c_str());
    } else if (cmd == "put") {
        if (idx + 1 >= argc) {
            usage();
            return 1;
        }
        std::string local = argv[idx++];
        std::string remote = argv[idx++];

        // 打开本地文件，读入内存（Stage 0 先不考虑巨型文件）
        std::ifstream ifs(local, std::ios::binary);
        if (!ifs) {
            std::fprintf(stderr, "Failed to open local file: %s\n", local.c_str());
            return 1;
        }
        std::vector<char> buf((std::istreambuf_iterator<char>(ifs)),
                               std::istreambuf_iterator<char>());

        // 1) CreateFile
        hcg::internal::CreateFileRequest creq;
        creq.set_path(remote);
        creq.set_mode(0644);
        creq.set_replication(1);
        creq.set_block_size(128ULL * 1024 * 1024);
        hcg::internal::CreateFileResponse crsp;
        if (!client.CreateFile(creq, crsp) || crsp.status().code() != 0) {
            std::fprintf(stderr, "CreateFile for put failed: code=%d msg=%s\n",
                         crsp.status().code(), crsp.status().message().c_str());
            return 1;
        }

        // 2) AllocateBlock（先简单一个 block）
        hcg::internal::AllocateBlockRequest abreq;
        abreq.set_path(remote);
        hcg::internal::AllocateBlockResponse abrsp;
        if (!client.AllocateBlock(abreq, abrsp) || abrsp.status().code() != 0) {
            std::fprintf(stderr, "AllocateBlock failed: code=%d msg=%s\n",
                         abrsp.status().code(), abrsp.status().message().c_str());
            return 1;
        }

        auto block = abrsp.block();

        // 3) WriteBlock：这里可以按小 chunk 追加写入
        const size_t chunk_size = 4 * 1024 * 1024;
        uint64_t offset_in_block = 0;
        size_t total = buf.size();
        size_t pos = 0;

        while (pos < total) {
            size_t n = std::min(chunk_size, total - pos);
            hcg::internal::WriteBlockRequest wreq;
            *wreq.mutable_block() = block;
            wreq.set_offset_in_block(offset_in_block);
            wreq.set_data(buf.data() + pos, n);

            hcg::internal::WriteBlockResponse wrsp;
            if (!client.WriteBlock(wreq, wrsp) || wrsp.status().code() != 0) {
                std::fprintf(stderr, "WriteBlock failed: code=%d msg=%s\n",
                             wrsp.status().code(), wrsp.status().message().c_str());
                return 1;
            }

            offset_in_block += wrsp.bytes_written();
            pos += n;
        }

        // 4) Complete
        hcg::internal::CompleteRequest comp_req;
        comp_req.set_path(remote);
        hcg::internal::CompleteResponse comp_rsp;
        if (!client.Complete(comp_req, comp_rsp) || comp_rsp.status().code() != 0) {
            std::fprintf(stderr, "Complete failed: code=%d msg=%s\n",
                         comp_rsp.status().code(), comp_rsp.status().message().c_str());
            return 1;
        }

        std::printf("Put %s -> %s, size=%zu bytes\n",
                    local.c_str(), remote.c_str(), buf.size());
    } else if (cmd == "cat") {
        if (idx >= argc) {
            usage();
            return 1;
        }
        std::string remote = argv[idx++];

        // 1) GetBlockLocations
        hcg::internal::GetBlockLocationsRequest glreq;
        glreq.set_path(remote);
        glreq.set_offset(0);
        glreq.set_length(0); // 表示全部，这个语义由服务端解释
        hcg::internal::GetBlockLocationsResponse glrsp;
        if (!client.GetBlockLocations(glreq, glrsp) || glrsp.status().code() != 0) {
            std::fprintf(stderr, "GetBlockLocations failed: code=%d msg=%s\n",
                         glrsp.status().code(), glrsp.status().message().c_str());
            return 1;
        }

        // 2) 逐 block 读（阶段0我们先假设只有一个 block）
        for (const auto& bl : glrsp.blocks()) {
            uint64_t remaining = bl.length();
            uint64_t offset_in_block = 0;
            const uint64_t chunk = 4 * 1024 * 1024;

            while (remaining > 0) {
                uint64_t to_read = std::min(remaining, chunk);
                hcg::internal::ReadBlockRequest rreq;
                *rreq.mutable_block() = bl.block();
                rreq.set_offset_in_block(offset_in_block);
                rreq.set_length(to_read);

                hcg::internal::ReadBlockResponse rrsp;
                if (!client.ReadBlock(rreq, rrsp) || rrsp.status().code() != 0) {
                    std::fprintf(stderr, "ReadBlock failed: code=%d msg=%s\n",
                                 rrsp.status().code(), rrsp.status().message().c_str());
                    return 1;
                }
                std::cout.write(rrsp.data().data(), rrsp.data().size());
                offset_in_block += rrsp.data().size();
                remaining -= rrsp.data().size();
                if (rrsp.data().empty()) break; // 防止死循环
            }
        }
    } else {
        usage();
        return 1;
    }

    return 0;
}
