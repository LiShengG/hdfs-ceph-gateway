#include "hdfs.pb.h"
#include "DatanodeProtocol.pb.h"

using namespace hadoop::hdfs::datanode;

namespace hcg {

// DataNode 与 NameNode 通信的内部服务接口
class IDatanodeInternalService {
public:
    virtual ~IDatanodeInternalService() = default;

    // 向 NameNode 注册
    virtual int registerDatanode(
        const RegisterDatanodeRequestProto& req,
        RegisterDatanodeResponseProto& rsp) = 0;

    // 发送心跳
    virtual int sendHeartbeat(
        const HeartbeatRequestProto& req,
        HeartbeatResponseProto& rsp) = 0;

    // 块报告
    virtual int blockReport(
        const BlockReportRequestProto& req,
        BlockReportResponseProto& rsp) = 0;

    // 增量块报告
    virtual int blockReceivedAndDeleted(
        const BlockReceivedAndDeletedRequestProto& req,
        BlockReceivedAndDeletedResponseProto& rsp) = 0;

    // 报告错误的块
    virtual int reportBadBlocks(
        const ReportBadBlocksRequestProto& req,
        ReportBadBlocksResponseProto& rsp) = 0;

    // 缓存报告
    virtual int cacheReport(
        const CacheReportRequestProto& req,
        CacheReportResponseProto& rsp) = 0;
};

} // namespace hcg
