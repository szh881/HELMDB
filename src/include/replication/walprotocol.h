/* -------------------------------------------------------------------------
 *
 * walprotocol.h
 *	  Definitions relevant to the streaming WAL transmission protocol.
 *
 * Portions Copyright (c) 2010-2012, PostgreSQL Global Development Group
 *
 * src/include/replication/walprotocol.h
 *
 * We tried to expand the traditional WAL protocol to transfer the separated replication data with
 * the specified #ref_xlog which could benefit from both the xlog synchronization and the I/O reducing.
 * -------------------------------------------------------------------------
 */
#ifndef _WALPROTOCOL_H
#define _WALPROTOCOL_H

#include "access/xlogdefs.h"
#include "datatype/timestamp.h"
#include "replication/replicainternal.h"
#include "pgxc/barrier.h"
#include <vector>
#define XLOG_NAME_LENGTH 24

/*
 * All messages from WalSender must contain these fields to allow us to
 * correctly calculate the replication delay.
 */
typedef struct ConfigModifyTimeMessage {
    time_t config_modify_time;
} ConfigModifyTimeMessage;

/*
 * All messages from WalSender must contain these fields to allow us to
 * correctly calculate the replication delay.
 */
typedef struct {
    /* Current end of WAL on the sender */
    XLogRecPtr walEnd;
    ServerMode peer_role;
    DbState peer_state;
    /* Sender's system clock at the time of transmission */
    TimestampTz sendTime;

    /*
     * If replyRequested is set, the client should reply immediately to this
     * message, to avoid a timeout disconnect.
     */
    bool replyRequested;
    bool catchup;
    bool uwal_catchup;
    bool fullSync = false;
} WalSndrMessage;

#define NVM_TYPE_CREATE 0x0001
#define NVM_TYPE_INSERT 0x0002
#define NVM_TYPE_DELETE 0x0004
#define NVM_TYPE_UPDATE 0x0008
#define NVM_TYPE_COMMIT 0x0010
#define NVM_TABLE_COL_NUM 32

struct NVMColumnDesc {
    // NVM 数据类型
    uint32  m_colType;
    // PG数据类型, 可以和m_colType相互转换
    uint32 m_colOid;
    // 类型占用空间
    uint64 m_colLen;
    // 这一列在一行中的数据偏移量
    uint64 m_colOffset;
    // 是否允许为空
    bool m_isNotNull;
    // 列名
    char m_colName[128];
};

struct NVMSerializeColumn {
    uint32 colOid;
    uint64 colLen;
    char colData[1024];
    bool isNull;
    char colName[128];
};

struct NVMRowId
{
	uint32 rowId;
	uint16 m_reserve;
};

typedef struct NVMSndMessage
{
	uint64	xid;
	int64_t type;
    int64_t relid;
    /* the total columns number */
    uint32 col_cnt = 0; 

    /* for insert tuple */
    NVMSerializeColumn col_data[NVM_TABLE_COL_NUM];

    /* for update */
    NVMRowId old_rowid;
    uint8 bitmap[16];

    /* for create table */
    uint64 row_len = 0;
    NVMColumnDesc col_desc[NVM_TABLE_COL_NUM];
}NVMSndMessage;

/*
 * Refence :PrimaryKeepaliveMessage
 * All messages from WalSender must contain these fields to allow us to
 * correctly calculate the replication delay.
 */
typedef struct RmXLogMessage {
    /* Current end of WAL on the sender */
    ServerMode peer_role;
    DbState peer_state;

    /* Sender's system clock at the time of transmission */
    TimestampTz sendTime;

    /*
     * If replyRequested is set, the client should reply immediately to this
     * message, to avoid a timeout disconnect.
     */
    bool replyRequested;
} RmXLogMessage;

/*
 * Refence :PrimaryKeepaliveMessage
 */
typedef struct EndXLogMessage {
    /* Current end of WAL on the sender */
    ServerMode peer_role;
    DbState peer_state;

    /* Sender's system clock at the time of transmission */
    TimestampTz sendTime;

    int percent;
} EndXLogMessage;

/*
 * Header for a WAL data message (message type 'w').  This is wrapped within
 * a CopyData message at the FE/BE protocol level.
 *
 * The header is followed by actual WAL data.  Note that the data length is
 * not specified in the header --- it's just whatever remains in the message.
 *
 * walEnd and sendTime are not essential data, but are provided in case
 * the receiver wants to adjust its behavior depending on how far behind
 * it is.
 */
typedef struct {
    /* WAL start location of the data included in this message */
    XLogRecPtr dataStart;

    /* Current end of WAL on the sender */
    XLogRecPtr walEnd;

    /* Sender's system clock at the time of transmission */
    TimestampTz sendTime;

    XLogRecPtr sender_sent_location;
    XLogRecPtr sender_write_location;
    XLogRecPtr sender_flush_location;
    XLogRecPtr sender_replay_location;

    bool catchup;
} WalDataMessageHeader;

/*
 * Header for a data replication message (message type 'd').  This is wrapped within
 * a CopyData message at the FE/BE protocol level.
 *
 * The header is followed by actual data page.  Note that the data length is
 * not specified in the header --- it's just whatever remains in the message.
 *
 * walEnd and sendTime are not essential data, but are provided in case
 * the receiver wants to adjust its behavior depending on how far behind
 * it is.
 */
typedef struct {
    /* the corresponding data_start_ptr and data_end_ptr */
    XLogRecPtr start_ptr;
    XLogRecPtr end_ptr;

    /* Sender's system clock at the time of transmission */
    TimestampTz sendTime;
} WalDataPageMessageHeader;

/*
 * Refence :ArchiveXlogMessage
 */
typedef struct ArchiveXlogMessage {
    XLogRecPtr targetLsn;
    uint term;
    int sub_term; 
    uint slice;
    uint32 tli;
    char slot_name[NAMEDATALEN];
} ArchiveXlogMessage;

/*
 * Refence :ArchiveXlogResponseMessage
 */
typedef struct ArchiveXlogResponseMessage {
    bool pitr_result;
    XLogRecPtr targetLsn;
    char slot_name[NAMEDATALEN];
} ArchiveXlogResponseMessage;

/*
 * Keepalive message from primary (message type 'k'). (lowercase k)
 * This is wrapped within a CopyData message at the FE/BE protocol level.
 *
 * Note that the data length is not specified here.
 */
typedef WalSndrMessage PrimaryKeepaliveMessage;

/*
 * switchover response
 */
typedef enum {
    SWITCHOVER_NONE = 0,
    SWITCHOVER_PROMOTE_REQUEST,
    SWITCHOVER_DEMOTE_FAILED,
    SWITCHOVER_DEMOTE_CATCHUP_EXIST
} SwitchResponseCode;

typedef enum {
    PITR_TASK_NONE = 0,
    PITR_TASK_GET,
    PITR_TASK_DONE
} PITR_TASK_STATUS;

/*
 * switchover response message from primary (message type 'p').  This is wrapped within
 * a CopyData message at the FE/BE protocol level.
 *
 * Note that the data length is not specified here.
 */
typedef struct {
    int switchResponse;

    /* Current end of WAL on the sender */
    XLogRecPtr walEnd;

    /* Sender's system clock at the time of transmission */
    TimestampTz sendTime;
} PrimarySwitchResponseMessage;

/*
 * Reply message from standby (message type 'r').  This is wrapped within
 * a CopyData message at the FE/BE protocol level.
 *
 * Note that the data length is not specified here.
 */
typedef struct StandbyReplyMessage {
    /*
     * The xlog locations that have been received, written, flushed, and applied by
     * standby-side. These may be invalid if the standby-side is unable to or
     * chooses not to report these.
     */
    XLogRecPtr receive;
    XLogRecPtr write;
    XLogRecPtr flush;
    XLogRecPtr apply;
    XLogRecPtr applyRead;

    /* local role  on walreceiver, they will be sent to walsender */
    ServerMode peer_role;
    DbState peer_state;

    /* Sender's system clock at the time of transmission */
    TimestampTz sendTime;

    /*
     * If replyRequested is set, the server should reply immediately to this
     * message, to avoid a timeout disconnect.
     */
    bool replyRequested;

    /* flag array
     * 0x00000001 flag IS_PAUSE_BY_TARGET_BARRIER
     * 
     * 0x00000010 flag IS_CANCEL_LOG_CTRL
     * If this flag is true, the walsend will set
     */
    uint32 replyFlags;
} StandbyReplyMessage;

typedef struct UwalCatchEndMessage {
    /* Sender's system clock at the time of transmission */
    TimestampTz sendTime;
    bool replyRequested;
} UwalCatchEndMessage;

/*
 * Hot Standby feedback from standby (message type 'h').  This is wrapped within
 * a CopyData message at the FE/BE protocol level.
 *
 * Note that the data length is not specified here.
 */
typedef struct StandbyHSFeedbackMessage {
    /*
     * The current xmin and epoch from the standby, for Hot Standby feedback.
     * This may be invalid if the standby-side does not support feedback, or
     * Hot Standby is not yet available.
     */
    TransactionId xmin;
    /* Sender's system clock at the time of transmission */
    TimestampTz sendTime;
} StandbyHSFeedbackMessage;

/*
 * @@GaussDB@@
 * switchover request message from standby (message type 's').  This is wrapped within
 * a CopyData message at the FE/BE protocol level.
 *
 * Note that the data length is not specified here.
 */
typedef struct StandbySwitchRequestMessage {
    /* primary demote mode */
    int demoteMode;

    /* receiver's system clock at the time of transmission */
    TimestampTz sendTime;
} StandbySwitchRequestMessage;

/*
 * switchover request message in the streaming dr (message type '').  This is wrapped within
 * a CopyData message at the FE/BE protocol level.
 *
 *
 */
typedef struct {
    /* The barrier LSN used by the streaming dr switchover this time */
    XLogRecPtr switchoverBarrierLsn;
    bool isMasterInstanceReady;
} HadrSwitchoverMessage;

/*
 * Reply message from hadr standby (message type 'R').
 *
 * Note that the data length is not specified here.
 */
typedef struct HadrReplyMessage {
    /* The target barrier Id in standby cluster */
    char targetBarrierId[MAX_BARRIER_ID_LENGTH];
    /* receiver's system clock at the time of transmission */
    TimestampTz sendTime;
    /* The target barrier LSN used by the streaming dr */
    XLogRecPtr targetBarrierLsn;
    /* reserved fields */
    uint32 pad1;
    uint32 pad2;
    uint64 pad3;
    uint64 pad4;
} HadrReplyMessage;

extern void NVMDataSend(void);
extern std::vector<NVMSndMessage> GetNVMDataMessage(void);
extern void						  PushNVMDataMessage(NVMSndMessage message);
extern void						  ReplayNVMDataFromQueue(void);
/*
 * Maximum data payload in a WAL data message.	Must be >= XLOG_BLCKSZ.
 *
 * We don't have a good idea of what a good value would be; there's some
 * overhead per message in both walsender and walreceiver, but on the other
 * hand sending large batches makes walsender less responsive to signals
 * because signals are checked only between messages.  128kB (with
 * default 8k blocks) seems like a reasonable guess for now.
 */
#define MAX_SEND_SIZE (XLOG_BLCKSZ * 16)

#endif /* _WALPROTOCOL_H */
