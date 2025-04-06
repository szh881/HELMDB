#ifndef NVMDB_TRANSACTION_H
#define NVMDB_TRANSACTION_H

#include "heap/nvm_rowid_map.h"
#include "undo/nvm_undo_context.h"
#include "index/nvm_index.h"
#include "common/pactree/pactree_snapshot.h"
#include "transaction/nvm_snapshot.h"

namespace NVMDB {

enum class TxStatus {
    EMPTY,
    IN_PROGRESS,
    WAIT_ABORT,
    COMMITTING,
    ABORTED,
    COMMITTED,
};

enum class TMResult {
    /*
     * Signals that the action succeeded (i.e. update/delete performed, lock was acquired)
     */
    OK,

    /* The affected tuple wasn't visible to the relevant snapshot */
    INVISIBLE,

    /* The affected tuple was already modified by the calling backend */
    SELF_MODIFIED,

    /* The affected tuple was updated by another transaction. */
    UPDATED,

    /* The affected tuple was deleted by another transaction */
    DELETED,

    /*
     * The affected tuple is currently being modified by another session. This will only be returned
     * if (update/delete/lock)_tuple are instructed not to wait.
     */
    BEING_MODIFIED,
    SELF_CREATED,
    SELF_UPDATED,

    /* The transaction generated this version is aborted. */
    ABORTED,
};

class Transaction {
public:
    char *undoRecordCache;

    Transaction();

    ~Transaction();

    void PrepareUndo();

    void Begin();

    void Begin_standbyredo(uint64 m_snapshotCSN, uint64 m_minSnapshot);
    
    void Commit();

    void Commit_standbyredo(uint64 m_commitCSN);

    void Abort();

    void WaitAbort() {
        m_txStatus = TxStatus::WAIT_ABORT;
    }

    UndoRecPtr insertUndoRecord(UndoRecord *record) {
        return m_undoTxContext->insertUndoRecord(record);
    }

    TMResult VersionIsVisible(const NVMTuple& tuple) const;

    // 事务能否修改当前 tuple
    TMResult SatisfiedUpdate(const NVMTuple& tuple) const;

    inline LookupSnapshot GetIndexLookupSnapshot() const {
        return LookupSnapshot {
            .snapshot = m_snapshotCSN,
            .min_csn = m_minSnapshot,
        };
    }

    // Index 只能返回某一个 key 和对应的 RowID，但是并不能做可见性判断，所以需要有一层 IndexAccess
    // 在Index基础上，做事务的可见性判断过滤
    void IndexInsert(NVMIndex *index, DRAMIndexTuple *indexTuple, RowId rowId) {
        Key_t key;
        this->PrepareUndo();
        index->Encode(indexTuple, &key, rowId);
        PrepareIndexInsertUndo(key);
        index->Insert(indexTuple, rowId);
    }

    void IndexDelete(NVMIndex *index, DRAMIndexTuple *indexTuple, RowId rowId) {
        Key_t key;
        this->PrepareUndo();
        index->Encode(indexTuple, &key, rowId);
        PrepareIndexDeleteUndo(key);
        index->Delete(indexTuple, rowId, this->GetTxSlotLocation());
    }

    inline void PushWriteSet(RowIdMapEntry *row) {
        m_writeSet.push_back(row); }

    const UndoTxContext *GetUndoTxContext() const
    {
        return m_undoTxContext.get();  // 返回指针但不转移所有权
    }
    void SetUndoTxContext(std::unique_ptr<UndoTxContext> undoTxContext)
    {

        m_undoTxContext = std::move(undoTxContext);
    }
    TxSlotPtr GetTxSlotLocation() const
    {
        return m_txSlotPtr;
    }
    void SetTxSlotLocation(TxSlotPtr txSlotPtr)
    {
        m_txSlotPtr = txSlotPtr;
    }
    uint64 GetSnapshot() const
    {
        return m_snapshotCSN;
    }
    void SetSnapshot(uint64 snapshot)
    {
        m_snapshotCSN = snapshot;
    }
    uint64 GetCommitCSN() const
    {
        return m_commitCSN;
    }
    void SetCommitCSN(uint64 commitCSN)
    {
        m_commitCSN = commitCSN;
    }
    uint64 GetMinSnapshot() const
    {
        return m_minSnapshot;
    }
    void SetMinSnapshot(uint64 minSnapshot)
    {
        m_minSnapshot = minSnapshot;
    }
    TxStatus GetTxStatus() const
    {
        return m_txStatus;
    }
    void SetTxStatus(TxStatus txStatus)
    {
        m_txStatus = txStatus;
    }
    uint32 GetProcArrayTID() const
    {
        return m_procArrayTID;
    }
    void SetProcArrayTID(uint32 procArrayTID)
    {
        m_procArrayTID = procArrayTID;
    }
    ProcessArray* GetProcessArray() const
    {
        return m_processArray;
    }
    void SetProcessArray(ProcessArray* processArray)
    {
        m_processArray = processArray;
    }

protected:
    void PrepareIndexInsertUndo(const Key_t &key);

    // 删除的时候可以保证，自己可以看见，且没有并发的修改，即在删除之前肯定是可见的。所以回滚直接设置value 为InvalidCSN即可。
    void PrepareIndexDeleteUndo(Key_t &key);

private:
    // NVM对应的undo segment
    std::unique_ptr<UndoTxContext> m_undoTxContext;
    // undo segment对应的事务槽ID
    TxSlotPtr m_txSlotPtr;

    // 小于m_snapshotCSN的版本不会再被写, 可以被安全的读取
    uint64 m_snapshotCSN;

    // 多个事务可能拥有相同的m_commitCSN, 提交时候确定
    uint64 m_commitCSN;

    // 小于m_minSnapshot的Undo不会再被读或写, 可以被安全回收
    uint64 m_minSnapshot;

    // 事务当前的状态
    TxStatus m_txStatus;
    std::vector<RowIdMapEntry *> m_writeSet;

    constexpr static uint32 INVALID_PROC_ARRAY_INDEX = 0xffffffff;
    uint32 m_procArrayTID = {INVALID_PROC_ARRAY_INDEX};
    ProcessArray* m_processArray;
};


// 线程本地事务变量对应的方法
void InitTransactionContext();
void DestroyTransactionContext();
Transaction *GetCurrentTxContext();

}  // namespace NVMDB

#endif  // NVMDB_TRANSACTION_H