package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {



    static class LockState {
        TransactionId tid;
        Permissions perm;

        public LockState(TransactionId tid, Permissions perm) {
            this.tid = tid;
            this.perm = perm;
        }

        public TransactionId getTid() {
            return tid;
        }

        public Permissions getPerm() {
            return perm;
        }
    }

    private ConcurrentHashMap<PageId, List<LockState>> lockStateMap;

    private ConcurrentHashMap<TransactionId, PageId> waitingInfo;

    public LockManager() {
        lockStateMap = new ConcurrentHashMap<>();
        waitingInfo = new ConcurrentHashMap<>();
    }

    public synchronized boolean grantSLock(TransactionId tid, PageId pid){
        List<LockState> states = lockStateMap.get(pid);

        // 表没有上锁，可以直接上锁然后返回
        if (states == null || states.isEmpty()){
            return lock(pid, tid, Permissions.READ_ONLY);
        }

        // 表中有一个锁，分两种情况考虑：锁是自己的 OR 锁是其他事务的
        if(states.size() == 1){
            LockState state = states.iterator().next();

            // 判断锁是不是自己的
            if (state.getTid().equals(tid)){
                // 锁是自己的，需要判断锁是s锁还是w锁
                // 如果是读锁则无需修改则返回，如果是写锁则加入读锁到锁表中
                return state.getPerm() == Permissions.READ_ONLY || lock(pid, tid, Permissions.READ_ONLY);
            }else{
                // 锁不是自己的
                return state.getPerm() == Permissions.READ_ONLY? lock(pid, tid, Permissions.READ_ONLY): wait(tid, pid);
            }
        }

        // 该页中存在两个或两个以上的锁，需要分情况讨论
        // 2个锁 读锁和写锁属于该事务      2个锁 读锁和写锁不属于该事务
        // 2个或2个以上读锁且其中有一个是属于该事务    2个或2个以上读锁且所有锁不属于该事务
        for (LockState state : states) {
            // 判断锁是否为写锁
            if (state.getPerm() == Permissions.READ_WRITE){
                // 是写锁则可以断定该页只有两个锁且属于同一个事务
                return state.getTid().equals(tid) || wait(tid, pid);
            }
            if (state.getTid().equals(tid)) return true;
        }

        return lock(pid, tid, Permissions.READ_ONLY);
    }

    public synchronized boolean grantXLock(TransactionId tid, PageId pid){
        List<LockState> states = lockStateMap.get(pid);

        // 表没有上锁，可以直接上锁
        if (states == null || states.isEmpty()){
            return lock(pid, tid, Permissions.READ_WRITE);
        }

        // 表中有一个锁的情况
        if (states.size() == 1){
            // 判断该锁是否是自己的
            LockState state = states.iterator().next();
            if (state.getTid().equals(tid)){
                return state.getPerm() == Permissions.READ_WRITE || lock(pid, tid, Permissions.READ_WRITE);
            }else{
                return wait(tid, pid);
            }
        }

        // 该页中存在两个或两个以上的锁，需要分情况讨论
        // 2个锁 读锁和写锁且属于该事务      至少有一个锁不是该事务的
        for (LockState state : states) {
            if (!state.getTid().equals(tid)){
                return wait(tid, pid);
            }
            if (state.getPerm() == Permissions.READ_WRITE)  return true;
        }

        return lock(pid,tid, Permissions.READ_WRITE);
    }

    private synchronized boolean lock(PageId pid, TransactionId tid, Permissions perm){
        List<LockState> states = lockStateMap.get(pid);
        if (states == null){
            states = new ArrayList<>();
            lockStateMap.put(pid, states);
        }
        states.add(new LockState(tid, perm));
        waitingInfo.remove(tid);
        return true;
    }

    private synchronized boolean wait(TransactionId tid, PageId pid){
        waitingInfo.put(tid, pid);
        return false;
    }

    public synchronized boolean unlock(TransactionId tid, PageId pid){
        List<LockState> states = lockStateMap.get(pid);
        if (states == null || states.isEmpty()){
            return false;
        }
        boolean isRemoved = false;
        Iterator<LockState> iterator = states.iterator();
        while (iterator.hasNext()){
            LockState state = iterator.next();
            if (state.getTid().equals(tid)){
                iterator.remove();
                isRemoved = true;
            }
        }

        if (states.isEmpty()){
            lockStateMap.remove(pid);
        }

        return isRemoved;
    }

    public void releaseTransactionLocks(TransactionId tid) {

        List<PageId> toRelease = getTransactionAllPage(tid);
        for (PageId pid : toRelease) {
            unlock(tid,pid);
        }
    }

    //==========================检测死锁的相关方法 beign======================================

    /***
     * 下面是实现等待图来检测是否有死锁的存在，具体理论知识可以参考《数据库系统实现》关于死锁的章节
     *
     * @param tid   申请获取锁的事务
     * @param pid   需要获取的资源(页)
     * @return  如果存在死锁返回真
     */
    public synchronized boolean deadlockOccurred(TransactionId tid, PageId pid){
        List<LockState> holders = lockStateMap.get(pid);

        if (holders == null || holders.isEmpty()) return false;
        List<PageId> tPidHolder = getTransactionAllPage(tid);

        for (LockState state : holders) {
            TransactionId holder = state.getTid();
            if (!holder.equals(tid)){
                boolean res = isWaitingResource(holder, tPidHolder, tid);
                if (res)    return true;
            }
        }
        return false;
    }

    /**
     *  
     *
     * @param tid
     * @param pids
     * @param toRemove
     * @return
     */
    private synchronized boolean isWaitingResource(TransactionId tid, List<PageId> pids, TransactionId toRemove){
        PageId waitPageId = waitingInfo.get(tid);

        if (waitPageId == null) return false;
        for (PageId pid : pids) {
            if (waitPageId.equals(pid)) return true;
        }

        // 如果当前事务等待的页并不在页集合中，那么获取等待页的锁，然后判断等待页中的事务锁是否在等待当前页
        List<LockState> holders = lockStateMap.get(waitPageId);
        if (holders == null || holders.size() == 0) return false;//该资源没有拥有者
        for (LockState state : holders) {
            TransactionId holder = state.getTid();
            if (!holder.equals(toRemove)){
                boolean isWaiting = isWaitingResource(holder, pids, toRemove);
                if (isWaiting) return true;
            }
        }
        return false;
    }

    //==========================检测死锁的相关方法 beign======================================

    //==========================查询与修改两个map信息的相关方法 beign=========================

    /**
     *
     * @param tid
     * @param pid
     * @return
     */
    public LockState getLockState(TransactionId tid, PageId pid){
        List<LockState> states = lockStateMap.get(pid);
        if (states == null || states.isEmpty()){
            return null;
        }

        for (LockState state : states) {
            if (state.getTid().equals(tid)){
                return state;
            }
        }

        return null;
    }

    /**
     * 得到tid所拥有的所有锁，以锁所在的资源pid的形式返回
     *
     * @param tid
     * @return
     */
    private List<PageId> getTransactionAllPage(TransactionId tid){
        ArrayList<PageId> holders = new ArrayList<>();

        for (Map.Entry<PageId, List<LockState>> entry : lockStateMap.entrySet()) {
            List<LockState> stateList = entry.getValue();
            for (LockState ls : stateList) {
                if (ls.getTid().equals(tid)){
                    holders.add(entry.getKey());
                    break;
                }
            }
        }
        return holders;
    }
    //==========================查询与修改两个map信息的相关方法 end===========================
}
