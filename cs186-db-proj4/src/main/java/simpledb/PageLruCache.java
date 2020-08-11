package simpledb;

public class PageLruCache extends LRUCache<PageId, Page> {

    public PageLruCache(int capacity) {
        super(capacity);
    }

    /**
     * 把页面重新读回缓存，覆盖掉缓存中的脏数据
     */
    public synchronized void reCachePage(PageId pid){
        if (!isCache(pid)){
            throw new IllegalArgumentException();
        }
        DbFile file = Database.getCatalog().getDbFile(pid.getTableId());
        Page originalPage = file.readPage(pid);
        Node node = new Node(pid, originalPage);
        cache.put(pid, node);
    }
}
