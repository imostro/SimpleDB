package simpledb.structure;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class LRUCache<K, V> {

    /**
     * 缓存容量大小
     */
    private final int capacity;

    /**
     * 链表头
     */
    private final Node head;

    /**
     * 链表尾
     */
    private final Node tail;

    /**
     * 缓存哈希表
     */
    private final HashMap<K, Node> cache;

    class Node{
        K key;
        V value;
        Node pre;
        Node next;

        public Node() {
        }

        public Node(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    /**
     * 从链表中移除节点
     * @param node 移除的节点
     */
    private void removeNode(Node node){
        Node pre = node.pre;
        Node next = node.next;

        pre.next = next;
        next.pre = pre;
    }

    /**
     * 添加一个节点到链表头
     * @param node 添加的节点
     */
    private void addNode(Node node){
        Node next = head.next;

        head.next = node;
        next.pre = node;

        node.next = next;
        node.pre = head;
    }

    /**
     * 从链表尾部删除一个节点
     * @return 删除的节点
     */
    private Node popTail(){
        Node pre = tail.pre;
        removeNode(pre);
        return pre;
    }

    /**
     * 把节点从链表移动到链表头
     * @param node
     */
    private void moveNodeToHead(Node node){
        removeNode(node);
        addNode(node);
    }

    public LRUCache(int capacity) {
        this.capacity = capacity;
        this.cache = new HashMap<>(capacity);
        this.head = new Node();
        this.tail = new Node();
        head.next = tail;
        tail.pre = head;
    }

    /**
     * 通过Key从缓存中获取数据
     * @param key 键
     * @return 返回获取到的数据
     */
    public V get(K key){
        Node node = cache.get(key);
        if (node == null)   return null;
        moveNodeToHead(node);
        return node.value;
    }

    public V put(K key, V value){
        if (key == null | value == null) {//不允许插入null值
            throw new IllegalArgumentException();
        }

        Node node = cache.get(key);
        V oldValue = null;
        if (node == null){
            if (size() >= capacity){
                Node del_node = popTail();
                cache.remove(del_node.key);
            }
            node = new Node(key, value);
            cache.put(key, node);
            addNode(node);
        }else {
            oldValue = node.value;
            node.value = value;
            moveNodeToHead(node);
        }

        return oldValue;
    }

    /**
     * 当前缓存的数据，但不会大于capacity.
     *
     * @return 缓存节点数
     */
    public int size(){
        return cache.size();
    }

    /**
     * 判断key是否在缓存中
     * @param k key
     * @return boolean
     */
    public boolean isCache(K k){
        return cache.containsKey(k);
    }

    public Iterator<V> iterator(){
        return new LRUIter();
    }

    private class LRUIter implements Iterator<V>{

        Iterator<Node> iter = cache.values().iterator();

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public V next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return iter.next().value;
        }
    }
}
