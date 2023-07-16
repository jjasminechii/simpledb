package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private Map<PageId, Page> pool;
    private int numPages;
    private LockManager lockManager;


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.pool = new HashMap<>();
        this.numPages = numPages;
        this.lockManager = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // TODO: i feel like we should double check this method, especially the evicting parts
        boolean acquired = false;
        while (!acquired) {
            synchronized (this) {
                if (perm == Permissions.READ_WRITE) {
                    acquired = this.lockManager.acquireExclusiveLock(tid, pid);
                } else {
                    acquired = this.lockManager.acquireSharedLock(tid, pid);
                }
            }
            // if lock wasn't acquired, pause the thread execution so we're not stuck forever
            if (!acquired) {
                try {
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        // for lab 1: if more than numPages requests are made for different pages,
        // instead of implementing an eviction policy, throw a DbException
        if (this.pool.size() >= numPages) {
            evictPage();
        }

        if (!this.pool.containsKey(pid)) {
            // if page not in buffer pool, get from disk
            if(pool.size() > numPages) {  // if too many pages we want to evict
                evictPage();
            }
            int tableId = pid.getTableId();
            DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
            Page page = dbFile.readPage(pid);
            this.pool.put(pid, page);
        }
        return this.pool.get(pid);
//        int lock = 1;
//        if(perm == Permissions.READ_ONLY) {
//            lock = 0;
//        }
//        if (!this.pool.containsKey(pid)) {
//            // if page not in buffer pool, get from disk
//            if(pool.size() > numPages) {  // if too many pages we want to evict
//                evictPage();
//            }
//            int tableId = pid.getTableId();
//            DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
//            Page page = dbFile.readPage(pid);
//            this.pool.put(pid, page);
//            return page;
//        }
//        return this.pool.get(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public synchronized void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        // only release lock on a page if transaction has committed
        lockManager.releaseExclusiveLock(tid, pid);
        lockManager.releaseSharedLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.hasExclusiveLock(tid, p) || lockManager.hasSharedLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public synchronized void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // Lab 4: NO FORCE: no longer need to force pages to disk at commit time
        //                  for each dirtied page, logWrite(tid, p.getBeforeImage(), p)
        //                  and then force the log to disk
        if(commit) {
            for (PageId pageId : pool.keySet()) {
                Page page = pool.get(pageId);
//                if (page.isDirty() == tid) {
//                    flushPage(pageId);
//                }
                Database.getLogFile().logWrite(tid, page.getBeforeImage(), page);
                Database.getLogFile().force();
                // use current page contents as the before-image
                // for the next transaction that modifies this page.
                page.setBeforeImage();

            }
        } else {
            for (PageId pageId : pool.keySet()) {
                Page page = pool.get(pageId);
                if (page.isDirty() == tid) {
                    int tableId = pageId.getTableId();
                    DbFile file = Database.getCatalog().getDatabaseFile(tableId);
                    Page pageDisk = file.readPage(pageId);
                    pool.put(pageId, pageDisk);
                }
            }
        }
        for (PageId pageId : pool.keySet()) {
            if (holdsLock(tid, pageId)) {
                releasePage(tid, pageId);
            }
        }
        // clean up dependency graph and locks
        lockManager.removeDependency(tid);
        lockManager.finishTransaction(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // get heap file (table)
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        // insert tuple into table and get back list of modified pages
        ArrayList<Page> pages = heapFile.insertTuple(tid, t);
        // mark any pages that were dirtied
        for (Page page : pages) {
            page.markDirty(true, tid);
            // if too many pages, evict
            if (pool.size() > numPages) {
                evictPage();
            }
            pool.put(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // get heap file (table)
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        // insert tuple into table and get back list of modified pages
        ArrayList<Page> pages = heapFile.deleteTuple(tid, t);
        // mark any pages that were dirtied
        for (Page page : pages) {
            page.markDirty(true, tid);
            pool.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for(Page current : pool.values()) {
            flushPage(current.getId());
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        pool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        if (pool.containsKey(pid)) {
            Page current = pool.get(pid);
                // append an update record to the log, with
                // a before-image and after-image.
                TransactionId dirtier = current.isDirty();
                if (dirtier != null){
                    Database.getLogFile().logWrite(dirtier, current.getBeforeImage(), current);

                    int tableid = pid.getTableId();
                    Database.getCatalog().getDatabaseFile(tableid).writePage(current);
                    current.markDirty(false, null);
                }
            }
        }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // Lab 3: NO-STEAL: cannot evict dirty page
        //        if all pages are dirty, throw DbException
//        PageId pageToEvict = null;
//        for (PageId pageId : pool.keySet()) {
//            Page currentPage = pool.get(pageId);
//            // only evict if not dirty
//            if (currentPage.isDirty() == null) {
//                pageToEvict = currentPage.getId();
//                break;
//            }
//        }
//
//        if (pageToEvict == null) {
//            throw new DbException("all pages are dirty; cannot evict any");
//        }

        // Lab 4: STEAL: can flush any page to disk
        //        choose a page randomly
        int randomPageIndex = (int) ((pool.size() - 1) * Math.random());
        ArrayList<PageId> pages = new ArrayList<>(pool.keySet());
        PageId pageToEvict = pages.get(randomPageIndex);

        try {
            flushPage(pageToEvict);
        } catch (IOException e) {
            e.printStackTrace();
        }
        discardPage(pageToEvict);
    }

}
