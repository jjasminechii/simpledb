package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        if(pid.getPageNumber() >= BufferPool.getPageSize()) {
            throw new IllegalArgumentException();
        }
        try {
            int pageNum = pid.getPageNumber();
            RandomAccessFile r = new RandomAccessFile(f, "r");  // r for read
            int offset = pageNum * BufferPool.getPageSize();
            byte[] arb = new byte[BufferPool.getPageSize()];
            r.seek(offset);
            r.read(arb);
            HeapPageId pageId = new HeapPageId(pid.getTableId(), pageNum);
            return new HeapPage(pageId, arb);
        } catch (Exception e) {
            e.printStackTrace();
        }
        throw new IllegalArgumentException();
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        RandomAccessFile w = new RandomAccessFile(this.f, "rw");
        int writeLoc = BufferPool.getPageSize() * page.getId().getPageNumber(); // offset into file where page should be written
        w.seek(writeLoc);
        w.write(page.getPageData());  // throws IOException if write fails
        w.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil(this.f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // must use BufferPool.getPage()

        ArrayList<Page> pages = new ArrayList<>();

        // go through each page to find one with space for new tuple
        for (int i = 0; i < this.numPages(); i++) {
            // get current page
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
            // if page has space, insert tuple
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                pages.add(page);
                break;
            }
        }

        // if there were no pages with space, create a new page and append it to the physical file on disk.
        if (pages.size() == 0) {
            // create new page
            HeapPageId newId = new HeapPageId(this.getId(), this.numPages());
            byte[] data = new byte[BufferPool.getPageSize()];
            HeapPage page = new HeapPage(newId, data);
            page.insertTuple(t);
            // append to file on disk
            this.writePage(page);

            pages.add(page);
        }

        return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // must use BufferPool.getPage()
        ArrayList<Page> pages = new ArrayList<>();  // modified pages
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        pages.add(page);
        return pages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapIterator(tid);
    }

    private class HeapIterator implements DbFileIterator {
        private TransactionId transactionId;
        private HeapPage heapFile;
        private Iterator<Tuple> tupleIterator;
        private int currentNum;

        public HeapIterator(TransactionId tid) {
            this.transactionId = tid;
//            this.heapFile = hp;
//            this.currentNum = 0;
//            this.tupleIterator = null;
        }

        @Override
        public void open()
                throws DbException, TransactionAbortedException {
            currentNum = 0;
            if (currentNum < numPages()) {
                HeapPageId hpId = new HeapPageId(getId(), currentNum);
                heapFile = (HeapPage)
                        Database.getBufferPool().getPage(transactionId, hpId,
                                Permissions.READ_ONLY);
                this.tupleIterator = heapFile.iterator();
            } else {
                this.tupleIterator = null;
            }
        }

        @Override
        public boolean hasNext()
                throws DbException, TransactionAbortedException {
            if (this.tupleIterator == null) {
                return false;
            }
            if (this.tupleIterator.hasNext()) {
                return true;
            } else {
                while (currentNum >= 0 && currentNum < (numPages() - 1)) {
                    currentNum++;
                    HeapPageId hpId = new HeapPageId(getId(), currentNum);
                    heapFile = (HeapPage)
                            Database.getBufferPool().getPage(transactionId, hpId,
                                    Permissions.READ_ONLY);
                    this.tupleIterator = heapFile.iterator();
                    if(this.tupleIterator.hasNext()) {
                        return true;
                    }
                }
                return false;
            }
        }

        @Override
        public Tuple next()
                throws DbException, TransactionAbortedException, NoSuchElementException {
            if(tupleIterator == null || !tupleIterator.hasNext()) {
                throw new NoSuchElementException();
            }
            return this.tupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            this.tupleIterator = null;
        }
    }
}


