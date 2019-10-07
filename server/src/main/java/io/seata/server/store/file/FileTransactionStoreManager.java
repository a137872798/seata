/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.server.store.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import io.seata.common.exception.StoreException;
import io.seata.common.loader.LoadLevel;
import io.seata.common.thread.NamedThreadFactory;
import io.seata.common.util.CollectionUtils;
import io.seata.server.session.BranchSession;
import io.seata.server.session.GlobalSession;
import io.seata.server.session.SessionCondition;
import io.seata.server.session.SessionManager;
import io.seata.server.store.AbstractTransactionStoreManager;
import io.seata.server.store.FlushDiskMode;
import io.seata.server.store.ReloadableStore;
import io.seata.server.store.SessionStorable;
import io.seata.server.store.StoreConfig;
import io.seata.server.store.TransactionStoreManager;
import io.seata.server.store.TransactionWriteStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type File transaction store manager.
 * 基于文件系统生成的 session 存储器
 * @author jimin.jm @alibaba-inc.com
 */
@LoadLevel(name = "file")
public class FileTransactionStoreManager extends AbstractTransactionStoreManager
    implements TransactionStoreManager, ReloadableStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileTransactionStoreManager.class);

    private static final int MAX_THREAD_WRITE = 1;

    /**
     * 用于异步刷盘的线程池
     */
    private ExecutorService fileWriteExecutor;

    private volatile boolean stopping = false;

    private static final int MAX_SHUTDOWN_RETRY = 3;

    private static final int SHUTDOWN_CHECK_INTERNAL = 1 * 1000;

    private static final int MAX_WRITE_RETRY = 5;

    private static final String HIS_DATA_FILENAME_POSTFIX = ".1";

    private static final AtomicLong FILE_TRX_NUM = new AtomicLong(0);

    private static final AtomicLong FILE_FLUSH_NUM = new AtomicLong(0);

    private static final int MARK_SIZE = 4;

    private static final int MAX_WAIT_TIME_MILLS = 2 * 1000;

    private static final int MAX_FLUSH_TIME_MILLS = 2 * 1000;

    private static final int MAX_FLUSH_NUM = 10;

    private static int PER_FILE_BLOCK_SIZE = 65535 * 8;

    private static long MAX_TRX_TIMEOUT_MILLS = 30 * 60 * 1000;

    private static volatile long trxStartTimeMills = System.currentTimeMillis();

    private File currDataFile;

    private RandomAccessFile currRaf;

    private FileChannel currFileChannel;

    private long recoverCurrOffset = 0;

    private long recoverHisOffset = 0;

    /**
     * 会话管理器 内部维护了 globalSession 的容器
     */
    private SessionManager sessionManager;

    private String currFullFileName;

    private String hisFullFileName;

    /**
     * 写入文件的 runnable 对象
     */
    private WriteDataFileRunnable writeDataFileRunnable;

    private ReentrantLock writeSessionLock = new ReentrantLock();

    private volatile long lastModifiedTime;

    private static final int MAX_WRITE_BUFFER_SIZE = StoreConfig.getFileWriteBufferCacheSize();

    private final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(MAX_WRITE_BUFFER_SIZE);

    private static final FlushDiskMode FLUSH_DISK_MODE = StoreConfig.getFlushDiskMode();

    private static final int MAX_WAIT_FOR_FLUSH_TIME_MILLS = 2 * 1000;

    private static final int INT_BYTE_SIZE = 4;

    /**
     * Instantiates a new File transaction store manager.
     * 创建 基于文件得到 session存储对象
     * @param fullFileName   the dir path
     * @param sessionManager the session manager
     * @throws IOException the io exception
     */
    public FileTransactionStoreManager(String fullFileName, SessionManager sessionManager) throws IOException {
        // 首先初始化文件
        initFile(fullFileName);
        fileWriteExecutor = new ThreadPoolExecutor(MAX_THREAD_WRITE, MAX_THREAD_WRITE, Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new NamedThreadFactory("fileTransactionStore", MAX_THREAD_WRITE, true));
        // 提交写入任务
        writeDataFileRunnable = new WriteDataFileRunnable();
        fileWriteExecutor.submit(writeDataFileRunnable);
        this.sessionManager = sessionManager;
    }

    /**
     * 初始化文件
     * @param fullFileName
     * @throws IOException
     */
    private void initFile(String fullFileName) throws IOException {
        this.currFullFileName = fullFileName;
        this.hisFullFileName = fullFileName + HIS_DATA_FILENAME_POSTFIX;
        try {
            currDataFile = new File(currFullFileName);
            if (!currDataFile.exists()) {
                //create parent dir first
                if (currDataFile.getParentFile() != null && !currDataFile.getParentFile().exists()) {
                    currDataFile.getParentFile().mkdirs();
                }
                currDataFile.createNewFile();
                trxStartTimeMills = System.currentTimeMillis();
            } else {
                trxStartTimeMills = currDataFile.lastModified();
            }
            lastModifiedTime = System.currentTimeMillis();
            // 创建文件引用对象 并获取 文件写入channel
            currRaf = new RandomAccessFile(currDataFile, "rw");
            currRaf.seek(currDataFile.length());
            currFileChannel = currRaf.getChannel();
        } catch (IOException exx) {
            LOGGER.error("init file error," + exx.getMessage());
            throw exx;
        }
    }

    /**
     * 将 session 根据指定的operation 写入
     * @param logOperation the log operation
     * @param session      the session
     * @return
     */
    @Override
    public boolean writeSession(LogOperation logOperation, SessionStorable session) {
        // 加锁避免 该对象被并发访问
        writeSessionLock.lock();
        long curFileTrxNum;
        try {
            // 写入失败
            if (!writeDataFile(new TransactionWriteStore(session, logOperation).encode())) {
                return false;
            }
            lastModifiedTime = System.currentTimeMillis();
            curFileTrxNum = FILE_TRX_NUM.incrementAndGet();
            // 看来每次 写入 一定的数量就要保存一下
            if (curFileTrxNum % PER_FILE_BLOCK_SIZE == 0 &&
                (System.currentTimeMillis() - trxStartTimeMills) > MAX_TRX_TIMEOUT_MILLS) {
                return saveHistory();
            }
        } catch (Exception exx) {
            LOGGER.error("writeSession error," + exx.getMessage());
            return false;
        } finally {
            writeSessionLock.unlock();
        }
        // 刷盘
        flushDisk(curFileTrxNum, currFileChannel);
        return true;
    }

    /**
     * 刷盘逻辑
     * @param curFileNum
     * @param currFileChannel
     */
    private void flushDisk(long curFileNum, FileChannel currFileChannel) {

        if (FLUSH_DISK_MODE == FlushDiskMode.SYNC_MODEL) {
            // 创建同步刷盘任务 并设置到 runnable 中
            SyncFlushRequest syncFlushRequest = new SyncFlushRequest(curFileNum, currFileChannel);
            writeDataFileRunnable.putRequest(syncFlushRequest);
            // 同步的特点就是 会阻塞当前线程并等待刷盘完成
            syncFlushRequest.waitForFlush(MAX_WAIT_FOR_FLUSH_TIME_MILLS);
        } else {
            // 异步刷盘不需要阻塞
            writeDataFileRunnable.putRequest(new AsyncFlushRequest(curFileNum, currFileChannel));
        }
    }

    /**
     * get all overTimeSessionStorables
     * merge write file
     *
     * @throws IOException
     */
    private boolean saveHistory() throws IOException {
        boolean result;
        try {
            // 找到所有超时session 写入到文件中并刷盘
            result = findTimeoutAndSave();
            // 这里设置了一个请求对象
            writeDataFileRunnable.putRequest(new CloseFileRequest(currFileChannel, currRaf));
            // 将文件移动到另一个 路径
            Files.move(currDataFile.toPath(), new File(hisFullFileName).toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException exx) {
            LOGGER.error("save history data file error," + exx.getMessage());
            result = false;
        } finally {
            // 重新初始化文件
            initFile(currFullFileName);
        }
        return result;
    }

    /**
     * 获取 所有超时的 session 对象
     * @return
     * @throws IOException
     */
    private boolean findTimeoutAndSave() throws IOException {
        List<GlobalSession> globalSessionsOverMaxTimeout =
            sessionManager.findGlobalSessions(new SessionCondition(MAX_TRX_TIMEOUT_MILLS));
        if (CollectionUtils.isEmpty(globalSessionsOverMaxTimeout)) {
            return true;
        }
        List<byte[]> listBytes = new ArrayList<>();
        int totalSize = 0;
        // 1. find all data and merge
        // 将所有session 写入到 list中
        for (GlobalSession globalSession : globalSessionsOverMaxTimeout) {
            TransactionWriteStore globalWriteStore = new TransactionWriteStore(globalSession, LogOperation.GLOBAL_ADD);
            byte[] data = globalWriteStore.encode();
            listBytes.add(data);
            totalSize += data.length + INT_BYTE_SIZE;
            List<BranchSession> branchSessIonsOverMaXTimeout = globalSession.getSortedBranches();
            if (null != branchSessIonsOverMaXTimeout) {
                for (BranchSession branchSession : branchSessIonsOverMaXTimeout) {
                    TransactionWriteStore branchWriteStore =
                        new TransactionWriteStore(branchSession, LogOperation.BRANCH_ADD);
                    data = branchWriteStore.encode();
                    listBytes.add(data);
                    totalSize += data.length + INT_BYTE_SIZE;
                }
            }
        }
        // 2. batch write
        // 写入到 buffer 中
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(totalSize);
        for (byte[] bytes : listBytes) {
            byteBuffer.putInt(bytes.length);
            byteBuffer.put(bytes);
        }
        // 如果写入到文件成功
        if (writeDataFileByBuffer(byteBuffer)) {
            // 刷盘
            currFileChannel.force(false);
            return true;
        }
        return false;
    }

    // 不允许直接从文件中查询

    @Override
    public GlobalSession readSession(String xid) {
        throw new StoreException("unsupport for read from file, xid:" + xid);
    }

    @Override
    public List<GlobalSession> readSession(SessionCondition sessionCondition) {
        throw new StoreException("unsupport for read from file");
    }

    /**
     * 关闭线程池
     */
    @Override
    public void shutdown() {
        if (null != fileWriteExecutor) {
            fileWriteExecutor.shutdown();
            stopping = true;
            int retry = 0;
            while (!fileWriteExecutor.isTerminated() && retry < MAX_SHUTDOWN_RETRY) {
                ++retry;
                try {
                    Thread.sleep(SHUTDOWN_CHECK_INTERNAL);
                } catch (InterruptedException ignore) {
                }
            }
            if (retry >= MAX_SHUTDOWN_RETRY) {
                // 如果等待这么多时间后 阻塞队列中还是有任务 强制关闭线程池 也就是不再执行队列中的任务
                fileWriteExecutor.shutdownNow();
            }
        }
        try {
            // 强制刷盘   metaData == true 代表连同元数据一起写入到文件中
            currFileChannel.force(true);
        } catch (IOException e) {
            LOGGER.error("filechannel force error", e);
        }
        // 关闭文件句柄
        closeFile(currRaf);
    }

    /**
     * 读取  writeStore 对象
     * @param readSize  the read size
     * @param isHistory the is history
     * @return
     */
    @Override
    public List<TransactionWriteStore> readWriteStore(int readSize, boolean isHistory) {
        File file = null;
        long currentOffset = 0;
        // 判断是否是历史文件  每当写入的数据 达到某个值时 就会将 生成的文件转移到历史文件中
        if (isHistory) {
            file = new File(hisFullFileName);
            currentOffset = recoverHisOffset;
        } else {
            file = new File(currFullFileName);
            currentOffset = recoverCurrOffset;
        }
        // 如果文件存在的情况 下 按照偏移量和 读取的尺寸 解析数据
        if (file.exists()) {
            return parseDataFile(file, readSize, currentOffset);
        }
        return null;
    }

    @Override
    public boolean hasRemaining(boolean isHistory) {
        File file = null;
        RandomAccessFile raf = null;
        long currentOffset = 0;
        if (isHistory) {
            file = new File(hisFullFileName);
            currentOffset = recoverHisOffset;
        } else {
            file = new File(currFullFileName);
            currentOffset = recoverCurrOffset;
        }
        try {
            raf = new RandomAccessFile(file, "r");
            return currentOffset < raf.length();

        } catch (IOException ignore) {
        } finally {
            closeFile(raf);
        }
        return false;
    }

    /**
     * 从指定文件中 按照 维护的 偏移量 以及本次读取的长度 将获取的数据解析成 writeStore 对象
     * @param file
     * @param readSize
     * @param currentOffset
     * @return
     */
    private List<TransactionWriteStore> parseDataFile(File file, int readSize, long currentOffset) {
        List<TransactionWriteStore> transactionWriteStores = new ArrayList<>(readSize);
        RandomAccessFile raf = null;
        FileChannel fileChannel = null;
        try {
            // 生成文件指针对象
            raf = new RandomAccessFile(file, "r");
            raf.seek(currentOffset);
            // 获取文件通道对象
            fileChannel = raf.getChannel();
            // 设置指针
            fileChannel.position(currentOffset);
            // 通道对象的长度
            long size = raf.length();
            // 获取 buffer 对象
            ByteBuffer buffSize = ByteBuffer.allocate(MARK_SIZE);
            // 代表还有数据可读
            while (fileChannel.position() < size) {
                try {
                    // 清空 buffer 对象
                    buffSize.clear();
                    // 返回读取数据的尺寸大小
                    int avilReadSize = fileChannel.read(buffSize);
                    // 代表 已经无数据可读了
                    if (avilReadSize != MARK_SIZE) {
                        break;
                    }
                    // 上面代表 buffer 已经读满
                    buffSize.flip();
                    int bodySize = buffSize.getInt();
                    // 创建一个 数组对象 并将buffer 中的数据转移到 数组中
                    byte[] byBody = new byte[bodySize];
                    ByteBuffer buffBody = ByteBuffer.wrap(byBody);
                    // 代表 读取了多少长度
                    avilReadSize = fileChannel.read(buffBody);
                    // 代表无数据可读
                    if (avilReadSize != bodySize) {
                        break;
                    }
                    TransactionWriteStore writeStore = new TransactionWriteStore();
                    writeStore.decode(byBody);
                    transactionWriteStores.add(writeStore);
                    // 代表达到了预定的长度大小
                    if (transactionWriteStores.size() == readSize) {
                        break;
                    }
                } catch (Exception ex) {
                    LOGGER.error("decode data file error:" + ex.getMessage());
                    break;
                }
            }
            return transactionWriteStores;
        } catch (IOException exx) {
            LOGGER.error("parse data file error:" + exx.getMessage() + ",file:" + file.getName());
            return null;
        } finally {
            try {
                if (null != fileChannel) {
                    // 更新 偏移量
                    if (isHisFile(file)) {
                        recoverHisOffset = fileChannel.position();
                    } else {
                        recoverCurrOffset = fileChannel.position();
                    }
                }
                closeFile(raf);
            } catch (IOException exx) {
                LOGGER.error("file close error," + exx.getMessage());
            }
        }

    }

    private boolean isHisFile(File file) {

        return file.getName().endsWith(HIS_DATA_FILENAME_POSTFIX);
    }

    private void closeFile(RandomAccessFile raf) {
        try {
            if (null != raf) {
                raf.close();
                raf = null;
            }
        } catch (IOException exx) {
            LOGGER.error("file close error," + exx.getMessage());
        }
    }

    /**
     * 写入序列化后的文件
     * @param bs
     * @return
     */
    private boolean writeDataFile(byte[] bs) {
        // 过大 不允许写入
        if (bs == null || bs.length >= Integer.MAX_VALUE - 3) {
            return false;
        }
        ByteBuffer byteBuffer = null;

        // 超过了当前 buffer 的大小 就重新分配一个
        if (bs.length + 4 > MAX_WRITE_BUFFER_SIZE) {
            //allocateNew
            byteBuffer = ByteBuffer.allocateDirect(bs.length + 4);
        } else {
            // 清除之前写入的内容
            byteBuffer = writeBuffer;
            //recycle
            byteBuffer.clear();
        }

        byteBuffer.putInt(bs.length);
        byteBuffer.put(bs);
        // 将数据写入到文件
        return writeDataFileByBuffer(byteBuffer);
    }

    private boolean writeDataFileByBuffer(ByteBuffer byteBuffer) {
        byteBuffer.flip();
        for (int retry = 0; retry < MAX_WRITE_RETRY; retry++) {
            try {
                // 基于fileChannel 将数据从buffer中写入到 channel  这里还没有进行刷盘
                while (byteBuffer.hasRemaining()) {
                    currFileChannel.write(byteBuffer);
                }
                return true;
            } catch (IOException exx) {
                LOGGER.error("write data file error:" + exx.getMessage());
            }
        }
        LOGGER.error("write dataFile failed,retry more than :" + MAX_WRITE_RETRY);
        return false;
    }

    interface StoreRequest {

    }

    /**
     * 刷盘请求对象
     */
    abstract class AbstractFlushRequest implements StoreRequest {
        private final long curFileTrxNum;

        /**
         * 被写入的文件通道
         */
        private final FileChannel curFileChannel;

        protected AbstractFlushRequest(long curFileTrxNum, FileChannel curFileChannel) {
            this.curFileTrxNum = curFileTrxNum;
            this.curFileChannel = curFileChannel;
        }

        public long getCurFileTrxNum() {
            return curFileTrxNum;
        }

        public FileChannel getCurFileChannel() {
            return curFileChannel;
        }
    }

    /**
     * 同步刷盘对象
     */
    class SyncFlushRequest extends AbstractFlushRequest {

        private final CountDownLatch countDownLatch = new CountDownLatch(1);

        public SyncFlushRequest(long curFileTrxNum, FileChannel curFileChannel) {
            super(curFileTrxNum, curFileChannel);
        }

        /**
         * 该方法当 刷盘完成时调用
         */
        public void wakeupCustomer() {
            this.countDownLatch.countDown();
        }

        public void waitForFlush(long timeout) {
            try {
                this.countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOGGER.error("Interrupted", e);
            }
        }
    }

    /**
     * 异步刷盘对象
     */
    class AsyncFlushRequest extends AbstractFlushRequest {

        public AsyncFlushRequest(long curFileTrxNum, FileChannel curFileChannel) {
            super(curFileTrxNum, curFileChannel);
        }

    }

    /**
     * 关闭文件的请求对象
     */
    class CloseFileRequest implements StoreRequest {

        private FileChannel fileChannel;

        private RandomAccessFile file;

        public CloseFileRequest(FileChannel fileChannel, RandomAccessFile file) {
            this.fileChannel = fileChannel;
            this.file = file;
        }

        public FileChannel getFileChannel() {
            return fileChannel;
        }

        public RandomAccessFile getFile() {
            return file;
        }
    }

    /**
     * The type Write data file runnable.
     * 将数据写入到文件中
     */
    class WriteDataFileRunnable implements Runnable {

        /**
         * 内部维护了刷盘请求对象
         */
        private LinkedBlockingQueue<StoreRequest> storeRequests = new LinkedBlockingQueue<>();

        public void putRequest(final StoreRequest request) {
            storeRequests.add(request);
        }

        @Override
        public void run() {
            while (!stopping) {
                try {
                    // 就是不断的从阻塞队列中拉取任务并执行
                    StoreRequest storeRequest = storeRequests.poll(MAX_WAIT_TIME_MILLS, TimeUnit.MILLISECONDS);
                    handleStoreRequest(storeRequest);
                } catch (Exception exx) {
                    LOGGER.error("write file error: {}", exx.getMessage(), exx);
                }
            }
            // 处理内部维护的请求对象
            handleRestRequest();
        }

        /**
         * handle the rest requests when stopping is true
         */
        private void handleRestRequest() {
            int remainNums = storeRequests.size();
            for (int i = 0; i < remainNums; i++) {
                handleStoreRequest(storeRequests.poll());
            }
        }

        /**
         * 处理刷盘请求
         * @param storeRequest
         */
        private void handleStoreRequest(StoreRequest storeRequest) {
            if (storeRequest == null) {
                flushOnCondition(currFileChannel);
            }
            if (storeRequest instanceof SyncFlushRequest) {
                // 同步刷盘
                syncFlush((SyncFlushRequest)storeRequest);
            } else if (storeRequest instanceof AsyncFlushRequest) {
                // 异步刷盘
                async((AsyncFlushRequest)storeRequest);
            } else if (storeRequest instanceof CloseFileRequest) {
                // 刷盘并关闭任务
                closeAndFlush((CloseFileRequest)storeRequest);
            }
        }

        /**
         * 刷盘并关闭任务
         * @param req
         */
        private void closeAndFlush(CloseFileRequest req) {
            long diff = FILE_TRX_NUM.get() - FILE_FLUSH_NUM.get();
            // 对某个文件channel 进行刷盘处理
            flush(req.getFileChannel());
            // 增加刷盘次数
            FILE_FLUSH_NUM.addAndGet(diff);
            // 关闭文件
            closeFile(currRaf);
        }

        private void async(AsyncFlushRequest req) {
            if (req.getCurFileTrxNum() < FILE_FLUSH_NUM.get()) {
                flushOnCondition(req.getCurFileChannel());
            }
        }

        private void syncFlush(SyncFlushRequest req) {
            if (req.getCurFileTrxNum() < FILE_FLUSH_NUM.get()) {
                long diff = FILE_TRX_NUM.get() - FILE_FLUSH_NUM.get();
                flush(req.getCurFileChannel());
                FILE_FLUSH_NUM.addAndGet(diff);
            }
            // notify
            req.wakeupCustomer();
        }

        private void flushOnCondition(FileChannel fileChannel) {
            if (FLUSH_DISK_MODE == FlushDiskMode.SYNC_MODEL) {
                return;
            }
            long diff = FILE_TRX_NUM.get() - FILE_FLUSH_NUM.get();
            if (diff == 0) {
                return;
            }
            if (diff % MAX_FLUSH_NUM == 0 ||
                System.currentTimeMillis() - lastModifiedTime > MAX_FLUSH_TIME_MILLS) {
                flush(fileChannel);
                FILE_FLUSH_NUM.addAndGet(diff);
            }
        }

        private void flush(FileChannel fileChannel) {
            try {
                fileChannel.force(false);
            } catch (IOException exx) {
                LOGGER.error("flush error:" + exx.getMessage());
            }
        }
    }
}
