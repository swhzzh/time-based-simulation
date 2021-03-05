package ruc.swh;

import com.google.common.base.Objects;
import org.isomorphism.util.TokenBucket;
import org.isomorphism.util.TokenBuckets;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

public class Workload implements Runnable{

    private String mName;
    private long mId;
    private long mDatasetId;
    private int mChunkNum;
    private int mEpochNum;
    private int mChunkSize;
    private int mChunkItemCount;
    private int mChunkItemSize;

    private int mBatchSize;
    private int mChunkBatchNum;
    private int mBatchItemCount;
    private long mBatchComputationTime;
    private long mChunkComputationTime;

    // 作为最大处理速度
    private int mMaxNumOfItemsPerSecond;
    private TokenBucket mTokenBucket;

    private int mCurrentEpoch;
    private int mStartChunk;
    private int mLatestChunk;
    private long mLatestTime;
//    private Map<Integer, Boolean> mChunkIdToUseMap;

    private ResourceManager mResourceManager;


    private long mWaitTime;

    private long mCurrentChunkRemainTime;

    private final Set<Integer> readChunks = new HashSet<>();

    private BufferedWriter mLogWriter;
    private BufferedWriter mChunkReadTimeWriter;

    public Workload(long id, long datasetId, int chunkNum, int epochNum, int batchItemCount,
        int chunkBatchNum, int maxNumOfItemsPerSecond, ResourceManager resourceManager) {
        mId = id;
        mDatasetId = datasetId;
        mChunkNum = chunkNum;
        mEpochNum = epochNum;
        mBatchItemCount = batchItemCount;
        mChunkBatchNum = chunkBatchNum;
        mChunkItemCount = mBatchItemCount * mChunkBatchNum;
        mMaxNumOfItemsPerSecond = maxNumOfItemsPerSecond;
        mResourceManager = resourceManager;
        mWaitTime = 0;
        mTokenBucket = TokenBuckets
            .builder().withCapacity(mMaxNumOfItemsPerSecond).withFixedIntervalRefillStrategy(mMaxNumOfItemsPerSecond / 1000, 1, TimeUnit.MILLISECONDS).build();;
    }

    // get time-based chunk access seq
    public WorkloadRunningTimeInfo getCurrentTimeInfo(){
        int minChunkConsumeTime = mChunkItemCount * 1000 / mMaxNumOfItemsPerSecond; //ms
        return new WorkloadRunningTimeInfo(mId, mDatasetId, mChunkNum, mLatestChunk, mLatestTime, minChunkConsumeTime);
    }

    public WorkloadDataReadingInfo getCurrentDataReadingInfo(){
        int minChunkConsumeTime = mChunkItemCount * 1000 / mMaxNumOfItemsPerSecond; //ms
        List<Integer> readChunkList = new ArrayList<>();
        List<Integer> unReadChunkList = new ArrayList<>();
        getReadAndUnReadChunks(readChunkList, unReadChunkList);
        return new WorkloadDataReadingInfo(mId, mDatasetId, mChunkNum, minChunkConsumeTime, mLatestTime, mCurrentChunkRemainTime, readChunkList, unReadChunkList);
    }




    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        int minChunkConsumeTime = mChunkItemCount * 1000 / mMaxNumOfItemsPerSecond;
        for (int i = 0; i < this.mEpochNum; i++) {
            System.out.println(this.mId + "'s " + "current epoch: " + i);
            try {
                mLogWriter.write(this.mId + "'s " + "current epoch: " + i + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
            mCurrentEpoch = i;

            mStartChunk = mResourceManager.getStartChunk(mDatasetId);
            mLatestChunk = mStartChunk;
            mLatestTime = System.currentTimeMillis();
            mCurrentChunkRemainTime = minChunkConsumeTime;
            int skipChunks = 0;
            while (readChunks.size() < mChunkNum){
                for (int j = 0; j < mChunkNum; j++) {
                    mLatestTime = System.currentTimeMillis();
                    mCurrentChunkRemainTime = minChunkConsumeTime;
                    if (readChunks.contains(j)){
                        continue;
                    }
                    if (!mResourceManager.isChunkCached(mDatasetId, j) && skipChunks < mChunkNum - readChunks.size()){
//                        System.out.println(mId + ": skip chunk " + j);
                        skipChunks++;
                        continue;
                    }
//                    if (mResourceManager.isChunkCached(mDatasetId,j)){
//                    if (skipChunks == mChunkNum - readChunks.size()){
                        System.out.println(mId + ": skip " + skipChunks + " chunk, need to consume uncached chunk " + j);
                        skipChunks = 0;
//                    }
                    long chunkStartTime = System.currentTimeMillis();
                        // load & evict
                        // load时计算当前未读的chunk中有多少cached的, 从未cached的并且未读的chunk中选出最小的两个作为备选
                        // evict时选择读过的chunk中的cache的最大的两个chunk作为备选
                        // 利用个数来计算上述相应的时间戳,
                        // 由于dataset的每个chunk都可以看作是等价的, 其标号并无实际意义, 甚至顺序越乱越好,
                        // 所以这种方式虽然最终计算出来的时间戳和相应的chunk的实际读取时间并不完全一致, 也不影响根据时间排序的这种思想
                        // share
                        // 对于共享dataset的情况, 因为都是从未cached的chunk中选, 所以还是按时间来选, 也没问题.
                    System.out.println(this.mId + "'s " + "current chunk: " + j);
                    try {
                        mLogWriter.write(this.mId + "'s " + "current chunk: " + j + "\n");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    mResourceManager.notifyUsingChunk(mId, mDatasetId, j);

                        // consume data
                        // 如果是cache, 那io就不是瓶颈, 只需要不停的算就行, 而反之则io成为瓶颈, 计算需要等io
                        // 能不能达到最大带宽, 也看本机器的性能
                        for (int k = 0; k < mChunkBatchNum; k++) {
                            // 这里不应该是顺序的
                            //
                            //                    System.out.println(mId + ": read batch " + k);
                            mTokenBucket.consume(mBatchItemCount);
                            while (true){
                                //                        System.out.println(mId + ": read batch " + k);
                                boolean result = mResourceManager.getBatchData(mId, mDatasetId, j, mBatchItemCount);
                                if (!result){
                                    try {
                                        Thread.sleep(10);
                                        mWaitTime += 10;
                                        //                                System.out.println(mId + "-workload waits for: " + mWaitTime);
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                }
                                else {
                                    break;
                                }
                            }
                            mCurrentChunkRemainTime = mCurrentChunkRemainTime * (mChunkBatchNum - k) / mChunkBatchNum;
                        }

                        mResourceManager.notifyDoneUsingChunk(mId, mDatasetId, j);
                        mCurrentChunkRemainTime = 0;
                        readChunks.add(j);
                    try {
                        mChunkReadTimeWriter.write(mId + ":" + i + ":" + j + ":" + (System.currentTimeMillis() - chunkStartTime) + "\n");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
//                        mLatestChunk = (mLatestChunk + 1) % mChunkNum;
//                        mLatestTime = System.currentTimeMillis();
//                    }

                }
            }
            readChunks.clear();
            try {
                mLogWriter.flush();
                mChunkReadTimeWriter.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        long totalRunningTime = System.currentTimeMillis() - startTime;
        System.out.println(mId + "-workload runs for " + totalRunningTime + "ms");
        System.out.println(mId + "-workload waits for: " + mWaitTime + "ms");
        try {
            mLogWriter.write(mId + "-workload runs for " + totalRunningTime + "ms\n");
            mLogWriter.write(mId + "-workload waits for: " + mWaitTime + "ms" + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public long getId() {
        return mId;
    }

    public long getDatasetId() {
        return mDatasetId;
    }

    public void getReadAndUnReadChunks(List<Integer> readChunkList, List<Integer> unReadChunkList){
        for (int i = 0; i < mChunkNum; i++) {
            if (!readChunks.contains(i)){
                unReadChunkList.add(i);
            }
            else {
                readChunkList.add(i);
            }
        }
    }

    public void setLogWriter(BufferedWriter logWriter) {
        mLogWriter = logWriter;
    }

    public void setChunkReadTimeWriter(BufferedWriter mChunkReadTimeWriter) {
        this.mChunkReadTimeWriter = mChunkReadTimeWriter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Workload workload = (Workload) o;
        return mId == workload.mId;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(mId);
    }
}
