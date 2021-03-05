package ruc.swh;


import javafx.util.Pair;
import org.isomorphism.util.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

// 多线程模拟最大同时加载的chunk数目, 每个线程内部代表其最大加载速度
public class RemoteDataLoader {

  private ExecutorService mExecutorService;
  private ResourceManager mResourceManager;
  private AtomicInteger mRunningTaskNum;
  private long mRemoteBWPerLoadingTask;
  private BufferedWriter mLogWriter;
  private boolean mNoChunkToLoadLastTime;
  private BufferedWriter mCacheUsageWriter;
  private BufferedWriter mBwUsageWriter;

  public RemoteDataLoader(int threadPoolSize, ResourceManager resourceManager, long remoteBWPerLoadingTask, BufferedWriter logWriter, BufferedWriter cacheUsageWriter, BufferedWriter bandwidthUsageWriter) {
    mExecutorService = Executors.newFixedThreadPool(threadPoolSize);
    mRunningTaskNum = new AtomicInteger(threadPoolSize);
    mResourceManager = resourceManager;
    mRemoteBWPerLoadingTask = remoteBWPerLoadingTask;
    mLogWriter = logWriter;
    mNoChunkToLoadLastTime = false;
    mCacheUsageWriter = cacheUsageWriter;
    mBwUsageWriter = bandwidthUsageWriter;
  }
  
  public void loadChunks(List<WorkloadDataReadingInfo> workloadDataReadingInfos)
      throws IOException, InterruptedException {
    Map<Long, List<Integer>> allCachedChunks = mResourceManager.getAllCachedChunks();

    Map<Pair<Long, Integer>, Long> chunksToLoadToTimeInfoMap = new HashMap<>();
    Map<Pair<Long, Integer>, Long> chunksToEvictToTimeInfoMap = new HashMap<>();

    for (WorkloadDataReadingInfo info : workloadDataReadingInfos) {
      long datasetId = info.getDatasetId();
      List<Integer> readChunks = info.getReadChunks();
      List<Integer> unReadChunks = info.getUnReadChunks();


      int unReadAndCachedChunkNum = 0;
      for (Integer unReadChunk : unReadChunks) {
        if (mResourceManager.isChunkCached(datasetId, unReadChunk)){
          unReadAndCachedChunkNum++;
        }
      }
      int readAndCachedChunkNum = 0;
      for (Integer readChunk : readChunks) {
        if (mResourceManager.isChunkCached(datasetId, readChunk)){
          readAndCachedChunkNum++;
        }
      }

      // 1.get time for chunks to load(unread & uncached)
      // 这里只从当前epoch中未读的chunks中选择要load的chunk, 但也可以为下一个epoch提前加载已读但被evict(目前uncached)的chunk
      // TODO: 2021/2/28 在cache空间有剩余的情况下, 加载上述所说的chunk, 如果空间无剩余, 可能导致自己的同样是已读的cached的chunk被evict, 而这样只会白白浪费带宽, 因为
//      List<Integer> cachedChunks = allCachedChunks.get(datasetId);
      int temp = 0;
      for (Integer unReadChunk : unReadChunks) {
        if (!mResourceManager.isChunkCached(datasetId, unReadChunk)){
          long time = info.getLatestTime() + (unReadAndCachedChunkNum + temp) * info.getMinChunkConsumeTime();
          // 说明第一个chunk还在等, 那第二个chunk的时间最早也是从现在算起, 而不应该用第一个chunk的时间,
          // 第一个chunk应该保留原有的时间, 从而实现等得越久优先级越高
          // TODO: 2021/3/5 这里直接用当前时间作为base也并不那么合理, 首先, 并不一定会load前面的chunk(第一个都无法load, 那之后的都没啥意义, 不过第一个都无法load的话, 那确实之后的肯定无法被load, 也还行), 其次因为可能加载的速度并不如计算快, 也不能用minChunkConsumeTime算.
          if (temp == 1 && time < System.currentTimeMillis()){
            time = System.currentTimeMillis() + (unReadAndCachedChunkNum + temp) * info.getMinChunkConsumeTime();
          }
          chunksToLoadToTimeInfoMap.put(new Pair<>(datasetId, unReadChunk), time);
          temp++;
          if (temp == 2){
            break;
          }
        }
      }
      int tempBase = temp;
      if (temp < 2){
        for (Integer readChunk : readChunks) {
          if (!mResourceManager.isChunkCached(datasetId, readChunk)){
            long time = info.getLatestTime() + (unReadChunks.size() + readChunks.size() - readAndCachedChunkNum + temp - tempBase) * info.getMinChunkConsumeTime();
            if (temp == 1 && time < System.currentTimeMillis()){
              time = System.currentTimeMillis() + (unReadChunks.size() + readChunks.size() - readAndCachedChunkNum + temp - tempBase) * info.getMinChunkConsumeTime();
            }
            chunksToLoadToTimeInfoMap.put(new Pair<>(datasetId, readChunk), time);
            temp++;
            if (temp == 2){
              break;
            }
          }
        }
      }

      // 2.get time for chunks to evict(read & cached)
      // TODO: 2021/3/5 也可能存在某个dataset的cached chunk全部未被读, 但是确实空间不足要淘汰chunk, 现在的实现是只考虑读过的chunk, 如果都没读就不考虑了,
      //  但可能存在未读的后面的chunk的访问时间比别人读过的chunk的下一次访问时间更晚, 或者别人在等着用cache去缓存chunk, 这种时候还是要考虑evict未读的cached的chunk

      temp = 0;
      for (int i = readChunks.size() - 1; i >= 0; i--) {
        int chunkId = readChunks.get(i);
        if (mResourceManager.isChunkCached(datasetId, chunkId)){
          // 这里不应该用latestTime, 而应该计算当前chunk剩余的时间, 否则可能出现当前chunk等了很久, 导致访问过的cache的chunk的时间序列提前的状况.
          chunksToEvictToTimeInfoMap.put(new Pair<>(datasetId, chunkId),
              System.currentTimeMillis() + info.getCurrentChunkRemainTime() + (unReadChunks.size() - 1 + readAndCachedChunkNum - 1 - temp) * info.getMinChunkConsumeTime());
          temp++;
          if (temp == 2){
            break;
          }
        }
      }

      if (temp < 2){
        for (int i = unReadChunks.size() - 1; i >= 0; i--) {
          int chunkId = unReadChunks.get(i);
          if (mResourceManager.isChunkCached(datasetId, chunkId)){
            chunksToEvictToTimeInfoMap.put(new Pair<>(datasetId, chunkId),
                System.currentTimeMillis() + info.getMinChunkConsumeTime() + i * info.getMinChunkConsumeTime());
            temp++;
            if (temp == 2){
              break;
            }
          }
        }
      }

    }

    // 3.decide chunks to load and evict globally
    Comparator<Map.Entry<Pair<Long, Integer>, Long>> ascCmp = Map.Entry.comparingByValue();
    Comparator<Map.Entry<Pair<Long, Integer>, Long>> desCmp = (o1, o2) -> o2.getValue().compareTo(o1.getValue());
    ArrayList<Map.Entry<Pair<Long, Integer>, Long>> chunksToLoadList =
        new ArrayList<>(chunksToLoadToTimeInfoMap.entrySet());
    chunksToLoadList.sort(ascCmp);
    int cacheableChunkNum = mResourceManager.getCacheableChunkNum();
    if (chunksToLoadList.size() == 0){
//      mNoChunkToLoadLastTime = true;
      Thread.sleep(1000);
    }
    else if(chunksToLoadList.size() == 1){
      // need to evict on cached chunk
      if (cacheableChunkNum == 0){
        ArrayList<Map.Entry<Pair<Long, Integer>, Long>> chunksToEvictList =
            new ArrayList<>(chunksToEvictToTimeInfoMap.entrySet());
        if (chunksToEvictList.size() == 0){
          Thread.sleep(1000);
          return;
        }
        chunksToEvictList.sort(desCmp);
        if (chunksToEvictList.get(0).getValue() <= chunksToLoadList.get(0).getValue()){

        }
        else {
          printCacheUsage(allCachedChunks);
          mResourceManager.deleteChunk(chunksToEvictList.get(0).getKey().getKey(), chunksToEvictList.get(0).getKey().getValue());
          loadChunks(chunksToLoadList.get(0).getKey(),null);
        }
      }
      else {
        printCacheUsage(allCachedChunks);
        loadChunks(chunksToLoadList.get(0).getKey(),null);
      }
    }
    else {
      if (cacheableChunkNum == 0){
        ArrayList<Map.Entry<Pair<Long, Integer>, Long>> chunksToEvictList = new ArrayList<>(chunksToEvictToTimeInfoMap.entrySet());
        if (chunksToEvictList.size() == 0){
          Thread.sleep(1000);
          return;
        }
        chunksToEvictList.sort(desCmp);
        if (chunksToEvictList.get(0).getValue() <= chunksToLoadList.get(0).getValue()){

        }
        else if (chunksToEvictList.size() == 1 || (chunksToEvictList.size() > 1 && chunksToEvictList.get(1).getValue() <= chunksToLoadList.get(1).getValue())){
          printCacheUsage(allCachedChunks);
          mResourceManager.deleteChunk(chunksToEvictList.get(0).getKey().getKey(), chunksToEvictList.get(0).getKey()
              .getValue());
          loadChunks(chunksToLoadList.get(0).getKey(),null);
        }
        else {
          printCacheUsage(allCachedChunks);
          mResourceManager.deleteChunk(chunksToEvictList.get(0).getKey().getKey(), chunksToEvictList.get(0).getKey().getValue());
          mResourceManager.deleteChunk(chunksToEvictList.get(1).getKey().getKey(), chunksToEvictList.get(1).getKey().getValue());
          loadChunks(chunksToLoadList.get(0).getKey(), chunksToLoadList.get(1).getKey());
        }
      }
      else if (cacheableChunkNum == 1){
        ArrayList<Map.Entry<Pair<Long, Integer>, Long>> chunksToEvictList = new ArrayList<>(chunksToEvictToTimeInfoMap.entrySet());
        if (chunksToEvictList.size() == 0){
          loadChunks(chunksToLoadList.get(0).getKey(), chunksToLoadList.get(1).getKey());
          Thread.sleep(1000);
          return;
        }
        printCacheUsage(allCachedChunks);
        chunksToEvictList.sort(desCmp);
        if (chunksToEvictList.get(0).getValue() <= chunksToEvictList.get(1).getValue()){
          loadChunks(chunksToLoadList.get(0).getKey(), null);
        }
        else{
          mResourceManager.deleteChunk(chunksToEvictList.get(0).getKey().getKey(), chunksToEvictList.get(0).getKey().getValue());
          loadChunks(chunksToLoadList.get(0).getKey(), chunksToLoadList.get(1).getKey());
        }
      }
      else {
        printCacheUsage(allCachedChunks);
        loadChunks(chunksToLoadList.get(0).getKey(), chunksToLoadList.get(1).getKey());
      }
    }
  }


  // 为简单起见, 假设每个chunk的加载时间都一样, 等待所有chunk都加载完
  public void loadChunks(Pair<Long, Integer> chunk1, Pair<Long, Integer> chunk2)
      throws IOException {

    List<Future> futures = new ArrayList<>();
//    for (Map.Entry<Long, Integer> entry : chunks.entrySet()) {
//      futures.add(mExecutorService.submit(new RemoteReader(entry.getKey(), entry.getValue())));
//    }
    if (chunk1 != null){
      futures.add(mExecutorService.submit(new RemoteReader(mRemoteBWPerLoadingTask, chunk1.getKey(), chunk1.getValue())));
      System.out.println("load chunk: " + chunk1.getKey() + "-" + chunk1.getValue());
      mLogWriter.write( "load chunk: " + chunk1.getKey() + "-" + chunk1.getValue() + "\n");
    }
    if (chunk2 != null){
      futures.add(mExecutorService.submit(new RemoteReader(mRemoteBWPerLoadingTask, chunk2.getKey(), chunk2.getValue())));
      System.out.println("load chunk: " + chunk2.getKey() + "-" + chunk2.getValue());
      mLogWriter.write("load chunk: " + chunk2.getKey() + "-" + chunk2.getValue() + "\n");
    }

    for (Long dataset : mResourceManager.getAllDatasets()) {
      int bw = 0;
      if (chunk1 != null && dataset.equals(chunk1.getKey())) {
        bw += mRemoteBWPerLoadingTask;
      }
      if (chunk2 != null && dataset.equals(chunk2.getKey())){
        bw += mRemoteBWPerLoadingTask;
      }
      mBwUsageWriter.write(dataset + ":" + bw + ",");
    }
    mBwUsageWriter.write("\n");

    mCacheUsageWriter.flush();
    mBwUsageWriter.flush();

    while (true){
      boolean flag = false;
      for (Future future : futures) {
        if (!future.isDone()){
          flag = true;
          break;
        }
      }
      if (flag){
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      else {
        break;
      }
    }
  }

  public void shutdown(){
    mExecutorService.shutdown();
  }

  public void printCacheUsage(Map<Long, List<Integer>> allCachedChunks) throws IOException {
    for (Map.Entry<Long, List<Integer>> entry : allCachedChunks.entrySet()) {
      StringBuilder sb = new StringBuilder();
      sb.append(entry.getKey()).append(": ");
      for (Integer chunk : entry.getValue()) {
        sb.append(chunk).append(", ");
      }
      System.out.println(sb);
      mLogWriter.write(sb.toString() + "\n");
      mCacheUsageWriter.write(entry.getKey() + ":" + entry.getValue().size() + ",");
    }
    mCacheUsageWriter.write("\n");
  }


  class RemoteReader implements Runnable{

    private long mRemoteBW;
    private TokenBucket mTokenBucket;
    private long mDatasetId;
    private int mChunkId;
//    private long mItemCount; // item count
//    private int mItemSize;

    public RemoteReader(long remoteBW, long datasetId, int chunkId) {
      mRemoteBW = remoteBW * 1000; // KB
      mTokenBucket = TokenBuckets.builder().withCapacity(mRemoteBW).withFixedIntervalRefillStrategy(mRemoteBW / 1000, 1, TimeUnit.MILLISECONDS).build();
//      mItemSize = 100; // KB
      mDatasetId = datasetId;
      mChunkId = chunkId;
//      mItemCount = itemCount;
    }

    @Override
    public void run() {
      // TODO: 2021/3/1 偶尔出现chunk加载不了, 导致相应的workload卡死的情况... 
      // 1.shuffle loading order
      List<Integer> itemIds = new ArrayList<>();
      int itemCount = mResourceManager.getChunkItemCount(mDatasetId, mChunkId);
      for (int i = 0; i < itemCount; i++) {
        itemIds.add(i);
      }
      Collections.shuffle(itemIds);

      // 2.load
      for (Integer itemId : itemIds) {
        int itemSize = mResourceManager.getChunkItemSize(mDatasetId, mChunkId, itemId);
        mTokenBucket.consume(itemSize);
        mResourceManager.putItem(mDatasetId, mChunkId, itemId);
      }

      // 3.done
      mResourceManager.notifyDoneLoadingChunk(mDatasetId, mChunkId);
    }
  }
}
