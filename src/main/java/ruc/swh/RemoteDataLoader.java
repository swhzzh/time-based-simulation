package ruc.swh;


import javafx.util.Pair;
import org.isomorphism.util.*;

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
  private FileWriter mFileWriter;
  private boolean mNoChunkToLoadLastTime;

  public RemoteDataLoader(int threadPoolSize, ResourceManager resourceManager, FileWriter fileWriter) {
    mExecutorService = Executors.newFixedThreadPool(threadPoolSize);
    mRunningTaskNum = new AtomicInteger(threadPoolSize);
    mResourceManager = resourceManager;
    mFileWriter = fileWriter;
    mNoChunkToLoadLastTime = false;
  }
  
  public void loadChunks(List<WorkloadDataReadingInfo> workloadDataReadingInfos)
      throws IOException, InterruptedException {
    Map<Long, List<Integer>> allCachedChunks = mResourceManager.getAllCachedChunks();

//    for (Map.Entry<Long, List<Integer>> entry : allCachedChunks.entrySet()) {
//      StringBuilder sb = new StringBuilder();
//      sb.append(entry.getKey()).append(": ");
//      for (Integer chunk : entry.getValue()) {
//        sb.append(chunk).append(", ");
//      }
//      System.out.println(sb);
//      mFileWriter.write(sb.toString() + "\n");
//    }

    Map<Pair<Long, Integer>, Long> chunksToLoadToTimeInfoMap = new HashMap<>();
    Map<Pair<Long, Integer>, Long> chunksToEvictToTimeInfoMap = new HashMap<>();

    for (WorkloadDataReadingInfo info : workloadDataReadingInfos) {
      long datasetId = info.getDatasetId();
      List<Integer> readChunks = info.getReadChunks();
      List<Integer> unReadChunks = info.getUnReadChunks();

      // 1.get time for chunks to load(unread & uncached)
      // 这里只从当前epoch中未读的chunks中选择要load的chunk, 但也可以为下一个epoch提前加载已读但被evict(目前uncached)的chunk
      // TODO: 2021/2/28 在cache空间有剩余的情况下, 加载上述所说的chunk 
//      List<Integer> cachedChunks = allCachedChunks.get(datasetId);
      int unReadAndCachedChunkNum = 0;
      for (Integer unReadChunk : unReadChunks) {
        if (mResourceManager.isChunkCached(datasetId, unReadChunk)){
          unReadAndCachedChunkNum++;
        }
      }
      int temp = 0;
      for (Integer unReadChunk : unReadChunks) {
        if (!mResourceManager.isChunkCached(datasetId, unReadChunk)){
          long time = info.getLatestTime() + (unReadAndCachedChunkNum + temp) * info.getMinChunkConsumeTime();
          // 说明第一个chunk还在等, 那第二个chunk的时间最早也是从现在算起, 而不应该用第一个chunk的时间,
          // 第一个chunk应该保留原有的时间, 从而实现等得越久优先级越高
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

      // 2.get time for chunks to evict(read & cached)
      int readAndCachedChunkNum = 0;
      for (Integer readChunk : readChunks) {
        if (mResourceManager.isChunkCached(datasetId, readChunk)){
          readAndCachedChunkNum++;
        }
      }
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
//      Iterator<Integer> descendingIterator = readChunks.descendingIterator();
//      temp = 0;
//      while (descendingIterator.hasNext()){
//        int chunkId = descendingIterator.next();
//        if (mResourceManager.isChunkCached(datasetId, chunkId)){
//          // 这里不应该用latestTime, 而应该计算当前chunk剩余的时间, 否则可能出现当前chunk等了很久, 导致访问过的cache的chunk的时间序列提前的状况.
//          chunksToEvictToTimeInfoMap.put(new Pair<>(datasetId, chunkId),
//              System.currentTimeMillis() + info.getCurrentChunkRemainTime() + (unReadChunks.size() - 1 + readAndCachedChunkNum - 1 - temp) * info.getMinChunkConsumeTime());
//          temp++;
//          if (temp == 2){
//            break;
//          }
//        }
//      }
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
          for (Map.Entry<Long, List<Integer>> entry : allCachedChunks.entrySet()) {
            StringBuilder sb = new StringBuilder();
            sb.append(entry.getKey()).append(": ");
            for (Integer chunk : entry.getValue()) {
              sb.append(chunk).append(", ");
            }
            System.out.println(sb);
            mFileWriter.write(sb.toString() + "\n");
          }
          mResourceManager.deleteChunk(chunksToEvictList.get(0).getKey().getKey(), chunksToEvictList.get(0).getKey().getValue());
          loadChunks(chunksToLoadList.get(0).getKey(),null);
        }
      }
      else {
        for (Map.Entry<Long, List<Integer>> entry : allCachedChunks.entrySet()) {
          StringBuilder sb = new StringBuilder();
          sb.append(entry.getKey()).append(": ");
          for (Integer chunk : entry.getValue()) {
            sb.append(chunk).append(", ");
          }
          System.out.println(sb);
          mFileWriter.write(sb.toString() + "\n");
        }
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
          for (Map.Entry<Long, List<Integer>> entry : allCachedChunks.entrySet()) {
            StringBuilder sb = new StringBuilder();
            sb.append(entry.getKey()).append(": ");
            for (Integer chunk : entry.getValue()) {
              sb.append(chunk).append(", ");
            }
            System.out.println(sb);
            mFileWriter.write(sb.toString() + "\n");
          }
          mResourceManager.deleteChunk(chunksToEvictList.get(0).getKey().getKey(), chunksToEvictList.get(0).getKey()
              .getValue());
          loadChunks(chunksToLoadList.get(0).getKey(),null);
        }
        else {
          for (Map.Entry<Long, List<Integer>> entry : allCachedChunks.entrySet()) {
            StringBuilder sb = new StringBuilder();
            sb.append(entry.getKey()).append(": ");
            for (Integer chunk : entry.getValue()) {
              sb.append(chunk).append(", ");
            }
            System.out.println(sb);
            mFileWriter.write(sb.toString() + "\n");
          }
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
        for (Map.Entry<Long, List<Integer>> entry : allCachedChunks.entrySet()) {
          StringBuilder sb = new StringBuilder();
          sb.append(entry.getKey()).append(": ");
          for (Integer chunk : entry.getValue()) {
            sb.append(chunk).append(", ");
          }
          System.out.println(sb);
          mFileWriter.write(sb.toString() + "\n");
        }
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
        for (Map.Entry<Long, List<Integer>> entry : allCachedChunks.entrySet()) {
          StringBuilder sb = new StringBuilder();
          sb.append(entry.getKey()).append(": ");
          for (Integer chunk : entry.getValue()) {
            sb.append(chunk).append(", ");
          }
          System.out.println(sb);
          mFileWriter.write(sb.toString() + "\n");
        }
        loadChunks(chunksToLoadList.get(0).getKey(), chunksToLoadList.get(1).getKey());
      }
    }
  }


/*  public void loadChunks(List<WorkloadRunningTimeInfo> workloadRunningTimeInfos){
    Map<Long, List<Integer>> allCachedChunks = mResourceManager.getAllCachedChunks();

    for (Map.Entry<Long, List<Integer>> entry : allCachedChunks.entrySet()) {
      StringBuilder sb = new StringBuilder();
      sb.append(entry.getKey()).append(": ");
      for (Integer chunk : entry.getValue()) {
        sb.append(chunk).append(", ");
      }
      System.out.println(sb);
    }
    // 1.get two chunks to load, and time for cached chunks
    Map<Pair<Long, Integer>, Long> chunkAccessTimeMap = new HashMap<>();
    Map<Pair<Long, Integer>, Long> cachedChunkAccessTimeMap = new HashMap<>();

    for (WorkloadRunningTimeInfo workloadRunningTimeInfo : workloadRunningTimeInfos) {
      // 1.1 get time for cached chunks
      long datasetId = workloadRunningTimeInfo.getDatasetId();
      List<Integer> cachedChunks = allCachedChunks.get(datasetId);
      for (Integer cachedChunk : cachedChunks) {
        Pair<Long, Integer> key = new Pair<>(datasetId, cachedChunk);
        long time;
//        long baseTime = workloadRunningTimeInfo.getLatestChunkTime();
//        if (baseTime < System.currentTimeMillis()){
//          baseTime = System.currentTimeMillis();
//        }
        if (cachedChunk >= workloadRunningTimeInfo.getLatestChunk()){
          time = workloadRunningTimeInfo.getLatestChunkTime() + (cachedChunk - workloadRunningTimeInfo.getLatestChunk()) * workloadRunningTimeInfo.getMinChunkConsumeTime();
        }
        else {
          time = workloadRunningTimeInfo.getLatestChunkTime() +
              (cachedChunk + workloadRunningTimeInfo.getChunkNum() - workloadRunningTimeInfo.getLatestChunk()) * workloadRunningTimeInfo.getMinChunkConsumeTime();
        }
        if (cachedChunkAccessTimeMap.containsKey(key)){
          // share, set the minimum time
          if (time < cachedChunkAccessTimeMap.get(key)){
            cachedChunkAccessTimeMap.put(key, time);
          }
        }
        else {
          cachedChunkAccessTimeMap.put(key, time);
        }
      }

      // 1.2 get two chunks to load
      int temp = 2;
      int chunkId = workloadRunningTimeInfo.getLatestChunk();
//      boolean flag = false;
      while (temp > 0){
        if (!mResourceManager.isChunkCached(datasetId, chunkId)){
          long time;
//          long baseTime = workloadRunningTimeInfo.getLatestChunkTime();
//          if (baseTime < System.currentTimeMillis()){
//            baseTime = System.currentTimeMillis();
//          }
          if (chunkId >= workloadRunningTimeInfo.getLatestChunk()){
            time = workloadRunningTimeInfo.getLatestChunkTime() + (chunkId - workloadRunningTimeInfo.getLatestChunk()) * workloadRunningTimeInfo.getMinChunkConsumeTime();
          }
          else {
            time = workloadRunningTimeInfo.getLatestChunkTime() +
                (chunkId + workloadRunningTimeInfo.getChunkNum() - workloadRunningTimeInfo.getLatestChunk()) * workloadRunningTimeInfo.getMinChunkConsumeTime();
          }
          Pair<Long, Integer> key = new Pair<>(datasetId, chunkId);
          if (chunkAccessTimeMap.containsKey(key)){
            if (time < chunkAccessTimeMap.get(key)){
              chunkAccessTimeMap.put(key, time);
            }
          }
          else {
            chunkAccessTimeMap.put(key, time);
          }
          temp--;
        }
        chunkId = (chunkId + 1) % mResourceManager.getChunkNum(datasetId);
        if (chunkId == workloadRunningTimeInfo.getLatestChunk()){
          break;
        }
      }
    }

    // 2.prefetch and evict
    // 2.1 get two chunks to load
    //自定义比较器
    Comparator<Map.Entry<Pair<Long, Integer>, Long>> ascCmp = Map.Entry.comparingByValue();
    Comparator<Map.Entry<Pair<Long, Integer>, Long>> desCmp = (o1, o2) -> o2.getValue().compareTo(o1.getValue());

    // chunks to load, sort asc, need to find the minimum timestamped chunk
    List<Map.Entry<Pair<Long, Integer>, Long>> chunksAccessTimeEntries = new ArrayList<>(chunkAccessTimeMap.entrySet());
    chunksAccessTimeEntries.sort(ascCmp);

    int cacheableChunkNum = mResourceManager.getCacheableChunkNum();
    if (chunksAccessTimeEntries.size() == 0){

    }
    else if(chunksAccessTimeEntries.size() == 1){

      if (cacheableChunkNum == 0){
        List<Map.Entry<Pair<Long, Integer>, Long>> cachedChunksAccessTimeEntries = new ArrayList<>(cachedChunkAccessTimeMap.entrySet());
        cachedChunksAccessTimeEntries.sort(desCmp);
        if (cachedChunksAccessTimeEntries.get(0).getValue() <= chunksAccessTimeEntries.get(0).getValue()){

        }
        else {
          mResourceManager.deleteChunk(cachedChunksAccessTimeEntries.get(0).getKey().getKey(), cachedChunksAccessTimeEntries.get(0).getKey().getValue());
          loadChunks(chunksAccessTimeEntries.get(0).getKey(), null);
        }
      }
      else {
        loadChunks(chunksAccessTimeEntries.get(0).getKey(), null);
      }
    }
    else{
      if (cacheableChunkNum < 2){
        // cached chunks, sort des, need to find the maximum timestamped chunk
        List<Map.Entry<Pair<Long, Integer>, Long>> cachedChunksAccessTimeEntries = new ArrayList<>(cachedChunkAccessTimeMap.entrySet());
        cachedChunksAccessTimeEntries.sort(desCmp);

        if (cacheableChunkNum == 1){ // need to evict one chunk
          // 判断缓存的chunk的访问时间与需要加载的chunk的访问时间之间的关系
          if (cachedChunksAccessTimeEntries.get(0).getValue() <= chunksAccessTimeEntries.get(1).getValue()){
            loadChunks(chunksAccessTimeEntries.get(0).getKey(), null);
          }
          else {
            mResourceManager.deleteChunk(cachedChunksAccessTimeEntries.get(0).getKey().getKey(), cachedChunksAccessTimeEntries.get(0).getKey().getValue());
            loadChunks(chunksAccessTimeEntries.get(0).getKey(), chunksAccessTimeEntries.get(1).getKey());
          }
        }
        else {
          if (cachedChunksAccessTimeEntries.get(0).getValue() <= chunksAccessTimeEntries.get(0).getValue()){

          }
          else if (cachedChunksAccessTimeEntries.get(0).getValue() <= chunksAccessTimeEntries.get(1).getValue()){
            mResourceManager.deleteChunk(cachedChunksAccessTimeEntries.get(0).getKey().getKey(), cachedChunksAccessTimeEntries.get(0).getKey().getValue());
            loadChunks(chunksAccessTimeEntries.get(0).getKey(), null);
          }
          else {
            mResourceManager.deleteChunk(cachedChunksAccessTimeEntries.get(0).getKey().getKey(), cachedChunksAccessTimeEntries.get(0).getKey().getValue());
            mResourceManager.deleteChunk(cachedChunksAccessTimeEntries.get(1).getKey().getKey(), cachedChunksAccessTimeEntries.get(1).getKey().getValue());
            loadChunks(chunksAccessTimeEntries.get(0).getKey(), chunksAccessTimeEntries.get(1).getKey());
          }
        }
      }
      else {
        loadChunks(chunksAccessTimeEntries.get(0).getKey(), chunksAccessTimeEntries.get(1).getKey());
      }
    }

  }*/

  // 为简单起见, 假设每个chunk的加载时间都一样, 等待所有chunk都加载完
  public void loadChunks(Pair<Long, Integer> chunk1, Pair<Long, Integer> chunk2)
      throws IOException {

    List<Future> futures = new ArrayList<>();
//    for (Map.Entry<Long, Integer> entry : chunks.entrySet()) {
//      futures.add(mExecutorService.submit(new RemoteReader(entry.getKey(), entry.getValue())));
//    }
    if (chunk1 != null){
      futures.add(mExecutorService.submit(new RemoteReader(chunk1.getKey(), chunk1.getValue())));
      System.out.println("load chunk: " + chunk1.getKey() + "-" + chunk1.getValue());
      mFileWriter.write( "load chunk: " + chunk1.getKey() + "-" + chunk1.getValue() + "\n");
    }
    if (chunk2 != null){
      futures.add(mExecutorService.submit(new RemoteReader(chunk2.getKey(), chunk2.getValue())));
      System.out.println("load chunk: " + chunk2.getKey() + "-" + chunk2.getValue());
      mFileWriter.write("load chunk: " + chunk2.getKey() + "-" + chunk2.getValue() + "\n");

    }

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

  class RemoteReader implements Runnable{

    private long mRemoteBW;
    private TokenBucket mTokenBucket;
    private long mDatasetId;
    private int mChunkId;
//    private long mItemCount; // item count
//    private int mItemSize;

    public RemoteReader(long datasetId, int chunkId) {
      mRemoteBW = 1000 * 1000; // KB
      mTokenBucket = TokenBuckets.builder().withCapacity(mRemoteBW).withFixedIntervalRefillStrategy(mRemoteBW / 1000, 1, TimeUnit.MILLISECONDS).build();
//      mItemSize = 100; // KB
      mDatasetId = datasetId;
      mChunkId = chunkId;
//      mItemCount = itemCount;
    }

    @Override
    public void run() {
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
