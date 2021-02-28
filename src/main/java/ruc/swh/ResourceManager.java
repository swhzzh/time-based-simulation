package ruc.swh;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResourceManager {

  private Cache mCache;
  private Storage mStorage;
  private Map<String, ChunkReadingInfo> mWorkloadToChunkReadOrderMap;


  public ResourceManager(){
    // 300GB cache
    mCache = new Cache(30);
    mStorage = new Storage();
    mWorkloadToChunkReadOrderMap = new HashMap<>();
  }



  public void notifyUsingChunk(long workloadId, long datasetId, int chunkId){
    List<Integer> itemIds = new ArrayList<>();
    int itemCount = mStorage.getChunkItemCount(datasetId, chunkId);
    for (int i = 0; i < itemCount; i++) {
      itemIds.add(i);
    }
    if (isChunkCached(datasetId, chunkId)){
      // 1.if chunk is cached
      // shuffle loading order
      Collections.shuffle(itemIds);
      mWorkloadToChunkReadOrderMap.put(concatNumbers(workloadId, datasetId, chunkId), new ChunkReadingInfo(itemIds, 0));
    }

    // 2.else
    else {
      mWorkloadToChunkReadOrderMap.put(concatNumbers(workloadId, datasetId, chunkId), new ChunkReadingInfo(itemIds, 0));
    }
  }

  public void notifyDoneUsingChunk(long workloadId, long datasetId, int chunkId){
    mWorkloadToChunkReadOrderMap.remove(concatNumbers(workloadId, datasetId, chunkId));
  }

  public int getStartChunk(long datasetId){
//    int chunkNum = mStorage.getChunkNum(datasetId);
//    for (int i = 0; i < chunkNum; i++) {
//      if (isChunkCached(datasetId, i)){
//        return i;
//      }
//    }
    return 0;
  }

  public boolean getBatchData(long workloadId, long datasetId, int chunkId, int batchSize){
    // lastReadingIndex, maxIndex

    // full cache : shuffle order
    // caching: normal order
    // no caching: waiting?
    ChunkReadingInfo info = mWorkloadToChunkReadOrderMap.get(concatNumbers(workloadId, datasetId, chunkId));
    int lastIndex = info.getLastIndex();

    // 如果数据未缓存, 就通知workload, 让它自己等会再请求数据, 阻塞workload而不是服务器
    // 实际上服务器应该多线程, 阻塞某个线程?..
    int totalCachedItems = mCache.getCachedChunkItemCount(datasetId, chunkId);
//    System.out.println(workloadId + "-" + datasetId + "-" + chunkId + "'s cached items: " + totalCachedItems);

    if (lastIndex + batchSize <= totalCachedItems){
      mWorkloadToChunkReadOrderMap.get(concatNumbers(workloadId, datasetId, chunkId)).setLastIndex(lastIndex + batchSize);
      return true;
    }
    return false;
  }

  public void putItem(long datasetId, int chunkId, int itemId){
    mCache.put(datasetId, chunkId, itemId);
  }

  public void notifyDoneLoadingChunk(long datasetId, int chunkId){
    mCache.doneLoadingChunk(datasetId, chunkId);
  }

  public void deleteChunk(long datasetId, int chunkId){
    mCache.deleteChunk(datasetId, chunkId);
  }
  public void deleteDataset(long datasetId){
    mCache.deleteDataset(datasetId);
  }

  public boolean isChunkCached(long datasetId, int chunkId){
    return mCache.getCachedChunkItemCount(datasetId, chunkId) == mStorage.getChunkItemCount(datasetId, chunkId);
  }

  public int getChunkItemCount(long datasetId, int chunkId){
    return mStorage.getChunkItemCount(datasetId, chunkId);
  }

  public int getChunkItemSize(long datasetId, int chunkId, int itemId){
    return mStorage.getChunkItemSize(datasetId, chunkId, itemId);
  }

  public int getChunkNum(long datasetId){
    return mStorage.getChunkNum(datasetId);
  }

  public int getCacheableChunkNum(){
    return mCache.getCacheableChunkNum();
  }

  public Map<Long, List<Integer>> getAllCachedChunks(){
//    Map<Long, Integer[]> datasetCachingInfo = mCache.getDatasetCachingInfo();
    Map<Long, List<Integer>> result = new HashMap<>();
//    for (Map.Entry<Long, Integer[]> entry : datasetCachingInfo.entrySet()) {
//      long datasetId = entry.getKey();
//      for (Integer cachedChunkItemCount : entry.getValue()) {
//
//      }
//    }
    for (Long datasetId : mCache.getAllCachingDatasets()) {
      List<Integer> chunks = new ArrayList<>();
      result.put(datasetId, chunks);
      int chunkNum = mStorage.getChunkNum(datasetId);
      for (int i = 0; i < chunkNum; i++) {
        if (isChunkCached(datasetId, i)){
          chunks.add(i);
        }
      }
    }

    return result;
  }

  public void addDataset(Dataset dataset){
    mCache.addDataset(dataset.getId(), dataset.getChunkNum());
    mStorage.addDataset(dataset);
  }

  public List<Integer> getCachedChunks(long datasetId){
    int chunkNum = getChunkNum(datasetId);
    List<Integer> cachedChunks = new ArrayList<>();
    for (int i = 0; i < chunkNum; i++) {
      if (isChunkCached(datasetId,i)){
        cachedChunks.add(i);
      }
    }
    return cachedChunks;
  }

  public List<Integer> getUnCachedChunks(long datasetId){
    int chunkNum = getChunkNum(datasetId);
    List<Integer> uncachedChunks = new ArrayList<>();
    for (int i = 0; i < chunkNum; i++) {
      if (!isChunkCached(datasetId,i)){
        uncachedChunks.add(i);
      }
    }
    return uncachedChunks;
  }

  private String concatNumbers(long... numbers){
    if (numbers.length == 0){
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (long number : numbers) {
      sb.append(number).append('-');
    }
    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();
  }

  class ChunkReadingInfo{
    List<Integer> orders;
    int lastIndex;

    public ChunkReadingInfo(List<Integer> orders, int lastIndex) {
      this.orders = orders;
      this.lastIndex = lastIndex;
    }

    public List<Integer> getOrders() {
      return orders;
    }

    public void setOrders(List<Integer> orders) {
      this.orders = orders;
    }

    public int getLastIndex() {
      return lastIndex;
    }

    public void setLastIndex(int lastIndex) {
      this.lastIndex = lastIndex;
    }
  }
}
