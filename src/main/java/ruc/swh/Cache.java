package ruc.swh;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Cache {

    private long mSpaceSize;
    private int mTotalChunkNum;
    private AtomicInteger mCachedChunkNum = new AtomicInteger(0);

    // every workload try to consume the minimum-labelled and cached chunk next.
    // prefetch: current epoch, noncached, nonused, minimum-labelled chunk
    // evict:
    // 有了share, 其实访问序列也不是完全已知的了
    // 比如新加入的share的job到底是从最小的chunk开始还是尽量去使用现有的job的刚加载的chunk, 前者是比较自然的, 但如果cache紧缺, 那刚加载完使用完的chunk是很容易被evict的, 先使用它也就免了在此加载它.

    private Map<Long, List<Integer>[]> mDatasetIdToChunkArrayMap;

    public Cache(int totalChunkNum) {
        mTotalChunkNum = totalChunkNum;
        mDatasetIdToChunkArrayMap = new HashMap<>();
    }

    // 怎么存cache 顺序
    public void deleteChunk(long datasetId, int chunkId){
        mDatasetIdToChunkArrayMap.get(datasetId)[chunkId].clear();
        mCachedChunkNum.decrementAndGet();
    }

    public void deleteDataset(long datasetId){
        for (int i = 0; i < mDatasetIdToChunkArrayMap.get(datasetId).length; i++) {
            deleteChunk(datasetId, i);
        }
    }

    public void put(long datasetId, int chunkId, int itemId){
        mDatasetIdToChunkArrayMap.get(datasetId)[chunkId].add(itemId);
    }

//    public boolean isCached(long datasetId, int chunkId){
//        return mDatasetIdToChunkArrayMap.get(datasetId)[chunkId].isEmpty();
//    }

    public int getCachedChunkItemCount(long datasetId, int chunkId){
        return mDatasetIdToChunkArrayMap.get(datasetId)[chunkId].size();
    }

    public Map<Long, Integer[]> getDatasetCachingInfo(){
        Map<Long, Integer[]> result = new HashMap<>();
        for (Map.Entry<Long, List<Integer>[]> entry : mDatasetIdToChunkArrayMap.entrySet()) {
            result.put(entry.getKey(), (Integer[]) Arrays.stream(entry.getValue()).map(List::size).toArray());
        }
        return result;
    }

    public Set<Long> getAllCachingDatasets(){
        return mDatasetIdToChunkArrayMap.keySet();
    }

    public void doneLoadingChunk(long datasetId, int chunkId){
        mCachedChunkNum.incrementAndGet();
    }

    public boolean isFull(){
        return mTotalChunkNum == mCachedChunkNum.get();
    }

    public int getCacheableChunkNum(){
        return mTotalChunkNum - mCachedChunkNum.get();
    }

    public void addDataset(long datasetId, int chunkNum){
        List<Integer>[] chunks = new List[chunkNum];
        for (int i = 0; i < chunkNum; i++) {
            chunks[i] = new ArrayList<>();
        }
        mDatasetIdToChunkArrayMap.put(datasetId, chunks);

    }

    // shuffle -> load  读取未加载完的chunk
    // shuffle -> read  针对share, 其实multi-job才需要顺序不一样, 其余的可能影响不大, 但还是都shuffle后再读把
}
