package ruc.swh;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

// token bucket
public class Storage {


  private Map<Long, Dataset> mDatasetMap;

  public Storage() {
    mDatasetMap = new HashMap<>();
  }

  public void addDataset(Dataset dataset){
    mDatasetMap.put(dataset.getId(), dataset);
  }

  public Set<Long> getAllDatasets(){
    return mDatasetMap.keySet();
  }


  public int getChunkItemCount(long datasetId, long chunkId){
    return mDatasetMap.get(datasetId).getChunkItemCount();
  }

  public int getChunkNum(long datasetId){
    return mDatasetMap.get(datasetId).getChunkNum();
  }


  public int getChunkItemSize(long datasetId, int chunkId, int itemId){
    return mDatasetMap.get(datasetId).getChunkItemSize(itemId);
  }
}
