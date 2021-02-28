package ruc.swh;

public class WorkloadRunningTimeInfo {

  long mWorkloadId;
  long mDatasetId;
  int mChunkNum;
  int mLatestChunk;
  long mLatestChunkTime;
  long mMinChunkConsumeTime;

  public WorkloadRunningTimeInfo(long workloadId, long datasetId, int chunkNum, int latestChunk,
      long latestChunkTime, long minChunkConsumeTime) {
    mWorkloadId = workloadId;
    mDatasetId = datasetId;
    mChunkNum = chunkNum;
    mLatestChunk = latestChunk;
    mLatestChunkTime = latestChunkTime;
    mMinChunkConsumeTime = minChunkConsumeTime;
  }

  public void updateLatestChunk(){
    mLatestChunk = (mLatestChunk + 1) % mChunkNum;
    mLatestChunkTime += mMinChunkConsumeTime;
  }

  public long getWorkloadId() {
    return mWorkloadId;
  }

  public void setWorkloadId(long workloadId) {
    mWorkloadId = workloadId;
  }

  public long getDatasetId() {
    return mDatasetId;
  }

  public void setDatasetId(long datasetId) {
    mDatasetId = datasetId;
  }

  public long getChunkNum() {
    return mChunkNum;
  }

  public void setChunkNum(int chunkNum) {
    mChunkNum = chunkNum;
  }

  public int getLatestChunk() {
    return mLatestChunk;
  }

  public void setLatestChunk(int latestChunk) {
    mLatestChunk = latestChunk;
  }

  public long getLatestChunkTime() {
    return mLatestChunkTime;
  }

  public void setLatestChunkTime(long latestChunkTime) {
    mLatestChunkTime = latestChunkTime;
  }

  public long getMinChunkConsumeTime() {
    return mMinChunkConsumeTime;
  }

  public void setMinChunkConsumeTime(long minChunkConsumeTime) {
    mMinChunkConsumeTime = minChunkConsumeTime;
  }
}
