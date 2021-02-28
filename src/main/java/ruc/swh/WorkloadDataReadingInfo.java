package ruc.swh;

import java.util.List;
import java.util.TreeSet;

public class WorkloadDataReadingInfo {

  long mWorkloadId;
  long mDatasetId;
  int mChunkNum;
  long mMinChunkConsumeTime;
  long mLatestTime;
  long mCurrentChunkRemainTime;
  List<Integer> mReadChunks;
  List<Integer> mUnReadChunks;

  public WorkloadDataReadingInfo(long workloadId, long datasetId, int chunkNum,
      long minChunkConsumeTime, long latestTime, long currentChunkRemainTime, List<Integer> readChunks, List<Integer> unReadChunks) {
    mWorkloadId = workloadId;
    mDatasetId = datasetId;
    mChunkNum = chunkNum;
    mLatestTime = latestTime;
    mCurrentChunkRemainTime = currentChunkRemainTime;
    mMinChunkConsumeTime = minChunkConsumeTime;
    mReadChunks = readChunks;
    mUnReadChunks = unReadChunks;
  }

  public long getCurrentChunkRemainTime() {
    return mCurrentChunkRemainTime;
  }

  public void setCurrentChunkRemainTime(long currentChunkRemainTime) {
    mCurrentChunkRemainTime = currentChunkRemainTime;
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

  public int getChunkNum() {
    return mChunkNum;
  }

  public void setChunkNum(int chunkNum) {
    mChunkNum = chunkNum;
  }

  public long getMinChunkConsumeTime() {
    return mMinChunkConsumeTime;
  }

  public void setMinChunkConsumeTime(long minChunkConsumeTime) {
    mMinChunkConsumeTime = minChunkConsumeTime;
  }

  public long getLatestTime() {
    return mLatestTime;
  }

  public void setLatestTime(long latestTime) {
    mLatestTime = latestTime;
  }

  public List<Integer> getReadChunks() {
    return mReadChunks;
  }

  public void setReadChunks(List<Integer> readChunks) {
    mReadChunks = readChunks;
  }

  public List<Integer> getUnReadChunks() {
    return mUnReadChunks;
  }

  public void setUnReadChunks(List<Integer> unReadChunks) {
    mUnReadChunks = unReadChunks;
  }
}
