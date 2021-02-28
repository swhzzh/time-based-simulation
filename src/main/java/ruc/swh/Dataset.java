package ruc.swh;

public class Dataset {

    private String mName;
    private long mId;

    private long mTotalSize;
    private long mChunkSize;
    private int mChunkNum;
    private int mChunkItemCount;
    private int mChunkItemSize;

    public Dataset(long id, int chunkNum, int chunkItemCount, int chunkItemSize) {
        mId = id;
        mChunkNum = chunkNum;
        mChunkItemCount = chunkItemCount;
        mChunkItemSize = chunkItemSize;
    }

    public long getId() {
        return mId;
    }

    public int getChunkItemCount(){
        return this.mChunkItemCount;
    }

    public int getChunkNum(){
        return this.mChunkNum;
    }

    public int getChunkItemSize(int itemId){
        return this.mChunkItemSize;
    }
}
