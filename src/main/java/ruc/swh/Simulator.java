package ruc.swh;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Simulator {

  private final int LOADING_THREAD_POOL_SIZE = 2;


  private Map<Workload, Future> mWorkloads = new ConcurrentHashMap<>();
  private ResourceManager mResourceManager;
  private RemoteDataLoader mRemoteDataLoader;
//  private ExecutorService mRemoteDataLoadingThread = Executors.newSingleThreadExecutor();
  private ExecutorService mWorkloadExecutors = Executors.newCachedThreadPool();
  private BufferedWriter mLogWriter;
  private BufferedWriter mCacheUsageWriter;
  private BufferedWriter mBwUsageWriter;
  private BufferedWriter mChunkReadTimeWriter;
  private long mRemoteBWPerLoadingTask;
  private long mCacheCapacity;
  private String mLogDir;

  public Simulator(){
    mResourceManager = new ResourceManager();
    mRemoteBWPerLoadingTask = 1000;
    mCacheCapacity = 300;
    try {
      mLogWriter = new BufferedWriter(new FileWriter("src/main/resources/logs/" + mCacheCapacity + "GB-" + mRemoteBWPerLoadingTask + "MBps-running-info.log"));
      mCacheUsageWriter = new BufferedWriter(new FileWriter("src/main/resources/logs/" + mCacheCapacity + "GB-" + mRemoteBWPerLoadingTask + "MBps-cache-usage.log"));
      mBwUsageWriter = new BufferedWriter(new FileWriter("src/main/resources/logs/" + mCacheCapacity + "GB-" + mRemoteBWPerLoadingTask + "MBps-bw-usage.log"));

    } catch (IOException e) {
      e.printStackTrace();
    }
    mRemoteDataLoader = new RemoteDataLoader(LOADING_THREAD_POOL_SIZE, mResourceManager, mRemoteBWPerLoadingTask, mLogWriter, mCacheUsageWriter, mBwUsageWriter);
  }

  public Simulator(long mRemoteBWPerLoadingTask, long mCacheCapacity, String mLogDir) {
    this.mRemoteBWPerLoadingTask = mRemoteBWPerLoadingTask;
    this.mCacheCapacity = mCacheCapacity;
    this.mLogDir = mLogDir;
    mResourceManager = new ResourceManager((int)mCacheCapacity); // GB
    try {
      mLogWriter = new BufferedWriter(new FileWriter(mLogDir + "/" + mCacheCapacity + "GB-" + mRemoteBWPerLoadingTask + "MBps-running-info.log"));
      mCacheUsageWriter = new BufferedWriter(new FileWriter(mLogDir + "/" + mCacheCapacity + "GB-" + mRemoteBWPerLoadingTask + "MBps-cache-usage.log"));
      mBwUsageWriter = new BufferedWriter(new FileWriter(mLogDir + "/" + mCacheCapacity + "GB-" + mRemoteBWPerLoadingTask + "MBps-bw-usage.log"));
      mChunkReadTimeWriter = new BufferedWriter(new FileWriter(mLogDir + "/" + mCacheCapacity + "GB-" + mRemoteBWPerLoadingTask + "MBps-chunk-reading-time-info.log"));
    } catch (IOException e) {
      e.printStackTrace();
    }
    mRemoteDataLoader = new RemoteDataLoader(LOADING_THREAD_POOL_SIZE, mResourceManager, mRemoteBWPerLoadingTask, mLogWriter, mCacheUsageWriter, mBwUsageWriter);

  }

  public static void main(String[] args) throws InterruptedException, IOException {
    Simulator simulator = new Simulator(900, 300, "src/main/resources/logs");

    // 1.define dataset
    // total: 200GB, chunkSize: 10GB, itemSize: 100KB
    Dataset dataset1 = new Dataset(1, 20, 100000, 100);
    // total: 150GB, chunkSize: 10GB, itemSize: 100KB
    Dataset dataset2 = new Dataset(2, 15, 100000, 100);
    // total: 120GB, chunkSize: 10GB, itemSize: 100KB
    Dataset dataset3 = new Dataset(3, 12, 100000, 100);
    simulator.mResourceManager.addDataset(dataset1);
    simulator.mResourceManager.addDataset(dataset2);
    simulator.mResourceManager.addDataset(dataset3);

    // 2.define workload
    Workload workload1 = new Workload(1, 1, dataset1.getChunkNum(), 10, 100, 1000, 10000, simulator.mResourceManager);
    Workload workload2 = new Workload(2, 2, dataset2.getChunkNum(), 10, 100, 1000, 5000, simulator.mResourceManager);
    Workload workload3 = new Workload(3, 3, dataset3.getChunkNum(), 10, 100, 1000, 4000, simulator.mResourceManager);
    workload1.setLogWriter(simulator.mLogWriter);
    workload2.setLogWriter(simulator.mLogWriter);
    workload3.setLogWriter(simulator.mLogWriter);
    workload1.setChunkReadTimeWriter(simulator.mChunkReadTimeWriter);
    workload2.setChunkReadTimeWriter(simulator.mChunkReadTimeWriter);
    workload3.setChunkReadTimeWriter(simulator.mChunkReadTimeWriter);
    simulator.mWorkloads.put(workload1,simulator.mWorkloadExecutors.submit(workload1));
    simulator.mWorkloads.put(workload2,simulator.mWorkloadExecutors.submit(workload2));
    simulator.mWorkloads.put(workload3,simulator.mWorkloadExecutors.submit(workload3));


    // 3.load data
    RemoteDataLoadingRunnable remoteDataLoadingRunnable = new RemoteDataLoadingRunnable(simulator);
    new Thread(remoteDataLoadingRunnable).start();
//    simulator.mRemoteDataLoadingThread.execute();

    // 4.exit after finishing all workloads
    while (true){
      Iterator<Workload> iterator = simulator.mWorkloads.keySet().iterator();
      boolean terminate = false;
      while (iterator.hasNext()){
        Workload workload = iterator.next();
        if (simulator.mWorkloads.get(workload).isDone()){
          System.out.println(workload + "-" + workload.getId() + " is done");
          iterator.remove();
          simulator.mWorkloads.remove(workload);
          boolean datasetStillBeUsed = false;
          for (Workload otherWorkload : simulator.mWorkloads.keySet()) {
            if (otherWorkload.getDatasetId() == workload.getDatasetId()) {
              datasetStillBeUsed = true;
              break;
            }
          }
          if (!datasetStillBeUsed){
            simulator.mResourceManager.deleteDataset(workload.getDatasetId());
          }
        }
        if (simulator.mWorkloads.isEmpty()){
          terminate = true;
          break;
        }
      }
      if (terminate){
        break;
      }
      Thread.sleep(1000);
    }

    remoteDataLoadingRunnable.setTerminate(true);
//    simulator.mRemoteDataLoadingThread.shutdown();
    simulator.mRemoteDataLoader.shutdown();
    simulator.mWorkloadExecutors.shutdown();
    System.out.println("All workloads are finished!");
    simulator.mLogWriter.write("All workloads are finished!\n");
    simulator.mLogWriter.close();
    simulator.mCacheUsageWriter.close();
    simulator.mBwUsageWriter.close();
    simulator.mChunkReadTimeWriter.close();
  }

  static class RemoteDataLoadingRunnable implements Runnable{
    boolean mTerminate;
    Simulator mSimulator;

    public RemoteDataLoadingRunnable(Simulator simulator) {
      mSimulator = simulator;
    }

    public void setTerminate(boolean terminate) {
      mTerminate = terminate;
    }

    @Override
    public void run() {
      List<WorkloadDataReadingInfo> workloadDataReadingInfos = new ArrayList<>();
      while (!mTerminate) {
        workloadDataReadingInfos.clear();
        for (Workload workload : mSimulator.mWorkloads.keySet()) {
          workloadDataReadingInfos.add(workload.getCurrentDataReadingInfo());
        }
        try {
          mSimulator.mRemoteDataLoader.loadChunks(workloadDataReadingInfos);
        } catch (IOException | InterruptedException e) {
          e.printStackTrace();
          // 在shutdown loading thread之后, 设置了interrupt标记, loadChunks中的sleep会强制抛出interruptedException,
          // 并清除了interrupt标记, 导致thread继续运行,
          // 因此这里可以强制重新设置interrupt标记, 但这种不太直观
          // 可以显式的传递terminate信号


          // Thread.interrupt()只是设置中断标志, 并不会去立即停止当前线程, 需要线程自己去判断这个标志, 并作相应的处理
          //          Thread.currentThread().interrupt();
          //          mTerminate = true;
        }
      }
    }
  }
}


