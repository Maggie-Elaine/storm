package com.beneil.storm.prewarm;

import com.beneil.storm.zk.ZooKeeperSession;

public class CachePreWarmThread extends Thread{
    @Override
    public void run() {
        ZooKeeperSession session = ZooKeeperSession.getInstance();
        //获取storm taskid列表
        String taskidList = session.getNodeData("/taskid-list");

        if(taskidList!=null&&"".equals(taskidList)){
            String[] split = taskidList.split(",");
            for (String id : split) {
                String taskpath="/taskid-lock-"+id;
                boolean result = session.acquireFastFailedDistributedLock(taskpath);
                if(!result){//拿锁失败
                    continue;
                }

                String taskStatusLockPath="/taskid-status-lock-"+id;
                session.acquireDistributedLock(taskStatusLockPath);
                String taskidStaus = session.getNodeData("/taskid-status-" + id);
                if("".equals(taskidStaus)){//预热
                    String productList = session.getNodeData("/task-hot-product-list-" + id);
                    //拿到所有
                    String[] list = productList.split(",");
                    //查询数据库 存到缓存
                        //.....
                    //设置预热状态
                    session.setNodeData(taskStatusLockPath,"success");
                }

                session.releaseDistributedLock(taskStatusLockPath);

                session.releaseDistributedLock(taskpath);
            }
        }

    }
}
