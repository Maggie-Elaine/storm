package com.beneil.storm.monitor;

import com.beneil.storm.utils.HttpClientUtils;
import com.beneil.storm.zk.ZooKeeperSession;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.trident.util.LRUMap;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.junit.Test;

import java.util.*;

/**
 * 商品访问次数统计
 */
public class ProductCountBolt extends BaseRichBolt {
    private LRUMap<Long,Long> productCountMap=new LRUMap(50);
    private OutputCollector collector;
    private ZooKeeperSession session ;
    private String idList;
    private int taskId;

    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        this.session= ZooKeeperSession.getInstance();
        this.collector=collector;
        this.taskId=context.getThisTaskId();
        initTaskId(taskId);//追加 写taskid  到zk  taskidlist
        new Thread(new ProductCoutThread()).start();//获取topN,写到zk 的指定taskid的里面  topNlist
        new Thread(new HotProductFinfThread()).start();//瞬时热点 预警  添加缓存
    }

    public void execute(Tuple tuple) {
        Long productId = tuple.getLongByField("productId");
        Long count = productCountMap.get(productId);
        if(count==null){
            count=0L;
        }
        count++;
        productCountMap.put(productId,count);
    }

    //--------------------------------------------------------------------------------------------//
    private class ProductCoutThread implements Runnable{//统计前三商品 30秒统计一次
        public void run() {
            ArrayList<Map.Entry<Long, Long>> topList = new ArrayList<Map.Entry<Long, Long>>();
            while (true){
                topList.clear();//每次重新计算topN
                idList=null;

                if(productCountMap.size()==0){
                    Utils.sleep(1000);
                    continue;
                }

                Set<Map.Entry<Long, Long>> set = productCountMap.entrySet();
                if(set!=null){
                    for (Map.Entry<Long, Long> entry : set) {//添加toplist(省空间的话 每进来一个都比较 )
                        topList.add(entry);
                    }
                    Collections.sort(topList,(a,b)->b.getValue().compareTo(a.getValue()));//排序
                    for(int i=0;i<3;i++){
                        Long id = topList.get(i).getKey();
                        if(i==2){
                            idList+=id.toString();//这里要分隔
                        }else {
                            idList+=id.toString()+",";//这里要分隔
                        }
                    }
                    session.setNodeData("/task-hot-product-list-"+taskId,idList);//存到zk中


                }
                Utils.sleep(30000);
            }
        }
    }

    private void initTaskId(int taskId){
        //将所有的bolt的id写到zk的idList中  记得上锁 因为有多个bolt并发操作
        session.acquireDistributedLock("/taskid-list-lock");

        String taskidList = session.getNodeData("/taskid-list");
        if(!"".equals(idList)){
            taskidList+=","+taskId;
        }else{
            taskidList+=taskId;
        }
        session.setNodeData("/taskid-list",idList);
        session.releaseDistributedLock("/taskid-list-lock");
    }

    private class HotProductFinfThread implements Runnable{//瞬时热点 10s统计一次
        @Override
        public void run() {
            ArrayList<Map.Entry<Long, Long>> topList = new ArrayList<Map.Entry<Long, Long>>();//存放所有top商品
            ArrayList<Map.Entry<Long, Long>> tophotList = new ArrayList<Map.Entry<Long, Long>>();//存放topN热点商品
            ArrayList<Long> topnowhotList = new ArrayList<Long>();//存放这一次的热点
            ArrayList<Long> toplasthotList = new ArrayList<Long>();//存放上一次的热点
            while(true){
                //排序,计算后95%的访问次数平均值
                //遍历比较 热点  5倍 平均值 就添加缓存
                topList.clear();//每次重新计算topN
                tophotList.clear();
                long avg=0;

                if(productCountMap.size()==0){
                    Utils.sleep(1000);
                    continue;
                }

                Set<Map.Entry<Long, Long>> set = productCountMap.entrySet();
                if(set!=null){
                    for (Map.Entry<Long, Long> entry : set) {
                        topList.add(entry);
                    }
                    Collections.sort(topList,(a,b)->b.getValue().compareTo(a.getValue()));//排序

                    for(int i = (int) (Math.floor(topList.size()*0.1)); i<topList.size(); i++){//遍历计算平均值
                        avg+=topList.get(i).getValue();
                    }
                    avg=avg/topList.size();

                    for(int i = 0; i<topList.size(); i++){//比较
                        if(topList.get(i).getValue()>(avg<<3)){
                            tophotList.add(topList.get(i));
                            topnowhotList.add(tophotList.get(i).getKey());

                            String distributeURL="http://192.168.240.129/hot?productId="+topList.get(i).getKey();
                            HttpClientUtils.sendGetRequest(distributeURL);//添加nginx缓存/或其他缓存
                        }else {//后面都不满足 直接跳出
                            break;
                        }
                    }
                    //上一次热点跟这一次热点比价
                    if(toplasthotList.size()!=0){
                        //比较
                        for (Long productId : toplasthotList){
                            if(!topnowhotList.contains(productId)){
                                //热点消失
                                //发送请求 取消流量分发nginx流量分发 (取消指定商品负载均衡)
                                //String distributeURL="http://192.168.240.129/hot?productId="+productId;
                                //HttpClientUtils.sendGetRequest(distributeURL);//添加nginx缓存/或其他缓存
                            }
                        }
                    }
                    toplasthotList.clear();//清空
                    for (Map.Entry<Long, Long> entry : tophotList){//保存这次热点
                        toplasthotList.add(entry.getKey());
                    }

                    //这样写比较好看
//                    if(tophotList.size()>0){
//                        //将热点缓存 发送到nginx或者tomcat本地缓存 实现分布式
//                        //将前端nginx的策略 修改成负载均衡
//                        sendCacheMsg(tophotList);//把上面的分发请求 封装到一个方法
//                        sendLoadBalenceMsg();
//                    }

                }
                Utils.sleep(1000);

            }
        }
    }
    //--------------------------------------------------------------------------------------------//

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }


}
