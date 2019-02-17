package com.transactionmq.producer;

import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.producer.*;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

import java.util.Map;
@Component
public class MQProducer {
    private final String GROUP_NAME="transaction-pa";
    private final String NAMESRV_ADDR="192.168.1.111:9876;192.168.1.112:9876;192.168.1.113:9876;192.168.1.114:9876";
    private TransactionMQProducer producer;

    public MQProducer(){
        this.producer = new TransactionMQProducer(GROUP_NAME);//设置生产组名
        this.producer.setNamesrvAddr(NAMESRV_ADDR);//设置nameserver服务地址
        this.producer.setCheckRequestHoldMax(2000);//设置队列数
        this.producer.setCheckThreadPoolMaxSize(20);//事务回看最大并发数
        this.producer.setCheckThreadPoolMinSize(5);//事务回看最小并发数
        //RocketMQ服务器回调producer,检查本地事务支付成功还是失败(该功能在开源版本中，被阿里禁用了)
        this.producer.setTransactionCheckListener(new TransactionCheckListener() {
            public LocalTransactionState checkLocalTransactionState(MessageExt messageExt) {
                System.out.println("state --"+messageExt.toString());
                //return LocalTransactionState.ROLLBACK_MESSAGE;//告诉mq是回滚的状态，mq会将之前消息清除掉
                //return LocalTransactionState.UNKNOW;//告诉mq是未知状态
                return LocalTransactionState.COMMIT_MESSAGE;//告诉mq是提交(其实是可以将mq放到相应的队列中)状态
            }
        });
        try {
            this.producer.start();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("===MQProducer.MQProducer=="+e.toString());
        }
    }

    public QueryResult queryMessage(String topic,String key,int maxNum,long begin,long end) throws Exception{
        return this.producer.queryMessage(topic,key,maxNum,begin,end);
    }

    public void sendTransactionMessage(Message message, LocalTransactionExecuter localTransactionExecuter, Map<String,Object> transactionMapArgs) throws Exception{
        TransactionSendResult transactionSendResult = this.producer.sendMessageInTransaction(message,localTransactionExecuter,transactionMapArgs);

        System.out.println("send返回内容:"+transactionSendResult.toString());
    }

    public void shutdown(){
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                producer.shutdown();
            }
        }));

        System.exit(0);
    }

}
