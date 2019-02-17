package com.transactionmq.consumer;

import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.transactionmq.busniess.logic.deal.BalanceService;
import com.transactionmq.busniess.logic.deal.PayService;
import net.sf.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class MQConsumer {
    private final String GROUP_NAME="transaction-balance";
    private final String NAMESRV_ADDR="192.168.1.111:9876;192.168.1.112:9876;192.168.1.113:9876;192.168.1.114:9876";
    private DefaultMQPushConsumer consumer;
    @Autowired
    private BalanceService balanceService;
    public MQConsumer(){
        try {
            this.consumer = new DefaultMQPushConsumer(GROUP_NAME);
            this.consumer.setNamesrvAddr(NAMESRV_ADDR);
            this.consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            this.consumer.subscribe("pay","*");
            this.consumer.registerMessageListener(new Listener());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("===MQProducer.MQConsumer=="+e.toString());
        }
    }

    public QueryResult queryMessage(String topic,String key,int maxNum,long begin,long end) throws Exception{
        long current = System.currentTimeMillis();
        return this.consumer.queryMessage(topic,key,maxNum,begin,end);
    }

    class Listener implements MessageListenerConcurrently{

        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
            MessageExt msg = msgs.get(0);
            try {
                String topic = msg.getTopic();
                //Message Body
                String msgBodyStr = new String(msg.getBody(), "utf-8");
                JSONObject msgBody = JSONObject.fromObject(msgBodyStr);
                String tags = msg.getTags();
                String keys = msg.getKeys();
                System.out.println("balance服务受到消息，keys:"+keys+",body:"+msgBody.toString());
                //userid
                String userid = msgBody.getString("userid");
                double money = msgBody.getDouble("money");
                String balance_mode = msgBody.getString("balance_mode");
                //业务逻辑处理
                balanceService.updateAmount(userid,money,balance_mode);//如果数据库层没抛出异常，说明处理成功了，
            } catch (Exception e) {//下面是对，没有消费成功的处理，若果，没有成功，让rocketMQ稍后重发消息，顶多重发3次，其实
                e.printStackTrace();
                //                //重试次数为3情况
                if(msg.getReconsumeTimes() == 3){
                    //此处应该对没有消费成功的消息进行报错，以便人工或者其他方式进行处理
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;//消费成功
                }
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;//稍后重新消费
            }

            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;//消费成功
        }
    }

}
