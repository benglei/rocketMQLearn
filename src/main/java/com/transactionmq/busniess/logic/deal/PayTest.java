package com.transactionmq.busniess.logic.deal;

import com.alibaba.rocketmq.client.producer.LocalTransactionExecuter;
import com.alibaba.rocketmq.common.message.Message;
import com.transactionmq.producer.MQProducer;
import net.sf.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class PayTest {
    @Autowired
    private MQProducer mqProducer;
    @Autowired
    private LocalTransactionExecuter transactionExecuterImpl;
    public void pay(){
        try {
            System.out.println("this.mqProducer:"+this.mqProducer);
            System.out.println("this.transactionExecuterImpl:"+this.transactionExecuterImpl);
            //构造消息数据
            Message message = new Message();
            //主题
            message.setTopic("pay");
            //字标签
            message.setTopic("tag");
            //key
            String uuid = UUID.randomUUID().toString();
            System.out.println("key:"+uuid);
            message.setKeys(uuid);
            JSONObject body = new JSONObject();
            body.put("userid","z3");
            body.put("money","1000");
            body.put("pay_mode","OUT");
            body.put("balance_mode","IN");

            message.setBody(body.toString().getBytes());
            //添加参数
            Map<String,Object> transactionMapArgs = new HashMap<String, Object>();

            this.mqProducer.sendTransactionMessage(message,this.transactionExecuterImpl,transactionMapArgs);

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("===PayTest.pay=====e:"+e);
        }
    }

}
