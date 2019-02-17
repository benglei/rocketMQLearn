package com.transactionmq.localTransaction;

import com.alibaba.rocketmq.client.producer.LocalTransactionExecuter;
import com.alibaba.rocketmq.client.producer.LocalTransactionState;
import com.alibaba.rocketmq.common.message.Message;
import com.transactionmq.busniess.logic.deal.PayService;
import net.sf.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
@Component
public class TransactionExecuterImpl implements LocalTransactionExecuter {
    @Autowired
    private PayService payService;

    public LocalTransactionState executeLocalTransactionBranch(Message message, Object arg) {
        try {
            //Message Body
            String msgBodyStr = new String(message.getBody(), "utf-8");
            net.sf.json.JSONObject msgBody = JSONObject.fromObject(msgBodyStr);
            //Transaction MapArgs
            Map<String,Object> mapArgs = (Map<String, Object>) arg;
            System.out.println("message body:"+message.getBody());
            System.out.println("message mapArgs:"+mapArgs);
            System.out.println("message tag:"+message.getTags());
            //userid
            String userid = msgBody.getString("userid");
            double money = msgBody.getDouble("money");
            String pay_mode = msgBody.getString("pay_mode");
            //pay
            this.payService.updateAmount(userid,money,pay_mode);
            //上面方法若成功则通知RocketMQ消息变更，该消息变为<确认发送>
            return LocalTransactionState.COMMIT_MESSAGE;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("=====TransactionExecuterImpl.executeLocalTransactionBranch====e:"+e.toString());
            //上面方法成功通知RocketMQ消息变更，该消息变为<回滚>
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }
    }
}
