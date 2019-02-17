package com.transactionmq.busniess.logic.deal;

import org.springframework.transaction.annotation.Transactional;

/**
 * 此处只是模拟业务处理
 */
public class PayService {
    //此处只是模拟处理业务，实际情况会调用银行接口，做钱的加减，如果失败，就要抛出异常，为了是mq知道消费异常
    @Transactional
    public void updateAmount(String userid,double money,String pay_mode)throws  Exception{

        System.out.println("userid:"+userid+",money:"+money+",pay_mode:"+pay_mode);
    }


}
