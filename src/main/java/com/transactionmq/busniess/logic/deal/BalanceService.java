package com.transactionmq.busniess.logic.deal;

import org.springframework.transaction.annotation.Transactional;

public class BalanceService {

    /**
     * 此处模拟对账号的钱需要做加的操作，如果失败，就要抛出异常，为了是mq知道消费异常
     */
    @Transactional
    public void updateAmount(String userid,double money,String balance_mode)throws  Exception{

        System.out.println("userid:"+userid+",money:"+money+",balance_mode:"+balance_mode);
    }

}
