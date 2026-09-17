package com.banking.transaction_service.service;

import java.util.List;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;

import com.banking.transaction_service.dto.TransactionResponse;
import com.banking.transaction_service.dto.TransferRequest;

public class TransactionService {


    public TransactionResponse transfer(TransferRequest request){
        return  null;
    }

    public TransactionResponse getTransaction(String transactionId){
        return  null;
    }

     public List getTransactionHistory(String accountNumber){
        return  null;
    }
    public TransactionResponse verifyOTP(String transactionId, String otp){
        return  null;
    }
}
