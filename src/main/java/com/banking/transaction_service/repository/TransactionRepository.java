package com.banking.transaction_service.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import com.banking.transaction_service.entity.Transaction;

public interface TransactionRepository extends JpaRepository<Transaction, String>{

    List<Transaction> findBySenderAccountNumberOrderByCreatedAtDesc(String accountNumber);
    
}
