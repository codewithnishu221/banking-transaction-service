package com.banking.transaction_service.service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;

import com.banking.transaction_service.client.AccountServiceClient;
import com.banking.transaction_service.dto.TransactionResponse;
import com.banking.transaction_service.dto.TransferRequest;
import com.banking.transaction_service.entity.Transaction;
import com.banking.transaction_service.entity.TransactionStatus;
import com.banking.transaction_service.entity.TransactionType;
import com.banking.transaction_service.event.TransactionInitiatedEvent;
import com.banking.transaction_service.repository.TransactionRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;



@Service 
@Slf4j
@RequiredArgsConstructor  
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient; 
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static  final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private static  final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private static  final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";
    
    /*
    * SAGA Step-1: Initiate transfer
    * Deducts from sender via feign
    * Saves transaction as Processing
    * Publish event to kafka for fraud check
    * Returns.
    * @Param request
    * @return  */
    public TransactionResponse transfer(TransferRequest request){
        log.info("SAGA START - Transfer: {} -> {} amount: {}", request.getSenderAccountNumber(), request.getReceiverAccountNumber(), request.getAmount());
         // Saga step-1: Deduct from sender
        accountServiceClient.deductBalance(request.getSenderAccountNumber(), request.getAmount());
         Transaction transaction = new Transaction();
         transaction.setAmount(request.getAmount());
         transaction.setSenderAccountNumber(request.getSenderAccountNumber());
         transaction.setReceiverAccountNumber(request.getSenderAccountNumber());
         transaction.setType(TransactionType.TRANSFER);
         transaction.setStatus(TransactionStatus.PROCESSING);
         transaction.setDescription(request.getDescription());
         transaction.setReferenceNumber(UUID.randomUUID().toString());
         Transaction savedTransaction = transactionRepository.save(transaction);
         log.info("Transaction saved as PROCESSING: {}", savedTransaction.getId());


         // SAGA Step-2: Publish for fraud check
         TransactionInitiatedEvent event = new TransactionInitiatedEvent(
            savedTransaction.getId(),
            savedTransaction.getSenderAccountNumber(),
            savedTransaction.getReceiverAccountNumber(),
            savedTransaction.getAmount(),
            savedTransaction.getDescription()
         );
          
         kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC, savedTransaction.getId(), event);
         log.info("SAGA Step 2 - Transaction InitiatedEvent published: {}", savedTransaction.getId());
        return  mapToResponse(savedTransaction);
    }

    private TransactionResponse mapToResponse(Transaction transaction){
     TransactionResponse response = new TransactionResponse();
        response.setId(transaction.getId());
        response.setSenderAccountNumber(transaction.getSenderAccountNumber());
        response.setReceiverAccountNumber(transaction.getReceiverAccountNumber());
        response.setAmount(transaction.getAmount());
        response.setType(transaction.getType());
         response.setStatus(transaction.getStatus());
        response.setDescription(transaction.getDescription());
        response.setFailureReason(transaction.getFailureReason());
        response.setReferenceNumber(transaction.getReferenceNumber());
        response.setCreatedAt(transaction.getCreatedAt());
        response.setCompletedAt(transaction.getCompletedAt());
        return  response;
    }



    public TransactionResponse getTransaction(String transactionId){

        return  mapToResponse(transactionRepository.findById(transactionId).orElseThrow(()-> 
         new RuntimeException("Transaction not found: "+ transactionId)));
    }

     public List<TransactionResponse> getTransactionHistory(String accountNumber){
        return  transactionRepository.findBySenderAccountNumberOrderByCreatedAtDesc(accountNumber).stream()
        .map(this::mapToResponse)
        .collect(Collectors.toList());
    }

    
    public TransactionResponse verifyOTP(String transactionId, String otp){
        return  null;
    }
}
