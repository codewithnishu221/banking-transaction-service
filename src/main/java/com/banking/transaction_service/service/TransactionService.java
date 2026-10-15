package com.banking.transaction_service.service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;

import com.banking.transaction_service.client.AccountServiceClient;
import com.banking.transaction_service.dto.TransactionResponse;
import com.banking.transaction_service.dto.TransferRequest;
import com.banking.transaction_service.entity.Transaction;
import com.banking.transaction_service.entity.TransactionStatus;
import com.banking.transaction_service.entity.TransactionType;
import com.banking.transaction_service.event.TransactionCompletedEvent;
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
    private final RedisTemplate<String, String> redisTemplate;
    private static  final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private static  final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private static  final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";
    private static  final String FRAUD_DETECTED_TOPIC  = "fraud.detected";

    
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
        log.info("OTP verification for the transaction: {}", transactionId);
        Transaction transaction = transactionRepository.findById(transactionId).orElseThrow(() -> new RuntimeException("Transaction not found "+ transactionId));
        String otpKey = "verification:otp" + transactionId;
        String storedOtp = redisTemplate.opsForValue().get(otpKey);

        if(storedOtp == null){
            // OTP Expired
            log.warn("OTP expired for transaction: {}", transactionId);
            compensateTransaction(transaction, "OTP expired - transaction cancelled and amunt refunded");
            return mapToResponse(transaction);
        }
        if(!storedOtp.equals(otp)){
            log.warn("Wrong OTP - blocking account and refunding: {}", transactionId);
            redisTemplate.delete(otpKey);
            blockAccountAndCompensate(transaction, "Wrong OTP entered - transaction cancelled, "+ "account blocked for security");

            return mapToResponse(transaction);
        }
        // OTP correct- complete transaction
        log.info("OTP verified - completing transaction: {}", transactionId);
        redisTemplate.delete(otpKey);
        completeTransaction(transaction);
        return mapToResponse(transaction);
    }

    private void compensateTransaction(Transaction transaction, String reason){
        log.warn("SAGA COMPENDATION - refunding: {} amount: {}", transaction.getSenderAccountNumber(), transaction.getAmount());
        //CREDIT MONEY BACK TO SENDER SYNCHRONOUSLY

        accountServiceClient.creditBalance(transaction.getSenderAccountNumber(), transaction.getAmount());
        transaction.setStatus(TransactionStatus.FLAGGED);
        transaction.setFailureReason(reason + " - SAGA Compensation executed, amount refunded at "+ LocalDateTime.now());

        transactionRepository.save(transaction);
        Map<String, Object> refundEvent = new HashMap<>();
        refundEvent.put("transactionId", transaction.getId());
        refundEvent.put("senderAccountNumber", transaction.getSenderAccountNumber());
        refundEvent.put("amount", transaction.getAmount());
        refundEvent.put("reason", reason);
        kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC, transaction.getId(), refundEvent);
        log.info("SAGA COMPENSATION COMPLETE - {} refunded to {}", transaction.getAmount(), transaction.getSenderAccountNumber());

    }

    private void blockAccountAndCompensate(Transaction transaction, String reason){
        Map<String, Object> fraudEvent = new HashMap<>();
        fraudEvent.put("transactionId", transaction.getId());
        fraudEvent.put("amount", transaction.getAmount());
        fraudEvent.put("reason", reason);
        kafkaTemplate.send(FRAUD_DETECTED_TOPIC, transaction.getSenderAccountNumber(), fraudEvent);
        log.warn("Fraud.detected published - account: {} will be blocked, Kindly contact to the bank", transaction.getSenderAccountNumber());

        //SAGA COMPENSATION - refund Sender
        compensateTransaction(transaction, reason);
    }
    
    private void completeTransaction(Transaction transaction){
        transaction.setStatus(TransactionStatus.COMPLETED);
        transaction.setCompletedAt(LocalDateTime.now());
        transactionRepository.save(transaction);

        TransactionCompletedEvent completedEvent = new TransactionCompletedEvent(
        transaction.getId(),
        transaction.getSenderAccountNumber(),
        transaction.getReceiverAccountNumber(),
        transaction.getAmount(),
        transaction.getDescription()
        );
        kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC, transaction.getId(), completedEvent);

        log.info("SAGA COMPLETE - Transaction {} completed", transaction.getId());
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


    public void processCleanresult(String transactionId){

        Transaction transaction = transactionRepository.findById(transactionId).orElseThrow(() -> new RuntimeException("Transaction not found "+ transactionId));

        if(transaction.getStatus() != TransactionStatus.PROCESSING){
            log.warn("Transaction {} not PROCESSING -  skipping", transactionId);
            return;
        }

        completeTransaction(transaction);

    }


}
