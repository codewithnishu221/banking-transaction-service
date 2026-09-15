package com.banking.transaction_service.entity;
/*
* Transaction Lifecycle flow:
* PENDING -> PROCESSING -> COMPLETED (clean transaction) -> PNDING_VERIFICATION(suspicious detected) -. COMPLETED(verified) -> FLAGGED(saga refund)
-> FAILED
 */
public enum TransactionStatus {
     PENDING,
     PROCESSING,
     COMPLETED,
     PENDING_VERIFICATION,
     FAILED,
     FLAGGED
}
