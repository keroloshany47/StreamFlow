package com.datamasterylab.dto;

import java.util.Random;
import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Transaction {

    private String transactionId;
    private String userId;
    private double amount;
    private long transactionTime;
    private String merchantId;
    private String transactionType;
    private String location;
    private String paymentMethod;
    private boolean isInternational;
    private String currency;

    private static final Random random = new Random();

    @Override
    public String toString() {
        return "Transaction{" +
                "transactionId='" + transactionId + '\'' +
                ", userId='" + userId + '\'' +
                ", amount=" + amount +
                ", transactionTime=" + transactionTime +
                ", merchantId='" + merchantId + '\'' +
                ", transactionType='" + transactionType + '\'' +
                ", location='" + location + '\'' +
                ", paymentMethod='" + paymentMethod + '\'' +
                ", isInternational=" + isInternational +
                ", currency='" + currency + '\'' +
                '}';
    }

    public static Transaction randomTransaction() {
        Transaction transaction = new Transaction();
        transaction.setTransactionId(UUID.randomUUID().toString());
        transaction.setUserId("user" + random.nextInt(100));
        transaction.setAmount(10 + (500 * random.nextDouble())); // Random amount between 10 and 500
        transaction.setTransactionTime(System.currentTimeMillis());
        transaction.setMerchantId("merchant" + random.nextInt(50));
        transaction.setTransactionType(random.nextBoolean() ? "purchase" : "refund");
        transaction.setLocation(random.nextBoolean() ? "NY, USA" : "LA, USA");
        transaction.setPaymentMethod(random.nextBoolean() ? "credit_card" : "debit_card");
        transaction.setInternational(random.nextBoolean());
        transaction.setCurrency(random.nextBoolean() ? "USD" : "EUR");
        return transaction;
    }
}