package com.jpmc.midascore;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDateTime;

@Service
public class TransactionListener {
    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRecordRepository transactionRecordRepository;

    @Autowired
    private RestTemplate restTemplate;

    private static final String INCENTIVE_API_URL = "http://localhost:8080/incentive";

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group", containerFactory = "kafkaListenerContainerFactory")
    public void listen(Transaction transaction) {
        System.out.println("Received transaction: " + transaction);

        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        // Validate Transaction
        if (sender == null || recipient == null) {
            System.out.println("Invalid sender or recipient. Skipping Transaction.");
            return;
        }

        if (sender.getBalance() < transaction.getAmount()) {
            System.out.println("Insufficient balance. Skipping transaction.");
            return;
        }

        // Call Incentive API
        Incentive incentiveResponse = restTemplate.postForObject(INCENTIVE_API_URL, transaction, Incentive.class);
        float incentiveAmount = (incentiveResponse != null) ? incentiveResponse.getAmount() : 0;

        System.out.println("Incentive amount received: " + incentiveAmount);

        // Adjust balances
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

        // Persist updated users
        userRepository.save(sender);
        userRepository.save(recipient);

        // Save Transaction record
        TransactionRecord record = new TransactionRecord();
        record.setSender(sender);
        record.setRecipient(recipient);
        record.setAmount(transaction.getAmount());
        record.setIncentive(incentiveAmount);
        record.setTimestamp(LocalDateTime.now());

        transactionRecordRepository.save(record);

        System.out.println("Transaction successfully processed and recorded.");

        // Check if Wilbur is involved and print balance after transaction
        if ("wilbur".equalsIgnoreCase(sender.getName()) || "wilbur".equalsIgnoreCase(recipient.getName())) {
            UserRecord wilbur = null;
            if ("wilbur".equalsIgnoreCase(sender.getName())) {
                wilbur = sender;
            } else {
                wilbur = recipient;
            }
            // Print Wilbur’s balance rounded down
            System.out.println("Wilbur’s balance after this transaction: " + (int) Math.floor(wilbur.getBalance()));
        }
    }
}
