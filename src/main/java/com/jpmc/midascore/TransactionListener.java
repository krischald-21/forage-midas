package com.jpmc.midascore;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class TransactionListener {
    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRecordRepository transactionRecordRepository;

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

        // Adjust balances
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount());

        // Persist updated users
        userRepository.save(sender);
        userRepository.save(recipient);

        // Save Transaction record
        TransactionRecord record = new TransactionRecord();
        record.setSender(sender);
        record.setRecipient(recipient);
        record.setAmount(transaction.getAmount());
        record.setTimestamp(LocalDateTime.now());

        transactionRecordRepository.save(record);

        System.out.println("Transaction successfully processed and recorded.");

        // Check if Waldorf is involved and print balance after transaction
        if ("waldorf".equalsIgnoreCase(sender.getName()) || "waldorf".equalsIgnoreCase(recipient.getName())) {
            UserRecord waldorf = null;
            if ("waldorf".equalsIgnoreCase(sender.getName())) {
                waldorf = sender;
            } else {
                waldorf = recipient;
            }
            // Print Waldorf’s balance rounded down
            System.out.println("Waldorf’s balance after this transaction: " + (int) Math.floor(waldorf.getBalance()));
        }
    }
}
