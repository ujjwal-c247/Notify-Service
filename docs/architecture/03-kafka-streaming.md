# Kafka Streaming

**Folder:** `src/infrastructure/kafka/`

This module is responsible for asynchronously moving events from the Outbox into the Notification Service.

## Important Logic & Conditions

### 1. Consumer Groups
**File:** `kafka.consumer.ts`

When the app starts up, it connects a Kafka Consumer to the `booking.events` topic using the group ID `notification-service`.
If you deploy 5 instances of the `notify-service`, Kafka will automatically distribute the workload across them because they share the same consumer group.

### 2. At-Least-Once Delivery
Kafka guarantees that a message is delivered *at least once*. 
However, due to network blips or crashes, Kafka might occasionally deliver the exact same event two or three times. 

**Condition:** 
The Kafka consumer does *not* blindly send an email. It just takes the raw event payload from Kafka and passes it to the `NotificationService`, which is responsible for catching any duplicate deliveries.
