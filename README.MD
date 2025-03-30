# Real-Time Customer Age Categorization using Kafka & Flink

This project sets up a real-time **Kafka & Flink pipeline** to categorize customer data based on age.

- Customers **under 25** → Sent to `target-audience-topic`
- Customers **25 and older** → Sent to `old-folks-topic`
- **Raw data remains unchanged** in `customer-input`

## **1. Prerequisites**
- **Docker & Docker Compose** installed
- **Python 3.7+** with `pip install kafka-python apache-flink`

---

## **2. Setup Kafka and Flink Using Docker**

### **Start the services:**
```bash
docker-compose up -d
```
- **Kafka Broker:** `localhost:9092`
- **Flink UI:** [http://localhost:8081](http://localhost:8081)

---

## **3. Create Kafka Topics**
```bash
docker exec -it kafka kafka-topics --create --topic customer-input --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -it kafka kafka-topics --create --topic target-audience-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -it kafka kafka-topics --create --topic old-folks-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

---

## **4. Start the Kafka Producer**

Run the producer:
```bash
python3 producer.py
```

---

## **5. Deploy Flink Job**

### **Deploy the Flink Job**
```bash
docker cp flink_job.py jobmanager:/flink_job.py
docker exec -it jobmanager flink run -py /flink_job.py
```

---

## **6. Verify Processed Data**
### **Target Audience Messages:**
```bash
python3 consumer-target.py
```
### **Old Folks Messages:**
```bash
python3 consumer-old.py 
```

---

## **7. Stop and Cleanup**
To stop services:
```bash
docker-compose down
```
To remove Kafka topics:
```bash
docker exec -it kafka kafka-topics --delete --topic customer-input --bootstrap-server localhost:9092
docker exec -it kafka kafka-topics --delete --topic target-audience-topic --bootstrap-server localhost:9092
docker exec -it kafka kafka-topics --delete --topic old-folks-topic --bootstrap-server localhost:9092
```

---

## 🎯 **Summary**
✅ Kafka Producer sends **customer data** to `customer-input`  
✅ Flink categorizes data and sends it to **2 different topics**  
✅ Kafka Consumers **read the processed messages** in real-time  

🚀 Now you have a **real-time streaming pipeline** with Kafka & Flink! 🎉
