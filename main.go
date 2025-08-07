package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"auditor/internal/audit"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
)

var (
	// Prometheus metrics
	auditsProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "auditor_audits_processed_total",
			Help: "Total number of website audits processed",
		},
		[]string{"status"},
	)
	
	auditDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "auditor_audit_duration_seconds",
			Help: "Duration of website audits",
		},
		[]string{"status"},
	)
	
	kafkaMessagesProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "auditor_kafka_messages_total",
			Help: "Total number of Kafka messages processed",
		},
		[]string{"topic", "status"},
	)
)

func init() {
	// Register Prometheus metrics
	prometheus.MustRegister(auditsProcessed)
	prometheus.MustRegister(auditDuration)
	prometheus.MustRegister(kafkaMessagesProcessed)
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("Starting LeadFlowX Website Auditor Service...")

	// Configuration from environment variables
	kafkaBroker := getEnv("KAFKA_BROKER", "kafka:9092")
	verifiedTopic := getEnv("KAFKA_VERIFIED_TOPIC", "lead.verified")
	auditTopic := getEnv("KAFKA_AUDIT_TOPIC", "lead.audit")
	dlqTopic := getEnv("KAFKA_DLQ_TOPIC", "lead.audit.dlq")
	groupID := getEnv("KAFKA_GROUP_ID", "auditor-group")
	metricsPort := getEnv("METRICS_PORT", "9091")

	log.Printf("Configuration: broker=%s, verified_topic=%s, audit_topic=%s, group_id=%s", 
		kafkaBroker, verifiedTopic, auditTopic, groupID)

	// Start metrics server
	go startMetricsServer(metricsPort)

	// Setup Kafka reader and writers
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   verifiedTopic,
		GroupID: groupID,
		Logger:  log.Default(),
	})
	defer reader.Close()

	auditWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaBroker},
		Topic:   auditTopic,
		Logger:  log.Default(),
	})
	defer auditWriter.Close()

	dlqWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaBroker},
		Topic:   dlqTopic,
		Logger:  log.Default(),
	})
	defer dlqWriter.Close()

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Received shutdown signal, gracefully stopping...")
		cancel()
	}()

	// Main processing loop
	log.Println("Starting message processing loop...")
	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down auditor service...")
			return
		default:
			processMessage(ctx, reader, auditWriter, dlqWriter)
		}
	}
}

func processMessage(ctx context.Context, reader *kafka.Reader, auditWriter, dlqWriter *kafka.Writer) {
	// Set read timeout
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	message, err := reader.ReadMessage(ctxWithTimeout)
	if err != nil {
		if err == context.DeadlineExceeded {
			return // Normal timeout, continue loop
		}
		log.Printf("Error reading Kafka message: %v", err)
		kafkaMessagesProcessed.WithLabelValues("lead.verified", "error").Inc()
		return
	}

	kafkaMessagesProcessed.WithLabelValues("lead.verified", "received").Inc()
	
	// Parse correlation ID from headers
	correlationID := getCorrelationIDFromHeaders(message.Headers)
	
	log.Printf("Processing lead audit for correlation_id=%s", correlationID)

	// Parse lead data
	var lead map[string]interface{}
	if err := json.Unmarshal(message.Value, &lead); err != nil {
		log.Printf("Error parsing lead data: %v", err)
		sendToDLQ(dlqWriter, message.Value, correlationID, "json_parse_error", err)
		return
	}

	// Extract website URL
	websiteURL, ok := lead["website"].(string)
	if !ok || websiteURL == "" {
		log.Printf("Missing or invalid website URL in lead data")
		sendToDLQ(dlqWriter, message.Value, correlationID, "missing_website", 
			fmt.Errorf("website field missing or invalid"))
		return
	}

	// Validate URL format
	if !audit.IsValidURL(websiteURL) {
		log.Printf("Invalid website URL format: %s", websiteURL)
		sendToDLQ(dlqWriter, message.Value, correlationID, "invalid_url", 
			fmt.Errorf("invalid URL format: %s", websiteURL))
		return
	}

	// Perform website audit
	start := time.Now()
	auditResult, err := audit.AuditSite(websiteURL)
	duration := time.Since(start)

	if err != nil {
		log.Printf("Audit failed for %s: %v", websiteURL, err)
		auditsProcessed.WithLabelValues("error").Inc()
		auditDuration.WithLabelValues("error").Observe(duration.Seconds())
		
		// Send to DLQ with audit error
		sendToDLQ(dlqWriter, message.Value, correlationID, "audit_error", err)
		return
	}

	// Add audit result to lead data
	lead["audit"] = auditResult
	lead["audit_timestamp"] = time.Now().UTC().Format(time.RFC3339)

	// Preserve correlation ID
	if correlationID != "" {
		lead["correlationId"] = correlationID
	}

	// Marshal enriched lead data
	enrichedData, err := json.Marshal(lead)
	if err != nil {
		log.Printf("Error marshaling enriched lead data: %v", err)
		sendToDLQ(dlqWriter, message.Value, correlationID, "marshal_error", err)
		return
	}

	// Send to audit topic
	auditMessage := kafka.Message{
		Value: enrichedData,
		Headers: []kafka.Header{
			{Key: "correlationId", Value: []byte(correlationID)},
			{Key: "source", Value: []byte("auditor")},
		},
	}

	if err := auditWriter.WriteMessages(ctx, auditMessage); err != nil {
		log.Printf("Error sending audit result to Kafka: %v", err)
		kafkaMessagesProcessed.WithLabelValues("lead.audit", "error").Inc()
		sendToDLQ(dlqWriter, message.Value, correlationID, "kafka_write_error", err)
		return
	}

	// Success metrics
	auditsProcessed.WithLabelValues("success").Inc()
	auditDuration.WithLabelValues("success").Observe(duration.Seconds())
	kafkaMessagesProcessed.WithLabelValues("lead.audit", "sent").Inc()

	log.Printf("Successfully audited and sent lead: correlation_id=%s, score=%d, duration=%.2fs", 
		correlationID, auditResult.AuditScore, duration.Seconds())
}

func sendToDLQ(dlqWriter *kafka.Writer, originalData []byte, correlationID, errorType string, err error) {
	dlqData := map[string]interface{}{
		"original_data":   string(originalData),
		"correlation_id":  correlationID,
		"error_type":     errorType,
		"error_message":  err.Error(),
		"timestamp":      time.Now().UTC().Format(time.RFC3339),
		"service":        "auditor",
	}

	dlqBytes, _ := json.Marshal(dlqData)
	
	dlqMessage := kafka.Message{
		Value: dlqBytes,
		Headers: []kafka.Header{
			{Key: "correlationId", Value: []byte(correlationID)},
			{Key: "errorType", Value: []byte(errorType)},
			{Key: "source", Value: []byte("auditor")},
		},
	}

	if writeErr := dlqWriter.WriteMessages(context.Background(), dlqMessage); writeErr != nil {
		log.Printf("Error sending message to DLQ: %v", writeErr)
	} else {
		log.Printf("Sent failed lead to DLQ: correlation_id=%s, error=%s", correlationID, errorType)
	}
	
	kafkaMessagesProcessed.WithLabelValues("lead.audit.dlq", "sent").Inc()
}

func getCorrelationIDFromHeaders(headers []kafka.Header) string {
	for _, header := range headers {
		if header.Key == "correlationId" {
			return string(header.Value)
		}
	}
	return ""
}

func startMetricsServer(port string) {
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "service": "auditor"})
	})
	
	log.Printf("Starting metrics server on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}


