package metrics

import (
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
)

var (
    // C端连接指标
    CendConnectionsTotal = promauto.NewGauge(prometheus.GaugeOpts{
        Name: "cend_connections_total",
        Help: "Total number of active C-end connections",
    })
    
    CendConnectionsCreated = promauto.NewCounter(prometheus.CounterOpts{
        Name: "cend_connections_created_total",
        Help: "Total number of C-end connections created",
    })
    
    CendMessagesReceived = promauto.NewCounter(prometheus.CounterOpts{
        Name: "cend_messages_received_total",
        Help: "Total number of C-end messages received",
    })
    
    CendMessagesSent = promauto.NewCounter(prometheus.CounterOpts{
        Name: "cend_messages_sent_total",
        Help: "Total number of C-end messages sent",
    })
    
    CendMessageProcessingTime = promauto.NewHistogram(prometheus.HistogramOpts{
        Name: "cend_message_processing_seconds",
        Help: "C-end message processing time",
        Buckets: prometheus.LinearBuckets(0.001, 0.001, 10),
    })
    
    // IoT端指标
    IoTPacketsReceived = promauto.NewCounter(prometheus.CounterOpts{
        Name: "iot_packets_received_total",
        Help: "Total number of IoT packets received",
    })
    
    IoTPacketsSent = promauto.NewCounter(prometheus.CounterOpts{
        Name: "iot_packets_sent_total",
        Help: "Total number of IoT packets sent",
    })
    
    IoTBytesReceived = promauto.NewCounter(prometheus.CounterOpts{
        Name: "iot_bytes_received_total",
        Help: "Total number of IoT bytes received",
    })
    
    IoTDevicesOnline = promauto.NewGauge(prometheus.GaugeOpts{
        Name: "iot_devices_online",
        Help: "Number of online IoT devices",
    })
    
    IoTPacketProcessingTime = promauto.NewHistogram(prometheus.HistogramOpts{
        Name: "iot_packet_processing_seconds",
        Help: "IoT packet processing time",
        Buckets: prometheus.LinearBuckets(0.0001, 0.0001, 10),
    })
    
    // 错误指标
    ErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "gateway_errors_total",
        Help: "Total number of errors",
    }, []string{"type", "gateway"})
)