package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// RabbitMQ management credentials match pkg/deployer/registry.go (rabbitmq component).
const rabbitMQMgmtUser = "cloud9"
const rabbitMQMgmtPass = "cloud9"

// scrapeNginxStubActiveConnections returns active connection count from lb:80/nginx_status, or 0 on any failure.
func (c *MetricsCollector) scrapeNginxStubActiveConnections(ctx context.Context, namespace string) float64 {
	if c == nil || c.scrapeClient == nil {
		return 0
	}
	u := fmt.Sprintf("http://lb.%s.svc.cluster.local/nginx_status", namespace)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0
	}
	resp, err := c.scrapeClient.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return 0
	}
	return parseNginxActiveConnections(string(body))
}

type rabbitOverviewSnapshot struct {
	QueueTotals struct {
		Messages        int `json:"messages"`
		MessagesReady   int `json:"messages_ready"`
		MessagesUnacked int `json:"messages_unacknowledged"`
	} `json:"queue_totals"`
	MessageStats struct {
		PublishDetails struct {
			Rate float64 `json:"rate"`
		} `json:"publish_details"`
		DeliverGetDetails struct {
			Rate float64 `json:"rate"`
		} `json:"deliver_get_details"`
	} `json:"message_stats"`
}

// scrapeRabbitMQOverview reads /api/overview from the mq service; returns zeros on failure.
func (c *MetricsCollector) scrapeRabbitMQOverview(ctx context.Context, namespace string) (depth, produce, consume, ready, unacked float64) {
	if c == nil || c.scrapeClient == nil {
		return 0, 0, 0, 0, 0
	}
	u := fmt.Sprintf("http://mq.%s.svc.cluster.local:15672/api/overview", namespace)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, 0, 0, 0, 0
	}
	req.SetBasicAuth(rabbitMQMgmtUser, rabbitMQMgmtPass)
	resp, err := c.scrapeClient.Do(req)
	if err != nil {
		return 0, 0, 0, 0, 0
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, 0, 0, 0, 0
	}
	var overview rabbitOverviewSnapshot
	if err := json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(&overview); err != nil {
		return 0, 0, 0, 0, 0
	}
	return float64(overview.QueueTotals.Messages),
		overview.MessageStats.PublishDetails.Rate,
		overview.MessageStats.DeliverGetDetails.Rate,
		float64(overview.QueueTotals.MessagesReady),
		float64(overview.QueueTotals.MessagesUnacked)
}
