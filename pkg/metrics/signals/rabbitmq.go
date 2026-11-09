package signals

// rabbitMQPodSel limits broker metrics to the RabbitMQ workload pod.
const rabbitMQPodSel = `,pod=~"rabbitmq.*"`
