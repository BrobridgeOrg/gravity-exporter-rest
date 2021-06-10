package subscriber

type SubscriptionConfig map[string][]RestRule

type RestRule struct {
	Method  string            `json:"method"`
	Uri     string            `json:"uri"`
	Headers map[string]string `json:"headers"`
}

type RuleConfig struct {
	Subscriptions SubscriptionConfig `json:"subscriptions"`
}
