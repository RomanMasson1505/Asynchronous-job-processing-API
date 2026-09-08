package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type Handler func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error)

type Registry map[string]Handler

func DefaultRegistry() Registry {
	return Registry{
		"sleep":     Sleep,
		"uppercase": Uppercase,
	}
}

func Sleep(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var in struct {
		MS int `json:"ms"`
	}
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &in); err != nil {
			return nil, fmt.Errorf("payload invalide: %w", err)
		}
	}
	if in.MS <= 0 {
		in.MS = 100
	}

	select {
	case <-time.After(time.Duration(in.MS) * time.Millisecond):
		out, err := json.Marshal(map[string]int{"slept_ms": in.MS})
		if err != nil {
			return nil, err
		}
		return out, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func Uppercase(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var in struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(payload, &in); err != nil {
		return nil, fmt.Errorf("payload invalide: %w", err)
	}
	if in.Text == "" {
		return nil, fmt.Errorf("champ \"text\" manquant ou vide")
	}

	out, err := json.Marshal(map[string]string{"text": strings.ToUpper(in.Text)})
	if err != nil {
		return nil, err
	}
	return out, nil
}
