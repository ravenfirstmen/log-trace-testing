package messaging

import (
	"encoding/json"
	"github.com/nats-io/nats.go"
)

type CreateMessage struct {
	Key  string `json:"key"`
	Info string `json:"info"`
}

type ListMessage struct {
	Key string `json:"key"`
}

type DeleteMessage struct {
	Key string `json:"key"`
}

func deserializeCreateMessage(msg *nats.Msg) (*CreateMessage, error) {
	createMsg := &CreateMessage{}
	err := json.Unmarshal(msg.Data, createMsg)
	if err != nil {
		return nil, err
	}

	return createMsg, nil
}

func deserializeListMessage(msg *nats.Msg) (*ListMessage, error) {
	listMsg := &ListMessage{}
	err := json.Unmarshal(msg.Data, listMsg)
	if err != nil {
		return nil, err
	}

	return listMsg, nil
}

func deserializeDeleteMessage(msg *nats.Msg) (*DeleteMessage, error) {
	deleteMsg := &DeleteMessage{}
	err := json.Unmarshal(msg.Data, deleteMsg)
	if err != nil {
		return nil, err
	}

	return deleteMsg, nil
}
