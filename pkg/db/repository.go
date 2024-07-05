package db

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"math/rand/v2"
	"time"
)

type Repository interface {
	Create(key string, info string) error
	List(key string) error
	Delete(key string) error
}

type DynamoDbRepository struct {
	context   context.Context
	tracer    trace.Tracer
	logger    *log.Entry
	client    *dynamodb.Client
	tableName string
}

func NewDynamoDbRepository(context context.Context, tracer trace.Tracer, log *log.Entry, tableName string) (*DynamoDbRepository, error) {
	logger := log.WithField("table_name", tableName)

	logger.Info("Building dynamodb client")
	cfg, err := config.LoadDefaultConfig(context)
	if err != nil {
		logger.WithError(err).Error("failed to AWS load config")
		return nil, err
	}
	logger.Info("Dynamodb client build successful")

	return &DynamoDbRepository{
		context:   context,
		logger:    logger,
		client:    dynamodb.NewFromConfig(cfg),
		tableName: tableName,
		tracer:    tracer,
	}, nil
}

func (d DynamoDbRepository) Create(key string, info string) error {
	ctx, span := d.tracer.Start(d.context,
		"Create record",
		trace.WithAttributes(
			attribute.String("key", key),
			attribute.String("info", info),
		))
	defer span.End()

	logger := d.logger.WithContext(ctx).WithFields(log.Fields{
		"key":  key,
		"info": key,
	})

	logger.Info("Saving dynamodb record")
	_, err := d.client.PutItem(d.context, &dynamodb.PutItemInput{
		TableName: aws.String(d.tableName),
		Item: map[string]types.AttributeValue{
			"key":  &types.AttributeValueMemberS{Value: key},
			"info": &types.AttributeValueMemberS{Value: info},
		},
	})
	if err != nil {
		logger.WithError(err).Error("failed to save dynamodb record")
		return err
	}
	logger.Info("Dynamodb record successfully created")

	d.aSubTask(ctx, "After create record", false)

	return nil
}

func (d DynamoDbRepository) List(key string) error {
	ctx, span := d.tracer.Start(d.context,
		"List records",
		trace.WithAttributes(attribute.String("key", key)),
	)
	defer span.End()

	logger := d.logger.WithContext(ctx).WithField("key", key)

	logger.Info("Starting dynamodb query")
	keyEx := expression.Key("key").BeginsWith(key)
	expr, err := expression.NewBuilder().WithKeyCondition(keyEx).Build()
	if err != nil {
		logger.WithError(err).Error("failed to build search expression")
		return err
	}

	logger.Info("Querying dynamodb")
	queryPaginator := dynamodb.NewQueryPaginator(d.client, &dynamodb.QueryInput{
		TableName:                 aws.String(d.tableName),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		KeyConditionExpression:    expr.KeyCondition(),
	})
	for queryPaginator.HasMorePages() {
		_, err := queryPaginator.NextPage(d.context)
		if err != nil {
			logger.WithError(err).Error("failed to fetch dynamodb record")
			return err
		}
		// mais coisas...
	}
	logger.Info("Finish querying dynamodb successfully")

	d.aSubTask(ctx, "After list records", false)

	return nil
}

func (d DynamoDbRepository) Delete(key string) error {
	ctx, span := d.tracer.Start(d.context,
		"Delete record",
		trace.WithAttributes(attribute.String("key", key)),
	)
	defer span.End()

	logger := d.logger.WithContext(ctx).WithField("key", key)

	logger.Info("Deleting dynamodb record")
	_, err := d.client.DeleteItem(d.context, &dynamodb.DeleteItemInput{
		TableName: aws.String(d.tableName),
		Key: map[string]types.AttributeValue{
			"key": &types.AttributeValueMemberS{Value: key},
		},
	})
	if err != nil {
		logger.WithError(err).Error("failed to delete dynamodb record")
		return err
	}
	logger.Info("Dynamodb record successfully deleted")

	d.aSubTask(ctx, "After Delete record", false)

	return nil
}

func (d DynamoDbRepository) aSubTask(tctx context.Context, taskName string, final bool) {
	ctx, span := d.tracer.Start(tctx,
		taskName,
	)
	defer span.End()
	logger := d.logger.WithContext(ctx)

	logger.Info(fmt.Sprintf("Starting doing task: %s", taskName))
	sleepBy := rand.IntN(3)
	time.Sleep(time.Duration(sleepBy) * time.Second)

	if !final {
		d.aSubTask(ctx, fmt.Sprintf("... a sub task of %s", taskName), true)
	}

	logger.Info(fmt.Sprintf("End doing task: %s", taskName))
}
