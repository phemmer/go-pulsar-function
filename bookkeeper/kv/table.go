package kv

import (
	"context"
	"fmt"
	"math/big"

	"github.com/phemmer/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/phemmer/go-pulsar-function/bookkeeper/proto/kv"
	"github.com/phemmer/go-pulsar-function/bookkeeper/proto/kv/rpc"
	"github.com/phemmer/go-pulsar-function/bookkeeper/proto/stream"
)

type Table struct {
	cl          rpc.TableServiceClient
	streamProps *stream.StreamProperties
}

func newTable(conn *grpc.ClientConn, streamProps *stream.StreamProperties) *Table {
	return &Table{
		cl:          rpc.NewTableServiceClient(conn),
		streamProps: streamProps,
	}
}

// rtCtx adds the routing information for the given key to the request context.
func (tbl *Table) rtCtx(ctx context.Context, key string) context.Context {
	return metadata.NewOutgoingContext(
		ctx,
		metadata.New(map[string]string{
			"bk-rt-sid-bin": int64bin(tbl.streamProps.StreamId),
			"bk-rt-Key-bin": key,
		}),
	)
}

func (tbl *Table) get(ctx context.Context, key string) (*kv.KeyValue, error) {
	req := &rpc.RangeRequest{
		Header: &rpc.RoutingHeader{
			StreamId: tbl.streamProps.StreamId,
			RKey:     []byte(key),
		},
		Key: []byte(key),
	}
	resp, err := tbl.cl.Range(tbl.rtCtx(ctx, key), req)
	if err := errChk(resp, err); err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, nil
		}
		return nil, errors.F(err, "Range")
	}

	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	return resp.Kvs[0], nil
}

// Retrieves the value and version of the specified key.
// Returns nil value if key does not exist.
func (tbl *Table) Get(ctx context.Context, key string) ([]byte, int64, error) {
	v, err := tbl.get(ctx, key)
	if err != nil {
		return nil, 0, err
	}
	if v == nil {
		return nil, 0, nil
	}
	return v.Value, v.Version, nil
}

// Retrieves the integer value of the specified key.
func (tbl *Table) GetInt(ctx context.Context, key string) (int64, error) {
	v, err := tbl.get(ctx, key)
	if err != nil {
		return 0, err
	}
	if !v.IsNumber {
		return 0, fmt.Errorf("not a number")
	}
	return v.NumberValue, nil
}

var big0 = big.NewInt(0)

func (tbl *Table) getRange(ctx context.Context, keyPrefix string) ([]*kv.KeyValue, error){
	key := []byte(keyPrefix)
	keyEnd := big.NewInt(0).Add(big0, big.NewInt(0).SetBytes(key)).Bytes()

	req := &rpc.RangeRequest{
		Header: &rpc.RoutingHeader{
			StreamId: tbl.streamProps.StreamId,
			RKey:     key,
		},
		Key:      key,
		RangeEnd: keyEnd,
	}
	resp, err := tbl.cl.Range(tbl.rtCtx(ctx, keyPrefix), req)
	if err := errChk(resp, err); err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}

	return resp.Kvs, nil
}

type KeyValue struct {
	Key   string
	Value []byte
}
func (tbl *Table) PrefixGet(ctx context.Context, prefix string) ([]KeyValue, error) {
	rkvs, err := tbl.getRange(ctx, prefix)
	if err != nil { return nil, err }

	kvs := make([]KeyValue, len(rkvs))
	for i, rkv := range rkvs {
		kvs[i] = KeyValue{string(rkv.Key), rkv.Value}
	}
	return kvs, nil
}

// Sets the value of the specified key to the given value.
// If version > 0, the key must already exist with the specified version.
// If version == 0, the key must not exist.
// If version == -1, version checking is not performed.
func (tbl *Table) Put(ctx context.Context, key string, version int64, value []byte) error {
	req := rpc.PutRequest{
		Key:             []byte(key),
		Value:           value,
		ExpectedVersion: version,
		Header: &rpc.RoutingHeader{
			StreamId: tbl.streamProps.StreamId,
			RKey:     []byte(key),
		},
	}
	return errChk(tbl.cl.Put(tbl.rtCtx(ctx, key), &req))
}

func (tbl *Table) Incr(ctx context.Context, key string, value int64) (int64, error) {
	req := rpc.IncrementRequest{
		Key:      []byte(key),
		Amount:   value,
		GetTotal: true,
		Header: &rpc.RoutingHeader{
			StreamId: tbl.streamProps.StreamId,
			RKey:     []byte(key),
		},
	}
	resp, err := tbl.cl.Increment(tbl.rtCtx(ctx, key), &req)
	if err := errChk(resp, err); err != nil {
		return 0, err
	}
	return resp.TotalAmount, nil
}

func (tbl *Table) Delete(ctx context.Context, key string) error {
	req := rpc.DeleteRangeRequest{
		Key: []byte(key),
		Header: &rpc.RoutingHeader{
			StreamId: tbl.streamProps.StreamId,
			RKey:     []byte(key),
		},
	}
	return errChk(tbl.cl.Delete(tbl.rtCtx(ctx, key), &req))
}
