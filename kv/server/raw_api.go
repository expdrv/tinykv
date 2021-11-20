package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	resp := &kvrpcpb.RawGetResponse{}
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		log.Errorf("[RawGet] Reader err:%v", err)
		resp.Error = err.Error()
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, nil
	}
	defer reader.Close()

	value, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		log.Errorf("[RawGet] Reader err:%v", err)
		resp.Error = err.Error()
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, nil
	}
	resp.Value = value
	if len(value) == 0 {
		resp.NotFound = true
	}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	resp := &kvrpcpb.RawPutResponse{}
	entry := storage.Modify{
		Data: storage.Put{
			Key:   req.GetKey(),
			Cf:    req.GetCf(),
			Value: req.GetValue(),
		},
	}
	err := server.storage.Write(req.GetContext(), []storage.Modify{entry})
	if err != nil {
		log.Errorf("[RawPut] write err:%v", err)
		resp.RegionError = util.RaftstoreErrToPbError(err)
		resp.Error = err.Error()
	}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	resp := &kvrpcpb.RawDeleteResponse{}
	entry := storage.Modify{
		Data: storage.Put{
			Key:   req.GetKey(),
			Cf:    req.GetCf(),
			Value: nil,
		},
	}
	err := server.storage.Write(req.GetContext(), []storage.Modify{entry})
	if err != nil {
		log.Errorf("[RawDelete] write err:%v", err)
		resp.RegionError = util.RaftstoreErrToPbError(err)
		resp.Error = err.Error()
	}
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	resp := &kvrpcpb.RawScanResponse{
		Kvs: []*kvrpcpb.KvPair{},
	}
	reader, err := server.storage.Reader(req.GetContext())

	if err != nil {
		log.Errorf("[RawScan] Reader err:%v", err)
		resp.Error = err.Error()
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, nil
	}
	defer reader.Close()

	iter := reader.IterCF(req.GetCf())
	defer iter.Close()
	iter.Seek(req.GetStartKey())
	for i := 0; i < int(req.GetLimit()) && iter.Valid(); i++ { // && iter.Valid()
		item := iter.Item()
		value, err := item.Value()
		if err != nil {
			log.Errorf("[RawScan] item.Value err:%v", err)
		}
		kvPair := &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: value,
		}
		resp.Kvs = append(resp.Kvs, kvPair)
		iter.Next()
	}
	return resp, nil
}
