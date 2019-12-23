package common

import (
	"context"
	"fmt"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/peer/common/blockfilewatcher"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/hyperledger/fabric/protos/pubsub"
	"github.com/klauspost/reedsolomon"
	"google.golang.org/grpc"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type SubscribeServiceServer struct {
	mspId string
}

func (s *SubscribeServiceServer) NotifySubscriber(ctx context.Context, req *peer.SubscribeRequest) (*peer.SubscribeResponse, error) {
	logger.Infof("Get subscriber create info: %+v", req)
	conn, err := grpc.Dial(req.OrdererEndPoint, grpc.WithInsecure())
	if err != nil {
		logger.Errorf("Dail order pubsub server failed! %s", err.Error())
		return nil, err
	}

	client := pubsub.NewPubSubServiceClient(conn)
	stream, err := client.Subscribe(context.Background(), &pubsub.String{Value: req.ChannelId})
	if err != nil {
		logger.Errorf("Get subscribe stream from orderer error: %s", err.Error())
		return nil, err
	}
	// zyy: 开一个新的协程持续获取来自pubsub server的消息
	go func() {
		for {
			distributeList, err := stream.Recv()
			if err != nil {
				logger.Warnf("Stream receive distribute error: %s", err.Error())
			} else {
				signal := blockfilewatcher.GetBlockFileSignal(distributeList.ChannelId)
				suffixNum := <-signal
				if fmt.Sprintf("%06d", suffixNum) == distributeList.FileSuffix {
					err := encodeBlockfile(suffixNum, distributeList, s.mspId)
					if err != nil {
						logger.Errorf("Split block file fail! %s", err.Error())
					}
				} else {
					logger.Errorf("Unmatched file suffix number, distribute list is %s, but signal is %06d", distributeList.FileSuffix, suffixNum)
				}
			}
		}
	}()

	return &peer.SubscribeResponse{
		Msg: "Bootstrap subscriber success",
	}, nil
}

func encodeBlockfile(suffixNum int, distributeList *pubsub.DistributeList, mspId string) error {
	blockFilePath := filepath.Join(ledgerconfig.GetBlockStorePath(), "chains", distributeList.ChannelId)
	blockFile := filepath.Join(blockFilePath, fmt.Sprintf("blockfile_%06d", suffixNum))
	logger.Debugf("Get block file path: %s", blockFile)
	bfile, err := ioutil.ReadFile(blockFile)
	if err != nil {
		return err
	}

	encoder, err := reedsolomon.New(int(distributeList.DataShards), int(distributeList.ParShards))
	if err != nil {
		return err
	}
	res, err := encoder.Split(bfile)
	if err != nil {
		return err
	}

	fileBlockList := make([]int, 0)
	for fileBlockName, orgId := range distributeList.Item {
		if strings.ToLower(mspId) == strings.ToLower(orgId) {
			fileNums := strings.Split(fileBlockName, "_")
			fileNum, err := strconv.Atoi(fileNums[len(fileNums)-1])
			if err != nil {
				return err
			}
			fileBlockList = append(fileBlockList, fileNum)
		}
	}

	for _, v := range fileBlockList {
		fileBlockPath := filepath.Join(blockFilePath, fmt.Sprintf("%s_%06d_%d", distributeList.ChannelId, suffixNum, v))
		f, err := os.Create(fileBlockPath)
		if err != nil {
			return err
		}
		_, err = f.Write(res[v])
		if err != nil {
			return err
		}
		f.Close()
	}

	// os.Remove(blockFile)
	return nil
}

func NewSubscribeServiceServer(mspId string) *SubscribeServiceServer {
	return &SubscribeServiceServer{
		mspId: mspId,
	}
}
