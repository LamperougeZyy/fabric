package blockfilewatcher

import (
	"context"
	"fmt"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/orderer/common/blockfetcher"
	"github.com/hyperledger/fabric/protos/pubsub"
	"google.golang.org/grpc"
	"sort"
)

var (
	logger        = flogging.MustGetLogger("orderer.common.blockfilewatcher")
	publishClient pubsub.PubSubServiceClient
	DataShards    int
	ParShards     int
)

type OrdererBlockFileWatcher struct {
	channelId     string
	blkFileLedger blockledger.Reader
}

func NewOrdererBlockFileWatcher(channelId string) *OrdererBlockFileWatcher {
	return &OrdererBlockFileWatcher{
		channelId: channelId,
	}
}

func (bfw *OrdererBlockFileWatcher) BlockFileFull(suffixNum int) {
	blockfetcher := blockfetcher.GetBlockFetcherInstance()
	distributeList := &pubsub.DistributeList{
		Algorithm:  "RS",
		ChannelId:  bfw.channelId,
		FileSuffix: fmt.Sprintf("%06d", suffixNum),
		DataShards: int32(DataShards),
		ParShards:  int32(ParShards),
	}

	orgs, err := blockfetcher.GetAppGroups(bfw.channelId)
	if err != nil {
		logger.Fatalf("Watcher Get Org Information error! %s", err.Error())
	}
	orgNameList := make([]string, 0)
	for orgName, _ := range orgs {
		orgNameList = append(orgNameList, orgName)
	}
	sort.Strings(orgNameList)

	list := make(map[string]string)
	for i := 0; i < ParShards+DataShards; i++ {
		orgName := orgNameList[i%len(orgNameList)]
		list[orgName] = fmt.Sprintf("%s_%06d_%d", bfw.channelId, suffixNum, i)
	}

	distributeList.Item = list
	_, err = publishClient.Publish(context.Background(), distributeList)
	if err != nil {
		logger.Fatalf("Publish distribute list error! %s", err.Error())
	}
}

func InitPublisClient(address string) {
	logger.Infof("Get peer server address: %s", address)
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	publishClient = pubsub.NewPubSubServiceClient(conn)
}
