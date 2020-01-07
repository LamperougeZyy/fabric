package blockfilewatcher

import (
	"fmt"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/orderer/common/blockfetcher"
	distribute_list_server "github.com/hyperledger/fabric/orderer/common/server/list_distribute_server"
	list_puller "github.com/hyperledger/fabric/protos/listpuller"
	"sort"
)

var (
	logger     = flogging.MustGetLogger("orderer.common.blockfilewatcher")
	DataShards = 3
	ParShards  = 3
)

type OrdererBlockFileWatcher struct {
	channelId string
}

func NewOrdererBlockFileWatcher(channelId string) *OrdererBlockFileWatcher {
	return &OrdererBlockFileWatcher{
		channelId: channelId,
	}
}

func (bfw *OrdererBlockFileWatcher) BlockFileFull(suffixNum int) {
	blockFetcher := blockfetcher.GetBlockFetcherInstance()
	distributeList := list_puller.DistributeList{
		Algorithm:  "RS",
		ChannelId:  bfw.channelId,
		FileSuffix: fmt.Sprintf("%06d", suffixNum),
		DataShards: int32(DataShards),
		ParShards:  int32(ParShards),
	}

	orgs, err := blockFetcher.GetAppGroups(bfw.channelId)
	if err != nil {
		logger.Fatalf("Watcher Get Org Information error! %s", err.Error())
	}
	orgNameList := make([]string, 0)
	orgName2anchorPeers := make(map[string][]byte)
	for orgName := range orgs {
		orgNameList = append(orgNameList, orgName)
		orgName2anchorPeers[orgName] = orgs[orgName].Values["AnchorPeers"].Value
	}
	sort.Strings(orgNameList)

	list := make(map[string]*list_puller.LocationInfo)
	for i := 0; i < ParShards+DataShards; i++ {
		orgName := orgNameList[i%len(orgNameList)]
		fileBlockId := fmt.Sprintf("%s_%06d_%d", bfw.channelId, suffixNum, i)
		list[fileBlockId] = &list_puller.LocationInfo{
			OrgName:     orgName,
			AnchorPeers: orgName2anchorPeers[orgName],
		}
	}

	distributeList.Item = list
	logger.Debugf("Create a new distribute lis: %+v", distributeList)

	// 存储到list_distribute_server的缓存中
	distribute_list_server.StoreDistributeList(distributeList.ChannelId, distributeList)
}
