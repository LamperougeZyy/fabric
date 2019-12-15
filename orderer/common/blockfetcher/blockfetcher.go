package blockfetcher

import (
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/configtx"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/utils"
	"sync"
)

const (
	// ApplicationGroupKey is the group name for the Application config
	ApplicationGroupKey = "Application"
)

type BlockFetcher struct {
	ledgerFactory blockledger.FactoryWithWatcher
}

var blockFetcher BlockFetcher
var once sync.Once

func InitializeBlockFetcherInstance(lf blockledger.FactoryWithWatcher) {
	once.Do(func() {
		blockFetcher = BlockFetcher{lf}
	})
}

func GetBlockFetcherInstance() BlockFetcher {
	return blockFetcher
}

func (b *BlockFetcher) GetAppGroups(channelId string) (map[string]*common.ConfigGroup, error) {
	ledger, err := b.ledgerFactory.GetOrCreate(channelId)
	if err != nil {
		return nil, err
	}
	lastBlock := blockledger.GetBlock(ledger, ledger.Height()-1)
	index, err := utils.GetLastConfigIndexFromBlock(lastBlock)
	if err != nil {
		return nil, err
	}
	configBlock := blockledger.GetBlock(ledger, index)
	if configBlock == nil {
		return nil, err
	}

	env := &common.Envelope{}
	err = proto.Unmarshal(configBlock.Data.Data[0], env)
	if err != nil {
		return nil, err
	}

	payload, err := utils.UnmarshalPayload(env.Payload)
	if err != nil {
		return nil, err
	}

	configEnvelope, err := configtx.UnmarshalConfigEnvelope(payload.Data)
	if err != nil {
		return nil, err
	}

	appGroups := configEnvelope.Config.ChannelGroup.Groups[ApplicationGroupKey].Groups
	return appGroups, nil
}
