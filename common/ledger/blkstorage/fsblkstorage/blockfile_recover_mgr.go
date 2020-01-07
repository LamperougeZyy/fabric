package fsblkstorage

import (
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"sync"
)

var (
	blockFileRecovererMgrInst *BlockFileRecovererMgr
	once                      sync.Once
)

type BlockFileRecovererMgr struct {
	store map[string]blkstorage.BlockFileRecoverer
	lock  sync.RWMutex
}

func (b BlockFileRecovererMgr) Notify(channelId string, suffixNum string) {
	b.lock.RLock()
	defer b.lock.RUnlock()
	if recoverer, ok := b.store[channelId]; !ok {
		logger.Errorf("Do not have channel %s recoverer", channelId)
	} else {
		recoverer.BlockFileRecover(suffixNum)
	}
}

func (b BlockFileRecovererMgr) RegistRecoverer(channelId string, recoverer blkstorage.BlockFileRecoverer) {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.store[channelId] = recoverer
}

func InitBlockFileRecoverMgr() {
	once.Do(func() {
		blockFileRecovererMgrInst = &BlockFileRecovererMgr{store: map[string]blkstorage.BlockFileRecoverer{}}
	})
}

func GetBlockFileRecoverMgrInst() *BlockFileRecovererMgr {
	return blockFileRecovererMgrInst
}
