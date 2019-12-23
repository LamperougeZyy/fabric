package blockfilewatcher

import "sync"

var (
	rwLock  sync.RWMutex
	signals map[string]chan int
)

type PeerBlockFileWatcher struct {
	ChannelId string
}

func (bfpw *PeerBlockFileWatcher) BlockFileFull(suffixNum int) {
	go func() {
		rwLock.Lock()
		signals[bfpw.ChannelId] <- suffixNum
		rwLock.Unlock()
	}()
}

func NewPeerBlockFileWatcher(channelId string) *PeerBlockFileWatcher {
	rwLock.Lock()
	signals[channelId] = make(chan int)
	rwLock.Unlock()
	return &PeerBlockFileWatcher{
		ChannelId: channelId,
	}
}

func GetBlockFileSignal(channelId string) chan int {
	rwLock.RLock()
	signal := signals[channelId]
	rwLock.RUnlock()
	return signal
}
