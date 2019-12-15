package blockfilewatcher

type PeerBlockFileWatcher struct {
	ChannelId string
}

func NewPeerBlockFileWatcher(channelId string) *PeerBlockFileWatcher {
	return &PeerBlockFileWatcher{
		ChannelId: channelId,
	}
}

func (bfpw *PeerBlockFileWatcher) BlockFileFull() {
	//todo: zyy
}
