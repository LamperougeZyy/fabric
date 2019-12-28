package distribute_list_server

import (
	list_puller "github.com/hyperledger/fabric/protos/listpuller"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

var channel2listArr map[string][]list_puller.DistributeList

type DistributeListServer struct {
}

func (d *DistributeListServer) GetDistributeList(_ context.Context, req *list_puller.Request) (*list_puller.DistributeList, error) {
	dLists, exist := channel2listArr[req.ChannelId]
	if !exist {
		return nil, errors.Errorf("Do Not have channel %+v list", req.ChannelId)
	}

	for _, dList := range dLists {
		if dList.FileSuffix == req.FileSuffix {
			return &dList, nil
		}
	}

	return nil, errors.Errorf("Do Not have file %v of channel %v list", req.FileSuffix, req.ChannelId)
}

func StoreDistributeList(channelId string, list list_puller.DistributeList) {
	if _, ok := channel2listArr[channelId]; !ok {
		channel2listArr[channelId] = make([]list_puller.DistributeList, 0)
	}
	channel2listArr[channelId] = append(channel2listArr[channelId], list)
}

func InitDistributeListServer() *DistributeListServer {
	channel2listArr = make(map[string][]list_puller.DistributeList)
	return &DistributeListServer{}
}
