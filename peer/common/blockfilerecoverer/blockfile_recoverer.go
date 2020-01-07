package blockfilerecoverer

import (
	"context"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blkstorage/fsblkstorage"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/peer/common/blockfilerecoverer/distributeliststore"
	"github.com/hyperledger/fabric/peer/common/blockfilewatcher"
	"github.com/hyperledger/fabric/protos/common"
	list_puller "github.com/hyperledger/fabric/protos/listpuller"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/klauspost/reedsolomon"
	"google.golang.org/grpc"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"time"
)

var (
	logger = flogging.MustGetLogger("peer.blockfilerecoverer")
)

const ChainsDir = "chains"

type PeerBlockfileRecoverer struct {
	channelId string
}

func (recoverer *PeerBlockfileRecoverer) BlockFileRecover(suffixNum string) {
	//zyy 开始文件恢复
	provider := distributeliststore.GetDistributeListDBProviderInst()
	distributeListDB, err := provider.GetDBHandle(recoverer.channelId)
	if err != nil {
		logger.Errorf("Get distribute list level db error! %+v", err)
		return
	}
	distributeList, err := distributeListDB.GetDistributeList(fmt.Sprintf("%s_%s", recoverer.channelId, suffixNum))

	mspId := blockfilewatcher.GetBlockFileEncoderInstance().GetMSPId()
	requestMap := make(map[*list_puller.LocationInfo][]string)
	for fileBlockId, locInfo := range distributeList.Item {
		if _, ok := requestMap[locInfo]; mspId != locInfo.OrgName && !ok {
			requestMap[locInfo] = []string{}
			requestMap[locInfo] = append(requestMap[locInfo], fileBlockId)
		} else if ok {
			requestMap[locInfo] = append(requestMap[locInfo], fileBlockId)
		}
	}

	fileDatas := map[string][]byte{}
	for locInfo, requestList := range requestMap {
		anchorPeers := &peer.AnchorPeers{}
		err := proto.Unmarshal(locInfo.AnchorPeers, anchorPeers)
		if err != nil {
			logger.Errorf("unmarshal anchor peer failed! %+v", err)
			return
		}
		for _, anchorPeer := range anchorPeers.AnchorPeers {
			endPoint := net.JoinHostPort(anchorPeer.Host, fmt.Sprintf("%d", anchorPeer.Port))
			conn, err := grpc.Dial(endPoint, grpc.WithInsecure())
			if err != nil {
				logger.Warnf("Connect to peer %s failed ! wait 5 second to connect to other peer", endPoint)
				time.Sleep(5 * time.Second)
				continue
			}
			client := common.NewLedgerTransferClient(conn)
			stream, err := client.LedgerTrans(context.Background(), &common.Request{
				FileSuffix:  suffixNum,
				ChannelId:   recoverer.channelId,
				RequestList: requestList,
			})
			if err != nil {
				logger.Warn("Get file transfer stream error ! %+v, connect to another anchor peer", err)
				continue
			}
			fileBlockPackage, err := stream.Recv()
			if err != nil || len(fileBlockPackage.FileBlock) != len(requestList) {
				logger.Warn("Get file block form %s error! try to fetch from another peer", endPoint)
				continue
			}

			for _, fileBlock := range fileBlockPackage.FileBlock {
				if _, ok := fileDatas[fileBlock.FileBlockId]; !ok {
					fileDatas[fileBlock.FileBlockId] = []byte{}
				}
				fileDatas[fileBlock.FileBlockId] = fileBlock.Content
			}
			break
		}
	}

	err = recoverer.startRecover(distributeList, fileDatas, suffixNum)
	if err != nil {
		logger.Error("Recover block file failed! %+v", err)
		return
	}
}

// zyy: 开始恢复区块文件
func (recoverer *PeerBlockfileRecoverer) startRecover(distributelist *list_puller.DistributeList, fileDatas map[string][]byte, suffixNum string) error {
	storePath := filepath.Join(ledgerconfig.GetBlockStorePath(), ChainsDir, distributelist.ChannelId)
	dataBuf := make([][]byte, distributelist.DataShards+distributelist.ParShards)
	for i := 0; int32(i) < distributelist.DataShards+distributelist.ParShards; i++ {
		fileBlockId := fmt.Sprintf("%s_%s_%d", distributelist.ChannelId, suffixNum, i)
		if _, ok := fileDatas[fileBlockId]; !ok {
			filePath := filepath.Join(storePath, fileBlockId)
			data, err := ioutil.ReadFile(filePath)
			if err != nil {
				return err
			}
			dataBuf[i] = data
		} else {
			dataBuf[i] = fileDatas[fileBlockId]
		}
	}

	encoder, err := reedsolomon.New(int(distributelist.DataShards), int(distributelist.ParShards))
	if err != nil {
		return err
	}
	encoder.Reconstruct(dataBuf)
	blockFile, err := os.Create(filepath.Join(storePath, fmt.Sprintf("blockfile_%s", suffixNum)))
	if err != nil {
		return err
	}
	err = encoder.Join(blockFile, dataBuf, len(dataBuf[0])*int(distributelist.DataShards))
	if err != nil {
		panic(err)
	}
	blockFile.Close()
	return nil
}

func InitBlockFileRecoverer(channelId string) {
	recoverer := &PeerBlockfileRecoverer{
		channelId: channelId,
	}
	fsblkstorage.GetBlockFileRecoverMgrInst().RegistRecoverer(channelId, recoverer)
}
