package blockfilewatcher

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/configtx"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/klauspost/reedsolomon"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/hyperledger/fabric/protos/common"
	list_puller "github.com/hyperledger/fabric/protos/listpuller"
	"github.com/hyperledger/fabric/protos/utils"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"math/rand"
	"time"
)

var logger = flogging.MustGetLogger("peer.blockfilewathcer")

type PeerBlockFileWatcher struct {
	ChannelId       string
	OrererAddresses []string
}

func (bfpw *PeerBlockFileWatcher) BlockFileFull(suffixNum int) {
	if len(bfpw.OrererAddresses) == 0 {
		logger.Errorf("Must set orderer address first!")
		return
	}

	for {
		ordererAddress := bfpw.OrererAddresses[rand.Intn(len(bfpw.OrererAddresses))]
		conn, err := grpc.Dial(ordererAddress, grpc.WithInsecure())
		if err != nil {
			logger.Warnf("Connect to orderer %v failed, try to connect other orderer again!", ordererAddress)
			time.Sleep(5 * time.Second)
			continue
		}
		client := list_puller.NewPullListServiceClient(conn)
		distributeList, err := client.GetDistributeList(context.Background(), &list_puller.Request{
			ChannelId:  bfpw.ChannelId,
			FileSuffix: fmt.Sprintf("%06d", suffixNum),
		})
		if err != nil {
			logger.Errorf("Get distribute list failed! %+v", err.Error())
			break
		}
		blockfileEncoder := GetBlockFileEncoderInstance()
		err = blockfileEncoder.StartEncodeBlockFile(suffixNum, distributeList)
		if err != nil {
			logger.Errorf("Encode block file error: %+v", err.Error())
			break
		}
		break
	}
}

func (bfpw *PeerBlockFileWatcher) SetOrdererAddressFromGenesisBlock(genesisblock *common.Block) error {
	env := &common.Envelope{}
	err := proto.Unmarshal(genesisblock.Data.Data[0], env)
	if err != nil {
		return err
	}

	payload, err := utils.UnmarshalPayload(env.Payload)
	if err != nil {
		return err
	}

	configEnvelope, err := configtx.UnmarshalConfigEnvelope(payload.Data)
	if err != nil {
		return err
	}

	ordererAddressesConfig := &common.OrdererAddresses{}
	globalOrderers := configEnvelope.Config.ChannelGroup.Values["OrdererAddresses"]
	if err := proto.Unmarshal(globalOrderers.Value, ordererAddressesConfig); err != nil {
		return errors.Wrap(err, "failed unmarshaling orderer addresses")
	}

	bfpw.OrererAddresses = ordererAddressesConfig.Addresses
	return nil
}

func NewPeerBlockFileWatcher(channelId string) *PeerBlockFileWatcher {
	return &PeerBlockFileWatcher{
		ChannelId:       channelId,
		OrererAddresses: []string{},
	}
}

type BlockFileEncoder struct {
	mspId          string
	blockStorePath string
}

var blockFileEncoderInst BlockFileEncoder
var once sync.Once

func (s *BlockFileEncoder) StartEncodeBlockFile(suffixNum int, distributeList *list_puller.DistributeList) error {
	return encodeBlockfile(suffixNum, distributeList, s.mspId, s.blockStorePath)
}

func encodeBlockfile(suffixNum int, distributeList *list_puller.DistributeList, mspId string, blockStorePath string) error {
	blockFile := filepath.Join(blockStorePath, distributeList.ChannelId, fmt.Sprintf("blockfile_%06d", suffixNum))
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
		fileBlockPath := filepath.Join(blockStorePath, distributeList.ChannelId, fmt.Sprintf("%s_%06d_%d", distributeList.ChannelId, suffixNum, v))
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

func InitBlockFileEncoder(mspId string, path string) {
	once.Do(func() {
		blockFileEncoderInst = BlockFileEncoder{
			mspId:          mspId,
			blockStorePath: path,
		}
	})
}

func GetBlockFileEncoderInstance() BlockFileEncoder {
	return blockFileEncoderInst
}
