package blockfiletransfer

import (
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/protos/common"
	"io/ioutil"
	"path/filepath"
)

const (
	ChainsDir = "chains"
)

var logger = flogging.MustGetLogger("peer.blockfiletransfer")

type LedgerTransferService struct {
}

func (l *LedgerTransferService) LedgerTrans(request *common.Request, stream common.LedgerTransfer_LedgerTransServer) error {
	storePath := filepath.Join(ledgerconfig.GetBlockStorePath(), ChainsDir, request.ChannelId)

	fileBlockPackage := []*common.FileBlock{}
	for _, fileBlockId := range request.RequestList {
		filePath := filepath.Join(storePath, fileBlockId)
		data, err := ioutil.ReadFile(filePath)
		if err != nil {
			logger.Error("Read File Error! %+v", err)
		}
		fileBlock := &common.FileBlock{
			FileBlockId: fileBlockId,
			Content:     data,
		}
		fileBlockPackage = append(fileBlockPackage, fileBlock)
	}

	err := stream.Send(&common.FileBlockPackage{
		FileBlock: fileBlockPackage,
	})
	if err != nil {
		logger.Error("Send file block error! %+v", err)
		return err
	}
	return nil
}

func NewLedgerTransferService() *LedgerTransferService {
	return &LedgerTransferService{}
}
