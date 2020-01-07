package distributeliststore

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	list_puller "github.com/hyperledger/fabric/protos/listpuller"
	"sync"
)

type DistributeListDBProvider struct {
	dbProvider *leveldbhelper.Provider
}

var (
	provider *DistributeListDBProvider
	once     sync.Once
)

func InitDistributeListDBProvider() {
	dbPath := ledgerconfig.GetDistributeListLevelDBPath()
	once.Do(func() {
		provider = &DistributeListDBProvider{
			dbProvider: leveldbhelper.NewProvider(&leveldbhelper.Conf{DBPath: dbPath}),
		}
	})
}

func GetDistributeListDBProviderInst() *DistributeListDBProvider {
	return provider
}

// GetDBHandle gets the handle to a named database
func (provider *DistributeListDBProvider) GetDBHandle(dbName string) (*DistributeListDB, error) {
	return newDistributeListDB(provider.dbProvider.GetDBHandle(dbName), dbName), nil
}

// Close closes the underlying db
func (provider *DistributeListDBProvider) Close() {
	provider.dbProvider.Close()
}

type DistributeListDB struct {
	db     *leveldbhelper.DBHandle
	dbName string
}

func newDistributeListDB(db *leveldbhelper.DBHandle, dbName string) *DistributeListDB {
	return &DistributeListDB{
		db:     db,
		dbName: dbName,
	}
}

func (dldb *DistributeListDB) CommitDistributeList(list *list_puller.DistributeList) error {
	key := fmt.Sprintf("%s_%s", list.ChannelId, list.FileSuffix)
	bList, err := proto.Marshal(list)
	if err != nil {
		return err
	}
	err = dldb.db.Put([]byte(key), bList, true)
	if err != nil {
		return err
	}
	return nil
}

func (dldb *DistributeListDB) GetDistributeList(key string) (*list_puller.DistributeList, error) {
	bList, err := dldb.db.Get([]byte(key))
	if err != nil {
		return nil, err
	}
	list := &list_puller.DistributeList{}
	err = proto.Unmarshal(bList, list)
	if err != nil {
		return nil, err
	}
	return list, nil
}
