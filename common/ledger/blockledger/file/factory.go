/*
Copyright IBM Corp. 2017 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

                 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package fileledger

import (
	"sync"

	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/ledger/blkstorage/fsblkstorage"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/common/metrics"
)

type fileLedgerFactory struct {
	blkstorageProvider blkstorage.BlockStoreProvider
	ledgers            map[string]blockledger.ReadWriter
	mutex              sync.Mutex
}

// GetOrCreate gets an existing ledger (if it exists) or creates it if it does not
func (flf *fileLedgerFactory) GetOrCreate(chainID string) (blockledger.ReadWriter, error) {
	flf.mutex.Lock()
	defer flf.mutex.Unlock()

	key := chainID
	// check cache
	ledger, ok := flf.ledgers[key]
	if ok {
		return ledger, nil
	}
	// open fresh
	blockStore, err := flf.blkstorageProvider.OpenBlockStore(key)
	if err != nil {
		return nil, err
	}
	ledger = NewFileLedger(blockStore)
	flf.ledgers[key] = ledger
	return ledger, nil
}

// ChainIDs returns the chain IDs the factory is aware of
func (flf *fileLedgerFactory) ChainIDs() []string {
	chainIDs, err := flf.blkstorageProvider.List()
	if err != nil {
		logger.Panic(err)
	}
	return chainIDs
}

// Close releases all resources acquired by the factory
func (flf *fileLedgerFactory) Close() {
	flf.blkstorageProvider.Close()
}

// New creates a new ledger factory
func New(directory string, metricsProvider metrics.Provider) blockledger.Factory {
	return &fileLedgerFactory{
		blkstorageProvider: fsblkstorage.NewProvider(
			fsblkstorage.NewConf(directory, -1),
			&blkstorage.IndexConfig{
				AttrsToIndex: []blkstorage.IndexableAttr{blkstorage.IndexableAttrBlockNum}},
			metricsProvider,
		),
		ledgers: make(map[string]blockledger.ReadWriter),
	}
}

// zyy 创建带监听者的账本文件工厂
type fileLedgerFactoryWithWatcher struct {
	blkstorageProviderWithWatcher blkstorage.BlockStoreProviderWithWatcher
	ledgers                       map[string]blockledger.ReadWriter
	watchers                      map[string]blkstorage.BlockFileWatcher
	mutex                         sync.Mutex
}

func (flfw *fileLedgerFactoryWithWatcher) GetOrCreate(chainID string) (blockledger.ReadWriter, error) {
	return flfw.GetOrCreateWithWatcher(chainID, nil)
}

func (flfw *fileLedgerFactoryWithWatcher) ChainIDs() []string {
	chainIDs, err := flfw.blkstorageProviderWithWatcher.List()
	if err != nil {
		logger.Panic(err)
	}
	return chainIDs
}

func (flfw *fileLedgerFactoryWithWatcher) Close() {
	flfw.blkstorageProviderWithWatcher.Close()
}

func NewWithWathcer(directory string, metricsProvider metrics.Provider) blockledger.FactoryWithWatcher {
	return &fileLedgerFactoryWithWatcher{
		ledgers: make(map[string]blockledger.ReadWriter),
		blkstorageProviderWithWatcher: fsblkstorage.NewProviderWithWatcher(
			fsblkstorage.NewConf(directory, -1),
			&blkstorage.IndexConfig{
				AttrsToIndex: []blkstorage.IndexableAttr{blkstorage.IndexableAttrBlockNum}},
			metricsProvider,
		),
	}
}

func (flfw *fileLedgerFactoryWithWatcher) GetOrCreateWithWatcher(chainID string, watcher blkstorage.BlockFileWatcher) (blockledger.ReadWriter, error) {
	flfw.mutex.Lock()
	defer flfw.mutex.Unlock()

	var blockStore blkstorage.BlockStore
	var err error
	key := chainID
	// check cache
	ledger, ok := flfw.ledgers[key]
	if ok {
		return ledger, nil
	}
	_, ok = flfw.watchers[key]
	if ok {
		// open fresh
		blockStore, err = flfw.blkstorageProviderWithWatcher.OpenBlockStoreWithWatcher(key, nil)
	} else {
		// open fresh
		blockStore, err = flfw.blkstorageProviderWithWatcher.OpenBlockStoreWithWatcher(key, watcher)
	}
	if err != nil {
		return nil, err
	}
	ledger = NewFileLedger(blockStore)
	flfw.ledgers[key] = ledger
	flfw.watchers[key] = watcher

	return ledger, nil
}
