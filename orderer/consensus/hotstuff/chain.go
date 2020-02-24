package hotstuff

import (
	"code.cloudfoundry.org/clock"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/orderer/common/cluster"
	"github.com/hyperledger/fabric/orderer/consensus"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/orderer/hotstuff"
	"go.etcd.io/etcd/raft"
	"time"
)

// Configurator is used to configure the communication layer
// when the Chain starts.
type Configurator interface {
	Configure(channel string, newNodes []cluster.RemoteNode)
}

type Options struct {
	HotstuffID uint64

	Clock clock.Clock

	Logger *flogging.FabricLogger

	BlockMetadata *hotstuff.BlockMetadata
	Consenters    map[uint64]*hotstuff.Consenter

	MigrationInit bool

	TickInterval time.Duration

	Cert []byte
}

type Chain struct {
	support consensus.ConsenterSupport
}

func NewChain(
	support consensus.ConsenterSupport,
	opts Options,
	conf Configurator,
	haltCallback func()) (*Chain, error) {

}

func (c Chain) Order(env *common.Envelope, configSeq uint64) error {
	panic("implement me")
}

func (c Chain) Configure(config *common.Envelope, configSeq uint64) error {
	panic("implement me")
}

func (c Chain) WaitReady() error {
	panic("implement me")
}

func (c Chain) Errored() <-chan struct{} {
	panic("implement me")
}

func (c Chain) Start() {
	panic("implement me")
}

func (c Chain) Halt() {
	panic("implement me")
}
