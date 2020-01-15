package hotstuff

import (
	"github.com/hyperledger/fabric/orderer/consensus"
	"github.com/hyperledger/fabric/protos/common"
)

type chain struct {
	support consensus.ConsenterSupport
}

func (c chain) Order(env *common.Envelope, configSeq uint64) error {
	panic("implement me")
}

func (c chain) Configure(config *common.Envelope, configSeq uint64) error {
	panic("implement me")
}

func (c chain) WaitReady() error {
	panic("implement me")
}

func (c chain) Errored() <-chan struct{} {
	panic("implement me")
}

func (c chain) Start() {
	panic("implement me")
}

func (c chain) Halt() {
	panic("implement me")
}
