package hotstuff

import (
	"code.cloudfoundry.org/clock"
	"fmt"
	"github.com/go-hotstuff"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/orderer/common/cluster"
	orderer_consensus "github.com/hyperledger/fabric/orderer/consensus"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/orderer"
	orderer_hotstuff "github.com/hyperledger/fabric/protos/orderer/hotstuff"
	"github.com/pkg/errors"
	"sync"
	"time"
)

const NONE uint64 = 0

// Configurator is used to configure the communication layer
// when the Chain starts.
type Configurator interface {
	Configure(channel string, newNodes []cluster.RemoteNode)
}

type RPC interface {
	SendConsensus(dest uint64, msg *orderer.ConsensusRequest) error
	SendSubmit(dest uint64, request *orderer.SubmitRequest) error
}

// BlockPuller is used to pull blocks from other OSN
type BlockPuller interface {
	PullBlock(seq uint64) *common.Block
	HeightsByEndpoints() (map[string]uint64, error)
	Close()
}

type CreateBlockPuller func() (BlockPuller, error)

type Options struct {
	HotstuffID uint64

	Clock clock.Clock

	Logger *flogging.FabricLogger

	BlockMetadata *orderer_hotstuff.BlockMetadata
	Consenters    map[uint64]*orderer_hotstuff.Consenter

	MigrationInit bool

	TickInterval time.Duration

	Metrics *Metrics
	Cert []byte
}

type Chain struct {
	configurator Configurator

	rpc RPC

	hotstuffID uint64
	channelID  string

	submitC chan *submit
	applyC  chan apply
	haltC   chan struct{}
	doneC   chan struct{}
	startC  chan struct{}

	errorCLock sync.RWMutex
	errorC     chan struct{}

	hotstuffMetadataLock sync.RWMutex

	support orderer_consensus.ConsenterSupport

	lastBlock    *common.Block
	appliedIndex uint64

	createPuller CreateBlockPuller

	fresh bool

	Node *Node
	opts Options

	Metrics *Metrics
	logger  *flogging.FabricLogger

	haltCallback func()
}

func (c *Chain) Consensus(req *orderer.ConsensusRequest, sender uint64) error {
	panic("implement me")
}

func (c *Chain) Submit(req *orderer.SubmitRequest, sender uint64) error {
	if err := c.isRunning(); err != nil {
		c.Metrics.ProposalFailures.Add(1)
		return err
	}

	leadC := make(chan uint64, 1)
	select {
	case c.submitC <- &submit{req: req, leader: leadC}:
		lead := <-leadC
		if lead == NONE {
			c.Metrics.ProposalFailures.Add(1)
			return errors.Errorf("no hotstuff leader")
		}

		if lead != c.hotstuffID {
			if err := c.rpc.SendSubmit(lead, req); err != nil {
				c.Metrics.ProposalFailures.Add(1)
				return err
			}
		}

	case <-c.doneC:
		c.Metrics.ProposalFailures.Add(1)
		return errors.Errorf("chain is stopped")
	}

	return nil
}

type submit struct {
	req    *orderer.SubmitRequest
	leader chan uint64
}

type apply struct {
	entries []orderer_hotstuff.Message
}

func NewChain(
	support orderer_consensus.ConsenterSupport,
	opts Options,
	conf Configurator,
	rpc RPC,
	f CreateBlockPuller,
	haltCallback func()) (*Chain, error) {

	lg := opts.Logger.With("channel", support.ChainID(), "node", opts.HotstuffID)

	b := support.Block(support.Height() - 1)
	if b == nil {
		return nil, errors.Errorf("failed to get last block")
	}

	c := &Chain{
		configurator: conf,
		rpc:          rpc,
		hotstuffID:   opts.HotstuffID,
		channelID:    support.ChainID(),
		submitC:      make(chan *submit),
		applyC:       make(chan apply),
		haltC:        make(chan struct{}),
		doneC:        make(chan struct{}),
		startC:       make(chan struct{}),
		errorC:       make(chan struct{}),
		support:      support,
		lastBlock:    b,
		appliedIndex: opts.BlockMetadata.RaftIndex,
		createPuller: f,
		opts:         opts,
		logger:       lg,
		haltCallback: haltCallback,
		Metrics: &Metrics{
			ClusterSize:             opts.Metrics.ClusterSize.With("channel", support.ChainID()),
			IsLeader:                opts.Metrics.IsLeader.With("channel", support.ChainID()),
			CommittedBlockNumber:    opts.Metrics.CommittedBlockNumber.With("channel", support.ChainID()),
			SnapshotBlockNumber:     opts.Metrics.SnapshotBlockNumber.With("channel", support.ChainID()),
			LeaderChanges:           opts.Metrics.LeaderChanges.With("channel", support.ChainID()),
			ProposalFailures:        opts.Metrics.ProposalFailures.With("channel", support.ChainID()),
			DataPersistDuration:     opts.Metrics.DataPersistDuration.With("channel", support.ChainID()),
			NormalProposalsReceived: opts.Metrics.NormalProposalsReceived.With("channel", support.ChainID()),
			ConfigProposalsReceived: opts.Metrics.ConfigProposalsReceived.With("channel", support.ChainID()),
		},
	}

	c.Metrics.ClusterSize.Set(float64(len(c.opts.BlockMetadata.ConsenterIds)))
	c.Metrics.IsLeader.Set(float64(0)) // all nodes start out as followers
	c.Metrics.CommittedBlockNumber.Set(float64(c.lastBlock.Header.Number))
	c.Metrics.SnapshotBlockNumber.Set(float64(c.lastSnapBlockNum))


	c.Node = &Node{
		chainID: c.channelID,
		chain: c,
		logger: c.logger,
		metrics: c.Metrics,
		store: ,
		rpc: c.rpc,
		conf:Config{},
		tickInterval: c.opts.TickInterval,
		metadata: c.opts.BlockMetadata,
	}

	return c, nil
}

func (c *Chain) Order(env *common.Envelope, configSeq uint64) error {
	c.Metrics.NormalProposalsReceived.Add(1)
	return c.Submit(&orderer.SubmitRequest{LastValidationSeq: configSeq, Payload: env, Channel: c.channelID}, 0)
}

func (c *Chain) Configure(env *common.Envelope, configSeq uint64) error {
	c.Metrics.ConfigProposalsReceived.Add(1)
	if err := c.checkConfigUpdateValidity(env); err != nil {
		c.logger.Warnf("Rejected config: %s", err)
		c.Metrics.ProposalFailures.Add(1)
		return err
	}
	return c.Submit(&orderer.SubmitRequest{LastValidationSeq: configSeq, Payload: env, Channel: c.channelID}, 0)
}

func (c *Chain) WaitReady() error {
	if err := c.isRunning(); err != nil {
		return err
	}

	select {
	case c.submitC <- nil:
	case <-c.doneC:
		return errors.Errorf("chain is stopped")
	}

	return nil
}

func (c *Chain) Errored() <-chan struct{} {
	c.errorCLock.RLock()
	defer c.errorCLock.RUnlock()
	return c.errorC
}

func (c *Chain) Start() {
	c.logger.Infof("Starting hotstuff node")

	if err := c.configureComm(); err != nil {
		c.logger.Errorf("Failed to start chain, aborting: +%v", err)
		close(c.doneC)
		return
	}

	isJoin := c.support.Height() > 1
	if isJoin && c.opts.MigrationInit {
		isJoin = false
		c.logger.Infof("Consensus-type migration detected, starting new raft node on an existing channel; height=%d", c.support.Height())
	}
	c.Node.Start(c.fresh, isJoin)

	close(c.startC)
	close(c.errorC)

	//go c.gc()
	go c.serveRequest()

	//es := c.newEvictionSuspector()
	//
	//interval := DefaultLeaderlessCheckInterval
	//if c.opts.LeaderCheckInterval != 0 {
	//	interval = c.opts.LeaderCheckInterval
	//}
	//
	//c.periodicChecker = &PeriodicCheck{
	//	Logger:        c.logger,
	//	Report:        es.confirmSuspicion,
	//	CheckInterval: interval,
	//	Condition:     c.suspectEviction,
	//}
	//c.periodicChecker.Run()
}

func (c *Chain) configureComm() error {
	// Reset unreachable map when communication is reconfigured
	c.Node.unreachableLock.Lock()
	c.Node.unreachable = make(map[uint64]struct{})
	c.Node.unreachableLock.Unlock()

	nodes, err := c.remotePeers()
	if err != nil {
		return err
	}

	c.configurator.Configure(c.channelID, nodes)
	return nil
}

func (c *Chain) remotePeers() ([]cluster.RemoteNode, error) {
	c.hotstuffMetadataLock.RLock()
	defer c.hotstuffMetadataLock.RUnlock()

	var nodes []cluster.RemoteNode
	for hotstuffID, consenter := range c.opts.Consenters {
		// No need to know yourself
		if hotstuffID == c.hotstuffID {
			continue
		}
		serverCertAsDER, err := pemToDER(consenter.ServerTlsCert, hotstuffID, "server", c.logger)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		clientCertAsDER, err := pemToDER(consenter.ClientTlsCert, hotstuffID, "client", c.logger)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		nodes = append(nodes, cluster.RemoteNode{
			ID:            hotstuffID,
			Endpoint:      fmt.Sprintf("%s:%d", consenter.Host, consenter.Port),
			ServerTLSCert: serverCertAsDER,
			ClientTLSCert: clientCertAsDER,
		})
	}
	return nodes, nil
}

func (c *Chain) Halt() {
	select {
	case <-c.startC:
	default:
		c.logger.Warnf("Attempted to halt a chain that has not started")
		return
	}

	select {
	case c.haltC <- struct{}{}:
	case <-c.doneC:
		return
	}
	<-c.doneC

	if c.haltCallback != nil {
		c.haltCallback()
	}
}

func (c *Chain) isRunning() error {
	select {
	case <-c.startC:
	default:
		return errors.Errorf("chain is not started")
	}

	select {
	case <-c.doneC:
		return errors.Errorf("chain is stopped")
	default:
	}

	return nil
}

func (c *Chain) serveRequest() {
	for{
		select {

		}
	}
}

func (c *Chain) checkConfigUpdateValidity(ctx *common.Envelope) error {

}