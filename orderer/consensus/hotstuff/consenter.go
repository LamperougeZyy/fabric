package hotstuff

import (
	"bytes"
	"encoding/pem"
	"fmt"
	"github.com/go-hotstuff/crypto"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/core/comm"
	"github.com/hyperledger/fabric/orderer/common/cluster"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/hyperledger/fabric/orderer/common/multichannel"
	"github.com/hyperledger/fabric/orderer/consensus"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/orderer"
	"github.com/hyperledger/fabric/protos/orderer/hotstuff"
	"github.com/kilic/bls12-381/blssig"
	"github.com/pkg/errors"
	"io/ioutil"
)

// CreateChainCallback creates a new chain
type CreateChainCallback func()

// InactiveChainRegistry registers chains that are inactive
type InactiveChainRegistry interface {
	// TrackChain tracks a chain with the given name, and calls the given callback
	// when this chain should be created.
	TrackChain(chainName string, genesisBlock *common.Block, createChain CreateChainCallback)
}

type Consenter struct {
	Logger                *flogging.FabricLogger
	OrdererConfig         localconfig.TopLevel
	Cert                  []byte
	InactiveChainRegistry InactiveChainRegistry
	CreateChain           func(chainName string)
	Communication         cluster.Communicator

	Signer   *crypto.BLS12381Signer
	Verifier *crypto.BLS12381Verifier
}

func (c *Consenter) OnConsensus(channel string, sender uint64, req *orderer.ConsensusRequest) error {
	panic("implement me")
}

func (c *Consenter) OnSubmit(channel string, sender uint64, req *orderer.SubmitRequest) error {
	panic("implement me")
}

func (c *Consenter) TargetChannel(message proto.Message) string {
	panic("implement me")
}

func New(
	clusterDialer *cluster.PredicateDialer,
	conf *localconfig.TopLevel,
	srvConf comm.ServerConfig,
	srv *comm.GRPCServer,
	r *multichannel.Registrar,
	icr InactiveChainRegistry,
	metricsProvider metrics.Provider,
) *Consenter {
	logger := flogging.MustGetLogger("orderer.consensus.hotstuff")

	consenter := &Consenter{
		Logger:                logger,
		OrdererConfig:         *conf,
		Cert:                  srvConf.SecOpts.Certificate,
		InactiveChainRegistry: icr,
		CreateChain:           r.CreateChain,
	}

	comm := createComm(clusterDialer, consenter, conf.General.Cluster, metricsProvider)
	consenter.Communication = comm
	svc := &cluster.Service{
		CertExpWarningThreshold:          conf.General.Cluster.CertExpirationWarningThreshold,
		MinimumExpirationWarningInterval: cluster.MinimumExpirationWarningInterval,
		StreamCountReporter: &cluster.StreamCountReporter{
			Metrics: comm.Metrics,
		},
		StepLogger: flogging.MustGetLogger("orderer.common.cluster.step"),
		Logger:     flogging.MustGetLogger("orderer.common.cluster"),
		Dispatcher: comm,
	}
	orderer.RegisterClusterServer(srv.Server(), svc)

	signer, err := generateBLSSigner(conf)
	if err != nil {
		logger.Errorf("Generate bls signer error: %v", err.Error())
	}
	consenter.Signer = signer

	// 接下来需要初始化verifier

	return consenter
}

func generateBLSSigner(conf *localconfig.TopLevel) (*crypto.BLS12381Signer, error) {
	keyStorePath := conf.General.BCCSP.SwOpts.FileKeystore.KeyStorePath
	keyFile, err := ioutil.ReadDir(keyStorePath)
	if err != nil {
		return nil, err
	}
	if len(keyFile) != 1 {
		return nil, fmt.Errorf("privite key number error! need 1 but get %d", len(keyFile))
	}
	keyFileBytes, err := ioutil.ReadFile(keyFile[0].Name())
	if err != nil {
		return nil, err
	}
	priv, err := blssig.SecretKeyFromBytes(keyFileBytes)
	if err != nil {
		return nil, err
	}
	return crypto.NewBLS12381Signer(priv), nil
}

func createComm(clusterDialer *cluster.PredicateDialer, consenter *Consenter, config localconfig.Cluster, metricsProvider metrics.Provider) *cluster.Comm {
	metrics := cluster.NewMetrics(metricsProvider)
	comm := &cluster.Comm{
		MinimumExpirationWarningInterval: cluster.MinimumExpirationWarningInterval,
		CertExpWarningThreshold:          config.CertExpirationWarningThreshold,
		SendBufferSize:                   config.SendBufferSize,
		Logger:                           flogging.MustGetLogger("orderer.common.cluster"),
		Chan2Members:                     make(map[string]cluster.MemberMapping),
		Connections:                      cluster.NewConnectionStore(clusterDialer, metrics.EgressTLSConnectionCount),
		Metrics:                          metrics,
		ChanExt:                          consenter,
		H:                                consenter,
	}
	consenter.Communication = comm
	return comm
}

func (c *Consenter) HandleChain(support consensus.ConsenterSupport, metadata *common.Metadata) (consensus.Chain, error) {
	//m := &hotstuff.ConfigMetadata{}
	//if err := proto.Unmarshal(support.SharedConfig().ConsensusMetadata(), m); err != nil {
	//	return nil, errors.Wrap(err, "failed to unmarshal consensus metadata")
	//}
	//
	//if m.Options == nil {
	//	return nil, errors.New("etcdraft options have not been provided")
	//}
	//
	//isMigration := (metadata == nil || len(metadata.Value) == 0) && (support.Height() > 1)
	//if isMigration {
	//	c.Logger.Debugf("Block metadata is nil at block height=%d, it is consensus-type migration", support.Height())
	//}
	//
	//// determine hotstuff replica set mapping for each node to its id
	//// for newly started chain we need to read and initialize raft
	//// metadata by creating mapping between conseter and its id.
	//// In case chain has been restarted we restore raft metadata
	//// information from the recently committed block meta data
	//// field.
	//blockMetadata, err := ReadBlockMetadata(metadata, m)
	//if err != nil {
	//	return nil, errors.Wrapf(err, "failed to read Raft metadata")
	//}
	//
	//consenters := map[uint64]*hotstuff.Consenter{}
	//for i, consenter := range m.Consenters {
	//	consenters[blockMetadata.ConsenterIds[i]] = consenter
	//}
	//
	//id, err := c.detectSelfID(consenters)
	//if err != nil {
	//	c.InactiveChainRegistry.TrackChain(support.ChainID(), support.Block(0), func() {
	//		c.CreateChain(support.ChainID())
	//	})
	//	return &inactive.Chain{Err: errors.Errorf("channel %s is not serviced by me", support.ChainID())}, nil
	//}
	//
	//// zyy: 设置节点的驱逐时间
	////var evictionSuspicion time.Duration
	////if c.EtcdRaftConfig.EvictionSuspicion == "" {
	////	c.Logger.Infof("EvictionSuspicion not set, defaulting to %v", DefaultEvictionSuspicion)
	////	evictionSuspicion = DefaultEvictionSuspicion
	////} else {
	////	evictionSuspicion, err = time.ParseDuration(c.EtcdRaftConfig.EvictionSuspicion)
	////	if err != nil {
	////		c.Logger.Panicf("Failed parsing Consensus.EvictionSuspicion: %s: %v", c.EtcdRaftConfig.EvictionSuspicion, err)
	////	}
	////}
	//
	//tickInterval, err := time.ParseDuration(m.Options.TickInterval)
	//if err != nil {
	//	return nil, errors.Errorf("failed to parse TickInterval (%s) to time duration", m.Options.TickInterval)
	//}
	//
	//opts := Options{
	//	HotstuffID: id,
	//	Clock:      clock.NewClock(),
	//	//MemoryStorage: raft.NewMemoryStorage(),
	//	Logger: c.Logger,
	//
	//	TickInterval: tickInterval,
	//	//ElectionTick:         int(m.Options.ElectionTick),
	//	//HeartbeatTick:        int(m.Options.HeartbeatTick),
	//	//MaxInflightBlocks:    int(m.Options.MaxInflightBlocks),
	//	//MaxSizePerMsg:        uint64(support.SharedConfig().BatchSize().PreferredMaxBytes),
	//	//SnapshotIntervalSize: m.Options.SnapshotIntervalSize,
	//
	//	BlockMetadata: blockMetadata,
	//	Consenters:    consenters,
	//
	//	MigrationInit: isMigration,
	//
	//	//WALDir:            path.Join(c.EtcdRaftConfig.WALDir, support.ChainID()),
	//	//SnapDir:           path.Join(c.EtcdRaftConfig.SnapDir, support.ChainID()),
	//	//EvictionSuspicion: evictionSuspicion,
	//	Cert: c.Cert,
	//	//Metrics:           c.Metrics,
	//}
	//
	//rpc := &cluster.RPC{
	//	Timeout:       c.OrdererConfig.General.Cluster.RPCTimeout,
	//	Logger:        c.Logger,
	//	Channel:       support.ChainID(),
	//	Comm:          c.Communication,
	//	StreamsByType: cluster.NewStreamsByType(),
	//}
	//return NewChain(
	//	support,
	//	opts,
	//)
	return nil, nil
}

// ReadBlockMetadata attempts to read raft metadata from block metadata, if available.
// otherwise, it reads raft metadata from config metadata supplied.
// zyy 先从block metadata中读数据，再从config metadata中读数据，因为要先保证这个不是迁移过来的，读config metadata 表示这个是新开的chain
func ReadBlockMetadata(blockMetadata *common.Metadata, configMetadata *hotstuff.ConfigMetadata) (*hotstuff.BlockMetadata, error) {
	if blockMetadata != nil && len(blockMetadata.Value) != 0 { // we have consenters mapping from block
		m := &hotstuff.BlockMetadata{}
		if err := proto.Unmarshal(blockMetadata.Value, m); err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal block's metadata")
		}
		return m, nil
	}

	m := &hotstuff.BlockMetadata{
		NextConsenterId: 1,
		ConsenterIds:    make([]uint64, len(configMetadata.Consenters)),
	}
	// need to read consenters from the configuration
	for i := range m.ConsenterIds {
		m.ConsenterIds[i] = m.NextConsenterId
		m.NextConsenterId++
	}

	return m, nil
}

// zyy: 这个函数是根据证书来判断自己是否服务于当前channel的排序
func (c *Consenter) detectSelfID(consenters map[uint64]*hotstuff.Consenter) (uint64, error) {
	thisNodeCertAsDER, err := pemToDER(c.Cert, 0, "server", c.Logger)
	if err != nil {
		return 0, err
	}

	var serverCertificates []string
	for nodeID, cst := range consenters {
		serverCertificates = append(serverCertificates, string(cst.ServerTlsCert))

		certAsDER, err := pemToDER(cst.ServerTlsCert, nodeID, "server", c.Logger)
		if err != nil {
			return 0, err
		}

		if bytes.Equal(thisNodeCertAsDER, certAsDER) {
			return nodeID, nil
		}
	}

	c.Logger.Warning("Could not find", string(c.Cert), "among", serverCertificates)
	return 0, cluster.ErrNotInChannel
}

func pemToDER(pemBytes []byte, id uint64, certType string, logger *flogging.FabricLogger) ([]byte, error) {
	bl, _ := pem.Decode(pemBytes)
	if bl == nil {
		logger.Errorf("Rejecting PEM block of %s TLS cert for node %d, offending PEM is: %s", certType, id, string(pemBytes))
		return nil, errors.Errorf("invalid PEM block")
	}
	return bl.Bytes, nil
}
