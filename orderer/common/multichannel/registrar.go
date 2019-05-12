/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package multichannel tracks the channel resources for the orderer.  It initially
// loads the set of existing channels, and provides an interface for users of these
// channels to retrieve them, or create new ones.
package multichannel

import (
	"fmt"

	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/configtx"
	"github.com/hyperledger/fabric/common/crypto"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/orderer/common/msgprocessor"
	"github.com/hyperledger/fabric/orderer/consensus"
	cb "github.com/hyperledger/fabric/protos/common"
	ab "github.com/hyperledger/fabric/protos/orderer"
	"github.com/hyperledger/fabric/protos/utils"

	"github.com/op/go-logging"
	"github.com/pkg/errors"
)

const (
	pkgLogID = "orderer/commmon/multichannel"

	msgVersion = int32(0)
	epoch      = 0
)

var logger *logging.Logger

func init() {
	logger = flogging.MustGetLogger(pkgLogID)
}

// checkResources makes sure that the channel config is compatible with this binary and logs sanity checks
func checkResources(res channelconfig.Resources) error {
	channelconfig.LogSanityChecks(res)
	oc, ok := res.OrdererConfig()
	if !ok {
		return errors.New("config does not contain orderer config")
	}
	if err := oc.Capabilities().Supported(); err != nil {
		return errors.Wrapf(err, "config requires unsupported orderer capabilities: %s", err)
	}
	if err := res.ChannelConfig().Capabilities().Supported(); err != nil {
		return errors.Wrapf(err, "config requires unsupported channel capabilities: %s", err)
	}
	return nil
}

// checkResourcesOrPanic invokes checkResources and panics if an error is returned
func checkResourcesOrPanic(res channelconfig.Resources) {
	if err := checkResources(res); err != nil {
		logger.Panicf("[channel %s] %s", res.ConfigtxValidator().ChainID(), err)
	}
}

type mutableResources interface {
	channelconfig.Resources
	Update(*channelconfig.Bundle)
}

type configResources struct {
	mutableResources
}

func (cr *configResources) CreateBundle(channelID string, config *cb.Config) (*channelconfig.Bundle, error) {
	return channelconfig.NewBundle(channelID, config)
}

func (cr *configResources) Update(bndl *channelconfig.Bundle) {
	checkResourcesOrPanic(bndl)
	cr.mutableResources.Update(bndl)
}

func (cr *configResources) SharedConfig() channelconfig.Orderer {
	oc, ok := cr.OrdererConfig()
	if !ok {
		logger.Panicf("[channel %s] has no orderer configuration", cr.ConfigtxValidator().ChainID())
	}
	return oc
}

type ledgerResources struct {
	*configResources
	blockledger.ReadWriter
}

// Registrar serves as a point of access and control for the individual channel resources.
type Registrar struct {
	chains          map[string]*ChainSupport	// 链支持对象字典
	consenters      map[string]consensus.Consenter	// 共识组件字典
	ledgerFactory   blockledger.Factory	// 账本工厂对象组件
	signer          crypto.LocalSigner	// 本地签名者实体
	systemChannelID string	// 系统通道 ID
	systemChannel   *ChainSupport	// 系统通道链支持对象
	templator       msgprocessor.ChannelConfigTemplator	// 通道配置末班，用于生成消息处理器 ？？？
	callbacks       []func(bundle *channelconfig.Bundle)	// tls 认证连接回调函数列表 ？？？
}

//获取
func getConfigTx(reader blockledger.Reader) *cb.Envelope {
	// 获取最新区块
	lastBlock := blockledger.GetBlock(reader, reader.Height()-1)
	// 获取最新区块上元数据中的cb.BlockMetadataIndex_LAST_CONFIG索引项，解析获得最新配置区块的索引号 index
	index, err := utils.GetLastConfigIndexFromBlock(lastBlock)
	if err != nil {
		logger.Panicf("Chain did not have appropriately encoded last config in its latest block: %s", err)
	}
	//从区块文件中获取指定区块号index对应的最新配置区块对象
	configBlock := blockledger.GetBlock(reader, index)
	if configBlock == nil {
		logger.Panicf("Config block does not exist")
	}
	// 解析配置区块，获取第一个交易对象
	return utils.ExtractEnvelopeOrPanic(configBlock, 0)
}

// NewRegistrar produces an instance of a *Registrar.
func NewRegistrar(ledgerFactory blockledger.Factory, consenters map[string]consensus.Consenter,
	signer crypto.LocalSigner, callbacks ...func(bundle *channelconfig.Bundle)) *Registrar {
	r := &Registrar{
		chains:        make(map[string]*ChainSupport),	// 链支持对象字典
		ledgerFactory: ledgerFactory,	// 账本工厂对象
		consenters:    consenters,	// 共识组件字典
		signer:        signer,	// 本地签名者
		callbacks:     callbacks,	// 回调函数（如 tls 认证连接回调函数）
	}

	// 获取该账本工厂对象关联的现存通道 ID 列表
	existingChains := ledgerFactory.ChainIDs()
	// 循环遍历现存通道 ID 列表
	for _, chainID := range existingChains {
		// 根据通道 ID 获取或者创建指定通道上的区块账本对象
		rl, err := ledgerFactory.GetOrCreate(chainID)
		if err != nil {
			logger.Panicf("Ledger factory reported chainID %s but could not retrieve it: %s", chainID, err)
		}
		//获取该通道账本上最新的配置交易对象
		configTx := getConfigTx(rl)
		if configTx == nil {
			logger.Panic("Programming error, configTx should never be nil here")
		}
		// 解析configTx（cb.Envelope), 并根据其创建ledger资源，
		ledgerResources := r.newLedgerResources(configTx)
		// 从 ledgerResources 重新获得 chainID
		// 疑问 该 chainID 与 循环的值有何不同？？？？？？？？？？
		chainID := ledgerResources.ConfigtxValidator().ChainID()

		// 如果存在 Consortiums 配置，则说明是系统通道
		if _, ok := ledgerResources.ConsortiumsConfig(); ok {
			if r.systemChannelID != "" {
				// 如果已经设置系统通道名称，则说明已经创建了系统通道的链支持对象
				logger.Panicf("There appear to be two system chains %s and %s", r.systemChannelID, chainID)
			}
			// 创建该通道的链支持对象
			chain := newChainSupport(
				r,	// 多通道管理器
				ledgerResources, // 账本资源对象
				consenters, // 共识组件字典
				signer) // 签名者实体

			// 创建默认通道配置模板 ？？？
			// TODO: READ
			r.templator = msgprocessor.NewDefaultTemplator(chain)
			// 创建系统通道消息处理器
			// TODO: READ
			chain.Processor = msgprocessor.NewSystemChannel(chain, r.templator,
				msgprocessor.CreateSystemChannelFilters(r, chain))

			// Retrieve genesis block to log its hash. See FAB-5450 for the purpose
			iter, pos := rl.Iterator(&ab.SeekPosition{Type: &ab.SeekPosition_Oldest{Oldest: &ab.SeekOldest{}}})
			defer iter.Close()
			if pos != uint64(0) {
				logger.Panicf("Error iterating over system channel: '%s', expected position 0, got %d", chainID, pos)
			}
			// 获取创世区块并在后续日志中记录其 hash（genesisBlock.Header.Hash()）
			genesisBlock, status := iter.Next()
			if status != cb.Status_SUCCESS {
				logger.Panicf("Error reading genesis block of system channel '%s'", chainID)
			}
			logger.Infof("Starting system channel '%s' with genesis block hash %x and orderer type %s", chainID, genesisBlock.Header.Hash(), chain.SharedConfig().ConsensusType())

			// 将系统通道注册到多通道管理器（此处只会执行一次）
			r.chains[chainID] = chain	// 注册到链支持对象字典
			r.systemChannelID = chainID	// 设置系统通道 ID
			r.systemChannel = chain	//	设置系统通道的链支持对象
			// We delay starting this chain, as it might try to copy and replace the chains map via newChain before the map is fully built
			defer chain.start() // TODO: for 循环中调用 defer
		} else {
			/*
				不存在联盟配置，走该分支
				直接创建链支持对象，注册到多通道注册管理器中
			*/
			logger.Debugf("Starting chain: %s", chainID)
			chain := newChainSupport(
				r,
				ledgerResources,
				consenters,
				signer)
			r.chains[chainID] = chain
			chain.start()	// TODO: 为什么此处不需要 defer
		}
	}

	if r.systemChannelID == "" {
		logger.Panicf("No system chain found.  If bootstrapping, does your system channel contain a consortiums group definition?")
	}

	return r
}

// SystemChannelID returns the ChannelID for the system channel.
func (r *Registrar) SystemChannelID() string {
	return r.systemChannelID
}

// BroadcastChannelSupport returns the message channel header, whether the message is a config update
// and the channel resources for a message or an error if the message is not a message which can
// be processed directly (like CONFIG and ORDERER_TRANSACTION messages)
func (r *Registrar) BroadcastChannelSupport(msg *cb.Envelope) (*cb.ChannelHeader, bool, *ChainSupport, error) {
	chdr, err := utils.ChannelHeader(msg)
	if err != nil {
		return nil, false, nil, fmt.Errorf("could not determine channel ID: %s", err)
	}

	cs, ok := r.chains[chdr.ChannelId]
	if !ok {
		cs = r.systemChannel
	}

	isConfig := false
	switch cs.ClassifyMsg(chdr) {
	case msgprocessor.ConfigUpdateMsg:
		isConfig = true
	case msgprocessor.ConfigMsg:
		return chdr, false, nil, errors.New("message is of type that cannot be processed directly")
	default:
	}

	return chdr, isConfig, cs, nil
}

// GetChain retrieves the chain support for a chain (and whether it exists)
func (r *Registrar) GetChain(chainID string) (*ChainSupport, bool) {
	cs, ok := r.chains[chainID]
	return cs, ok
}

func (r *Registrar) newLedgerResources(configTx *cb.Envelope) *ledgerResources {
	//
	payload, err := utils.UnmarshalPayload(configTx.Payload)
	if err != nil {
		logger.Panicf("Error umarshaling envelope to payload: %s", err)
	}

	if payload.Header == nil {
		logger.Panicf("Missing channel header: %s", err)
	}

	chdr, err := utils.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		logger.Panicf("Error unmarshaling channel header: %s", err)
	}

	configEnvelope, err := configtx.UnmarshalConfigEnvelope(payload.Data)
	if err != nil {
		logger.Panicf("Error umarshaling config envelope from payload data: %s", err)
	}

	bundle, err := channelconfig.NewBundle(chdr.ChannelId, configEnvelope.Config)
	if err != nil {
		logger.Panicf("Error creating channelconfig bundle: %s", err)
	}

	checkResourcesOrPanic(bundle)

	ledger, err := r.ledgerFactory.GetOrCreate(chdr.ChannelId)
	if err != nil {
		logger.Panicf("Error getting ledger for %s", chdr.ChannelId)
	}

	return &ledgerResources{
		configResources: &configResources{
			mutableResources: channelconfig.NewBundleSource(bundle, r.callbacks...),
		},
		ReadWriter: ledger,
	}
}

func (r *Registrar) newChain(configtx *cb.Envelope) {
	ledgerResources := r.newLedgerResources(configtx)
	ledgerResources.Append(blockledger.CreateNextBlock(ledgerResources, []*cb.Envelope{configtx}))

	// Copy the map to allow concurrent reads from broadcast/deliver while the new chainSupport is
	newChains := make(map[string]*ChainSupport)
	for key, value := range r.chains {
		newChains[key] = value
	}

	cs := newChainSupport(r, ledgerResources, r.consenters, r.signer)
	chainID := ledgerResources.ConfigtxValidator().ChainID()

	logger.Infof("Created and starting new chain %s", chainID)

	newChains[string(chainID)] = cs
	cs.start()

	r.chains = newChains
}

// ChannelsCount returns the count of the current total number of channels.
func (r *Registrar) ChannelsCount() int {
	return len(r.chains)
}

// NewChannelConfig produces a new template channel configuration based on the system channel's current config.
func (r *Registrar) NewChannelConfig(envConfigUpdate *cb.Envelope) (channelconfig.Resources, error) {
	return r.templator.NewChannelConfig(envConfigUpdate)
}

// CreateBundle calls channelconfig.NewBundle
func (r *Registrar) CreateBundle(channelID string, config *cb.Config) (channelconfig.Resources, error) {
	return channelconfig.NewBundle(channelID, config)
}
