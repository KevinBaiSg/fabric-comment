/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/crypto"
	"github.com/hyperledger/fabric/orderer/common/blockcutter"
	"github.com/hyperledger/fabric/orderer/common/msgprocessor"
	cb "github.com/hyperledger/fabric/protos/common"
)

// Consenter defines the backing ordering mechanism.
type Consenter interface {
	// HandleChain should create and return a reference to a Chain for the given set of resources.
	// It will only be invoked for a given chain once per process.  In general, errors will be treated
	// as irrecoverable and cause system shutdown.  See the description of Chain for more details
	// The second argument to HandleChain is a pointer to the metadata stored on the `ORDERER` slot of
	// the last block committed to the ledger of this Chain.  For a new chain, this metadata will be
	// nil, as this field is not set on the genesis block
	// 创建共识组件链对象
	HandleChain(support ConsenterSupport, metadata *cb.Metadata) (Chain, error)
}

// Chain defines a way to inject messages for ordering.
// Note, that in order to allow flexibility in the implementation, it is the responsibility of the implementer
// to take the ordered messages, send them through the blockcutter.Receiver supplied via HandleChain to cut blocks,
// and ultimately write the ledger also supplied via HandleChain.  This design allows for two primary flows
// 1. Messages are ordered into a stream, the stream is cut into blocks, the blocks are committed (solo, kafka)
// 2. Messages are cut into blocks, the blocks are ordered, then the blocks are committed (sbft)
type Chain interface {
	// NOTE: The kafka consenter has not been updated to perform the revalidation
	// checks conditionally.  For now, Order/Configure are essentially Enqueue as before.
	// This does not cause data inconsistency, but it wastes cycles and will be required
	// to properly support the ConfigUpdate concept once introduced
	// Once this is done, the MsgClassification logic in msgprocessor should return error
	// for non ConfigUpdate/Normal msg types

	// Order accepts a message which has been processed at a given configSeq.
	// If the configSeq advances, it is the responsibility of the consenter
	// to revalidate and potentially discard the message
	// The consenter may return an error, indicating the message was not accepted
	Order(env *cb.Envelope, configSeq uint64) error	// 提交普通交易消息进行排序

	// Configure accepts a message which reconfigures the channel and will
	// trigger an update to the configSeq if committed.  The configuration must have
	// been triggered by a ConfigUpdate message. If the config sequence advances,
	// it is the responsibility of the consenter to recompute the resulting config,
	// discarding the message if the reconfiguration is no longer valid.
	// The consenter may return an error, indicating the message was not accepted
	Configure(config *cb.Envelope, configSeq uint64) error	// 提交通道配置交易消息进行通道管理

	// WaitReady blocks waiting for consenter to be ready for accepting new messages.
	// This is useful when consenter needs to temporarily block ingress messages so
	// that in-flight messages can be consumed. It could return error if consenter is
	// in erroneous states. If this blocking behavior is not desired, consenter could
	// simply return nil.
	WaitReady() error	// 检查并阻塞等待共识组件能够接收和处理新消息

	// Errored returns a channel which will close when an error has occurred.
	// This is especially useful for the Deliver client, who must terminate waiting
	// clients when the consenter is not up to date.
	Errored() <-chan struct{}	// 返回通道，当发生错误时关闭该通道

	// Start should allocate whatever resources are needed for staying up to date with the chain.
	// Typically, this involves creating a thread which reads from the ordering source, passes those
	// messages to a block cutter, and writes the resulting blocks to the ledger.
	Start()	// 启动共识组件

	// Halt frees the resources which were allocated for this Chain.
	Halt()
}

// ConsenterSupport provides the resources available to a Consenter implementation.
type ConsenterSupport interface {
	crypto.LocalSigner	// 本地签名者实体
	msgprocessor.Processor	// 消息处理器

	// BlockCutter returns the block cutting helper for this channel.
	BlockCutter() blockcutter.Receiver	// 消息打包组件

	// SharedConfig provides the shared config from the channel's current config block.
	SharedConfig() channelconfig.Orderer	// 获取通道 orderer 配置

	// CreateNextBlock takes a list of messages and creates the next block based on the block with highest block number committed to the ledger
	// Note that either WriteBlock or WriteConfigBlock must be called before invoking this method a second time.
	CreateNextBlock(messages []*cb.Envelope) *cb.Block	// 创建区块

	// WriteBlock commits a block to the ledger.
	WriteBlock(block *cb.Block, encodedMetadataValue []byte)	// 提交封装了普通交易消息的区块到账本

	// WriteConfigBlock commits a block to the ledger, and applies the config update inside.
	WriteConfigBlock(block *cb.Block, encodedMetadataValue []byte)	// 提交配置区块到账本，创建新通道或更新通道配置

	// Sequence returns the current config squence.
	Sequence() uint64 // 返回当前通道的配置序号

	// ChainID returns the channel ID this support is associated with.
	ChainID() string	// 返回关联的通道 ID

	// Height returns the number of blocks in the chain this channel is associated with.
	Height() uint64	// 返回关联的区块链结构高度
}
