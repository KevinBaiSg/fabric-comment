/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package server

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	_ "net/http/pprof" // This is essentially the main package for the orderer

	"os"
	"time"

	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/crypto"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/common/tools/configtxgen/encoder"
	genesisconfig "github.com/hyperledger/fabric/common/tools/configtxgen/localconfig"
	"github.com/hyperledger/fabric/core/comm"
	"github.com/hyperledger/fabric/msp"
	"github.com/hyperledger/fabric/orderer/common/bootstrap/file"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/hyperledger/fabric/orderer/common/metadata"
	"github.com/hyperledger/fabric/orderer/common/multichannel"
	"github.com/hyperledger/fabric/orderer/consensus"
	"github.com/hyperledger/fabric/orderer/consensus/kafka"
	"github.com/hyperledger/fabric/orderer/consensus/solo"
	cb "github.com/hyperledger/fabric/protos/common"
	ab "github.com/hyperledger/fabric/protos/orderer"
	"github.com/hyperledger/fabric/protos/utils"

	"github.com/hyperledger/fabric/common/localmsp"
	"github.com/hyperledger/fabric/common/util"
	mspmgmt "github.com/hyperledger/fabric/msp/mgmt"
	"github.com/hyperledger/fabric/orderer/common/performance"
	"github.com/op/go-logging"
	"gopkg.in/alecthomas/kingpin.v2"
)

const pkgLogID = "orderer/common/server"

var logger *logging.Logger

func init() {
	logger = flogging.MustGetLogger(pkgLogID)
}

//command line flags
var (
	app = kingpin.New("orderer", "Hyperledger Fabric orderer node")

	start     = app.Command("start", "Start the orderer node").Default()
	version   = app.Command("version", "Show version information")
	benchmark = app.Command("benchmark", "Run orderer in benchmark mode")
)

// Main is the entry point of orderer process
func Main() {
	fullCmd := kingpin.MustParse(app.Parse(os.Args[1:]))

	// "version" command
	if fullCmd == version.FullCommand() {
		fmt.Println(metadata.GetVersionInfo())
		return
	}

	// 加载配置信息
	conf, err := config.Load()
	if err != nil {
		logger.Error("failed to parse config: ", err)
		os.Exit(1)
	}
	// 初始化日志级别
	initializeLoggingLevel(conf)
	// 初始化本地MSP组件
	initializeLocalMsp(conf)
	// 打印配置信息
	prettyPrintStruct(conf)
	// 启动 orderer 服务
	Start(fullCmd, conf)
}

// Start provides a layer of abstraction for benchmark test
func Start(cmd string, conf *config.TopLevel) {
	// 创建本地 MSP 签名者实体
	signer := localmsp.NewSigner()
	//初始化 tls 认证的安全服务器配置项
	serverConfig := initializeServerConfig(conf)
	// 初始化 grpc 服务器
	grpcServer := initializeGrpcServer(conf, serverConfig)
	// 创建 CA support 组件
	caSupport := &comm.CASupport{
		AppRootCAsByChain:     make(map[string][][]byte),	// Application根CA证书字典
		OrdererRootCAsByChain: make(map[string][][]byte),	// Orderer根CA证书字典
		ClientRootCAs:         serverConfig.SecOpts.ClientRootCAs, // 设置 TLS 认证的客户端根CA证书列表
	}

	// TLS 连接认证的回调函数
	tlsCallback := func(bundle *channelconfig.Bundle) {
		// only need to do this if mutual TLS is required
		if grpcServer.MutualTLSRequired() {	// 检查是否需要认证 TLS 客户端证书，源码可追溯到 General.TLS.ClientAuthRequired
			logger.Debug("Executing callback to update root CAs")
			updateTrustedRoots(grpcServer, caSupport, bundle) // 执行回调函数更新根 CA 证书
		}
	}
	// 初始化多channel管理器对象
	manager := InitializeMultichannelRegistrar(conf, signer, tlsCallback)
	// 设置 tls 双向认证标志
	mutualTLS := serverConfig.SecOpts.UseTLS && serverConfig.SecOpts.RequireClientCert
	// 创建 orderer 排序服务器
	server := NewServer(manager,
		signer,
		&conf.Debug,
		conf.General.Authentication.TimeWindow,
		mutualTLS)

	switch cmd {
	case start.FullCommand(): // "start" command
		logger.Infof("Starting %s", metadata.GetVersionInfo())
		//
		initializeProfilingService(conf)
		//
		ab.RegisterAtomicBroadcastServer(grpcServer.Server(), server)
		logger.Info("Beginning to serve requests")
		//
		grpcServer.Start()
	case benchmark.FullCommand(): // "benchmark" command
		logger.Info("Starting orderer in benchmark mode")
		benchmarkServer := performance.GetBenchmarkServer()
		benchmarkServer.RegisterService(server)
		benchmarkServer.Start()
	}
}

// Set the logging level
func initializeLoggingLevel(conf *config.TopLevel) {
	flogging.InitBackend(flogging.SetFormat(conf.General.LogFormat), os.Stderr)
	flogging.InitFromSpec(conf.General.LogLevel)
}

// Start the profiling service if enabled.
func initializeProfilingService(conf *config.TopLevel) {
	if conf.General.Profile.Enabled {
		go func() {
			logger.Info("Starting Go pprof profiling service on:", conf.General.Profile.Address)
			// The ListenAndServe() call does not return unless an error occurs.
			logger.Panic("Go pprof service failed:", http.ListenAndServe(conf.General.Profile.Address, nil))
		}()
	}
}

// 初始化TLS认证的安全配置
func initializeServerConfig(conf *config.TopLevel) comm.ServerConfig {
	// secure server config
	secureOpts := &comm.SecureOptions{
		UseTLS:            conf.General.TLS.Enabled,
		RequireClientCert: conf.General.TLS.ClientAuthRequired,
	}
	// check to see if TLS is enabled
	if secureOpts.UseTLS {
		msg := "TLS"
		// load crypto material from files
		serverCertificate, err := ioutil.ReadFile(conf.General.TLS.Certificate)
		if err != nil {
			logger.Fatalf("Failed to load server Certificate file '%s' (%s)",
				conf.General.TLS.Certificate, err)
		}
		serverKey, err := ioutil.ReadFile(conf.General.TLS.PrivateKey)
		if err != nil {
			logger.Fatalf("Failed to load PrivateKey file '%s' (%s)",
				conf.General.TLS.PrivateKey, err)
		}
		var serverRootCAs, clientRootCAs [][]byte
		for _, serverRoot := range conf.General.TLS.RootCAs {
			root, err := ioutil.ReadFile(serverRoot)
			if err != nil {
				logger.Fatalf("Failed to load ServerRootCAs file '%s' (%s)",
					err, serverRoot)
			}
			serverRootCAs = append(serverRootCAs, root)
		}
		if secureOpts.RequireClientCert {
			for _, clientRoot := range conf.General.TLS.ClientRootCAs {
				root, err := ioutil.ReadFile(clientRoot)
				if err != nil {
					logger.Fatalf("Failed to load ClientRootCAs file '%s' (%s)",
						err, clientRoot)
				}
				clientRootCAs = append(clientRootCAs, root)
			}
			msg = "mutual TLS"
		}
		secureOpts.Key = serverKey
		secureOpts.Certificate = serverCertificate
		secureOpts.ServerRootCAs = serverRootCAs
		secureOpts.ClientRootCAs = clientRootCAs
		logger.Infof("Starting orderer with %s enabled", msg)
	}
	kaOpts := comm.DefaultKeepaliveOptions()
	// keepalive settings
	// ServerMinInterval must be greater than 0
	if conf.General.Keepalive.ServerMinInterval > time.Duration(0) {
		kaOpts.ServerMinInterval = conf.General.Keepalive.ServerMinInterval
	}
	kaOpts.ServerInterval = conf.General.Keepalive.ServerInterval
	kaOpts.ServerTimeout = conf.General.Keepalive.ServerTimeout

	return comm.ServerConfig{SecOpts: secureOpts, KaOpts: kaOpts}
}

func initializeBootstrapChannel(conf *config.TopLevel, lf blockledger.Factory) {
	var genesisBlock *cb.Block

	// Select the bootstrapping mechanism
	switch conf.General.GenesisMethod {
	case "provisional":
		genesisBlock = encoder.New(genesisconfig.Load(conf.General.GenesisProfile)).GenesisBlockForChannel(conf.General.SystemChannel)
	case "file":
		genesisBlock = file.New(conf.General.GenesisFile).GenesisBlock()
	default:
		logger.Panic("Unknown genesis method:", conf.General.GenesisMethod)
	}

	chainID, err := utils.GetChainIDFromBlock(genesisBlock)
	if err != nil {
		logger.Fatal("Failed to parse chain ID from genesis block:", err)
	}
	gl, err := lf.GetOrCreate(chainID)
	if err != nil {
		logger.Fatal("Failed to create the system chain:", err)
	}

	err = gl.Append(genesisBlock)
	if err != nil {
		logger.Fatal("Could not write genesis block to ledger:", err)
	}
}

func initializeGrpcServer(conf *config.TopLevel, serverConfig comm.ServerConfig) comm.GRPCServer {
	/*
		参数 conf 源于配置文件 load
		serverConfig：由 initializeServerConfig() 产生
	*/
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d",
		conf.General.ListenAddress/* 127.0.0.1 */, conf.General.ListenPort /* 7050 */))
	if err != nil {
		logger.Fatal("Failed to listen:", err)
	}

	// Create GRPC server - return if an error occurs
	grpcServer, err := comm.NewGRPCServerFromListener(lis, serverConfig)
	if err != nil {
		logger.Fatal("Failed to return new GRPC server:", err)
	}

	return grpcServer
}

func initializeLocalMsp(conf *config.TopLevel) {
	// Load local MSP
	// 根据MSP配置文件路径，bccsp 密码服务组件配置，msp名称初始化本地msp组件
	err := mspmgmt.LoadLocalMsp(conf.General.LocalMSPDir, conf.General.BCCSP, conf.General.LocalMSPID)
	if err != nil { // Handle errors reading the config file
		logger.Fatal("Failed to initialize local MSP:", err)
	}
}

func InitializeMultichannelRegistrar(conf *config.TopLevel, signer crypto.LocalSigner,
	callbacks ...func(bundle *channelconfig.Bundle)) *multichannel.Registrar {
	lf, _ := createLedgerFactory(conf)
	// Are we bootstrapping?
	if len(lf.ChainIDs()) == 0 {
		// 初始化 Bootstrap channel
		initializeBootstrapChannel(conf, lf)
	} else {
		logger.Info("Not bootstrapping because of existing chains")
	}

	consenters := make(map[string]consensus.Consenter)
	consenters["solo"] = solo.New()
	consenters["kafka"] = kafka.New(conf.Kafka)

	// 创建多channel 注册管理器对象
	return multichannel.NewRegistrar(lf, consenters, signer, callbacks...)
}

func updateTrustedRoots(srv comm.GRPCServer, rootCASupport *comm.CASupport,
	cm channelconfig.Resources) {
	rootCASupport.Lock()
	defer rootCASupport.Unlock()

	appRootCAs := [][]byte{}
	ordererRootCAs := [][]byte{}
	appOrgMSPs := make(map[string]struct{})
	ordOrgMSPs := make(map[string]struct{})

	if ac, ok := cm.ApplicationConfig(); ok {
		//loop through app orgs and build map of MSPIDs
		for _, appOrg := range ac.Organizations() {
			appOrgMSPs[appOrg.MSPID()] = struct{}{}
		}
	}

	if ac, ok := cm.OrdererConfig(); ok {
		//loop through orderer orgs and build map of MSPIDs
		for _, ordOrg := range ac.Organizations() {
			ordOrgMSPs[ordOrg.MSPID()] = struct{}{}
		}
	}

	if cc, ok := cm.ConsortiumsConfig(); ok {
		for _, consortium := range cc.Consortiums() {
			//loop through consortium orgs and build map of MSPIDs
			for _, consortiumOrg := range consortium.Organizations() {
				appOrgMSPs[consortiumOrg.MSPID()] = struct{}{}
			}
		}
	}

	cid := cm.ConfigtxValidator().ChainID()
	logger.Debugf("updating root CAs for channel [%s]", cid)
	msps, err := cm.MSPManager().GetMSPs()
	if err != nil {
		logger.Errorf("Error getting root CAs for channel %s (%s)", cid, err)
	}
	if err == nil {
		for k, v := range msps {
			// check to see if this is a FABRIC MSP
			if v.GetType() == msp.FABRIC {
				for _, root := range v.GetTLSRootCerts() {
					// check to see of this is an app org MSP
					if _, ok := appOrgMSPs[k]; ok {
						logger.Debugf("adding app root CAs for MSP [%s]", k)
						appRootCAs = append(appRootCAs, root)
					}
					// check to see of this is an orderer org MSP
					if _, ok := ordOrgMSPs[k]; ok {
						logger.Debugf("adding orderer root CAs for MSP [%s]", k)
						ordererRootCAs = append(ordererRootCAs, root)
					}
				}
				for _, intermediate := range v.GetTLSIntermediateCerts() {
					// check to see of this is an app org MSP
					if _, ok := appOrgMSPs[k]; ok {
						logger.Debugf("adding app root CAs for MSP [%s]", k)
						appRootCAs = append(appRootCAs, intermediate)
					}
					// check to see of this is an orderer org MSP
					if _, ok := ordOrgMSPs[k]; ok {
						logger.Debugf("adding orderer root CAs for MSP [%s]", k)
						ordererRootCAs = append(ordererRootCAs, intermediate)
					}
				}
			}
		}
		rootCASupport.AppRootCAsByChain[cid] = appRootCAs
		rootCASupport.OrdererRootCAsByChain[cid] = ordererRootCAs

		// now iterate over all roots for all app and orderer chains
		trustedRoots := [][]byte{}
		for _, roots := range rootCASupport.AppRootCAsByChain {
			trustedRoots = append(trustedRoots, roots...)
		}
		for _, roots := range rootCASupport.OrdererRootCAsByChain {
			trustedRoots = append(trustedRoots, roots...)
		}
		// also need to append statically configured root certs
		if len(rootCASupport.ClientRootCAs) > 0 {
			trustedRoots = append(trustedRoots, rootCASupport.ClientRootCAs...)
		}

		// now update the client roots for the gRPC server
		err := srv.SetClientRootCAs(trustedRoots)
		if err != nil {
			msg := "Failed to update trusted roots for orderer from latest config " +
				"block.  This orderer may not be able to communicate " +
				"with members of channel %s (%s)"
			logger.Warningf(msg, cm.ConfigtxValidator().ChainID(), err)
		}
	}
}

func prettyPrintStruct(i interface{}) {
	params := util.Flatten(i)
	var buffer bytes.Buffer
	for i := range params {
		buffer.WriteString("\n\t")
		buffer.WriteString(params[i])
	}
	logger.Infof("Orderer config values:%s\n", buffer.String())
}
