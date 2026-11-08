package client

import (
	"github.com/sentinel-official/sentinel-go-sdk/v3/session"
)

const (
	ChannelStatusClosed  = "closed"
	ChannelStatusClosing = "closing"
	ChannelStatusOpen    = "open"
	ChannelStatusPending = "pending"
	SessionStatusActive  = "active"
	SessionStatusEnded   = "ended"
	// SessionStatusReconnecting: the node cannot be reached (a failing poll, or no contact for a while), or the
	// daemon is redialling it after the node timeout. Back to active on the next contact or redial.
	SessionStatusReconnecting = "reconnecting"
)

type Channel struct {
	Address     string `gorm:"primaryKey"`
	Cumulative  uint64
	Escrow      uint64
	Mode        uint8
	NodeAddress string `gorm:"index"`
	OpenedUnix  int64
	Redeemed    uint64
	Status      string
	UpdatedUnix int64 `gorm:"autoUpdateTime"`
}

type Firewall struct {
	DNSApplied    bool
	ID            uint `gorm:"primaryKey"`
	Iface         string
	NodeAddrs     string
	NodeURI       string
	Persistent    bool
	RPCAddrs      string
	RPCAndWSAddrs string
	RPCURL        string
	WSURL         string
}

func (Firewall) TableName() string {
	return "firewalls"
}

type Session struct {
	ChannelAddress   string
	Clean            bool
	EndedUnix        int64
	Error            string
	ID               string `gorm:"primaryKey"`
	NodeAddress      string
	OwedBaseUnits    uint64
	PaidBaseUnits    uint64
	ProxyEndpoint    string
	RxBytes          uint64
	Seconds          uint64
	Service          string
	SettledBaseUnits uint64
	SpentBaseUnits   uint64 `gorm:"column:spent"`
	StartedUnix      int64
	Status           string `gorm:"index"`
	TicketHash       string
	TxBytes          uint64
	UpdatedUnix      int64 `gorm:"autoUpdateTime"`
}

func (s *Session) Usage() session.Usage {
	return session.Usage{
		RxBytes: s.RxBytes,
		Seconds: s.Seconds,
		TxBytes: s.TxBytes,
	}
}

type Ticket struct {
	DataCapBytes        uint64
	Encoded             string
	ExpiresUnix         int64
	Hash                string `gorm:"primaryKey"`
	NodeAddress         string
	ProviderAddress     string
	SubscriptionAddress string
}

type Voucher struct {
	AcceptedUnix   int64
	ChannelAddress string `gorm:"index"`
	Cumulative     uint64
	ID             uint `gorm:"primaryKey;autoIncrement"`
	IssuedUnix     int64
	SessionID      string `gorm:"index"`
	Signature      []byte
}
