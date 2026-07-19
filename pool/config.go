package pool

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/yugank/flume/flusher"
)

// Config holds the pool parameters exposed via env vars or a JSON config file.
type Config struct {
	PoolSize   uint64 `json:"pool_size"`
	SlotCount  uint64 `json:"slot_count"`
	SlotSize   uint64 `json:"slot_size"`
	BufferID   int64  `json:"buffer_id"` // reserved for future use; defaults to 0
	MaxWriters int    `json:"max_writers"`
	MaxReaders int    `json:"max_readers"`
	Flusher    string `json:"flusher"` // "noop" or "dummy" (default); selects the flusher wired into DefaultPool
}

// DefaultPool is the package-level pool initialized from env vars or a JSON
// config file at startup. It is nil if no configuration source is found.
var DefaultPool *Pool

// DefaultConfig is the resolved configuration used to create DefaultPool.
var DefaultConfig Config

const defaultWorkers = 64

func init() {
	cfg, ok := loadConfig()
	if !ok {
		return
	}
	if cfg.MaxWriters == 0 {
		cfg.MaxWriters = defaultWorkers
	}
	if cfg.MaxReaders == 0 {
		cfg.MaxReaders = defaultWorkers
	}
	var f flusher.Flusher = &flusher.DummyFlusher{}
	if cfg.Flusher == "noop" {
		f = flusher.NoopFlusher{}
	}

	details := cfg.PoolSize | (cfg.SlotCount << slotCountShift) | (cfg.SlotSize << slotSizeShift)
	p, err := CreatePool(details, cfg.BufferID, f)
	if err != nil {
		panic("pool: auto-init failed: " + err.Error())
	}
	DefaultPool = p
	DefaultConfig = cfg
}

// loadConfig resolves configuration from env vars (primary) or a JSON config
// file (fallback). Returns (Config, true) when a source is found and valid,
// (Config{}, false) when no source is present at all. Panics on malformed input.
func loadConfig() (Config, bool) {
	if cfg, ok := loadFromEnv(); ok {
		return cfg, true
	}
	return loadFromFile()
}

func loadFromEnv() (Config, bool) {
	ps := os.Getenv("FLUME_POOL_SIZE")
	sc := os.Getenv("FLUME_SLOT_COUNT")
	ss := os.Getenv("FLUME_SLOT_SIZE")
	if ps == "" || sc == "" || ss == "" {
		return Config{}, false
	}

	poolSize, err := strconv.ParseUint(ps, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("pool: FLUME_POOL_SIZE %q: %v", ps, err))
	}
	slotCount, err := strconv.ParseUint(sc, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("pool: FLUME_SLOT_COUNT %q: %v", sc, err))
	}
	slotSize, err := strconv.ParseUint(ss, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("pool: FLUME_SLOT_SIZE %q: %v", ss, err))
	}

	var bufferID int64
	if bid := os.Getenv("FLUME_BUFFER_ID"); bid != "" {
		bufferID, err = strconv.ParseInt(bid, 10, 64)
		if err != nil {
			panic(fmt.Sprintf("pool: FLUME_BUFFER_ID %q: %v", bid, err))
		}
	}

	var maxWriters, maxReaders int
	if mw := os.Getenv("FLUME_MAX_WRITERS"); mw != "" {
		v, err := strconv.Atoi(mw)
		if err != nil {
			panic(fmt.Sprintf("pool: FLUME_MAX_WRITERS %q: %v", mw, err))
		}
		maxWriters = v
	}
	if mr := os.Getenv("FLUME_MAX_READERS"); mr != "" {
		v, err := strconv.Atoi(mr)
		if err != nil {
			panic(fmt.Sprintf("pool: FLUME_MAX_READERS %q: %v", mr, err))
		}
		maxReaders = v
	}

	return Config{
		PoolSize:   poolSize,
		SlotCount:  slotCount,
		SlotSize:   slotSize,
		BufferID:   bufferID,
		MaxWriters: maxWriters,
		MaxReaders: maxReaders,
		Flusher:    os.Getenv("FLUME_FLUSHER"),
	}, true
}

func loadFromFile() (Config, bool) {
	path := configFilePath()
	if path == "" {
		return Config{}, false
	}

	data, err := os.ReadFile(path)
	if err != nil {
		panic(fmt.Sprintf("pool: reading config file %q: %v", path, err))
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		panic(fmt.Sprintf("pool: parsing config file %q: %v", path, err))
	}

	return cfg, true
}

func configFilePath() string {
	if p := os.Getenv("FLUME_CONFIG_FILE"); p != "" {
		return p
	}
	for _, candidate := range []string{"./flume.json", "../flume.json"} {
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	if home, err := os.UserHomeDir(); err == nil {
		candidate := home + "/.flume.json"
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return ""
}
