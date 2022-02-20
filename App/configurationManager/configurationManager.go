package configurationManager

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"pair"
	"time"
)

type config struct {
	MemTableThreshold     uint32 `json:"MemTableThreshold"`
	MemTableCapacity      uint32 `json:"MemTableCapacity"`
	WalSegmentSize        uint64 `json:"WalSegmentSize"`
	WalDirectory          string `json:"WalDirectory"`
	LowWaterMark          uint32 `json:"LowWaterMark"`
	DataFile              string `json:"DataFile"`
	CacheCapacity         uint32 `json:"CacheCapacity"`
	LSMDirectory          string `json:"LSMDirectory"`
	LSMlevelNum           uint32 `json:"LSMlevelNum"`
	TokenBucketNumOfTries uint32 `json:"TokenBucketNumOfTries"`
	TokenBucketInterval   uint32 `json:"TokenBucketInterval"`
}

func (c *config) GetMemTableThreshold() uint32     { return c.MemTableThreshold }
func (c *config) GetMemTableCapacity() uint32      { return c.MemTableCapacity }
func (c *config) GetWalSegmentSize() uint64        { return c.WalSegmentSize }
func (c *config) GetWalDirectory() string          { return c.WalDirectory }
func (c *config) GetLowWaterMark() uint32          { return c.LowWaterMark }
func (c *config) GetDataFile() string              { return c.DataFile }
func (c *config) GetLSMDirectory() string          { return c.LSMDirectory }
func (c *config) GetLSMlevelNum() uint32           { return c.LSMlevelNum }
func (c *config) GetCacheCapacity() uint32         { return c.CacheCapacity }
func (c *config) GetTokenBucketNumOfTries() uint32 { return c.TokenBucketNumOfTries }
func (c *config) GetTokenBucketInterval() uint32   { return c.TokenBucketInterval }

var Configuration config

func LoadUserConfiguration(filePath string) {
	err := parseJSON(filePath, &Configuration)
	if err != nil {
		LoadDefaultConfiguration()
		return
	} else if Configuration.MemTableThreshold <= 0 {
		LoadDefaultConfiguration()
		return
	} else if Configuration.MemTableCapacity <= 0 {
		LoadDefaultConfiguration()
		return
	} else if Configuration.WalSegmentSize <= 0 {
		LoadDefaultConfiguration()
		return
	} else if !Exists(Configuration.WalDirectory) {
		LoadDefaultConfiguration()
		return
	} else if Configuration.LowWaterMark < 0 {
		LoadDefaultConfiguration()
		return
	} else if !Exists(Configuration.DataFile) {
		LoadDefaultConfiguration()
		return
	} else if !Exists(Configuration.LSMDirectory) {
		LoadDefaultConfiguration()
		return
	} else if Configuration.LSMlevelNum <= 0 {
		LoadDefaultConfiguration()
		return
	} else if Configuration.CacheCapacity <= 0 {
		LoadDefaultConfiguration()
		return
	} else if Configuration.TokenBucketNumOfTries <= 0 {
		LoadDefaultConfiguration()
		return
	} else if Configuration.TokenBucketInterval <= 0 {
		LoadDefaultConfiguration()
		return
	}

}

func LoadDefaultConfiguration() {
	Configuration.MemTableThreshold = 128
	Configuration.MemTableCapacity = 256
	Configuration.WalSegmentSize = 1024
	Configuration.WalDirectory = "Data/WAL/"
	Configuration.LowWaterMark = 1
	Configuration.DataFile = "Data/testData.txt"
	Configuration.CacheCapacity = 10
	Configuration.LSMDirectory = "Data/LSM/"
	Configuration.LSMlevelNum = 4
	Configuration.TokenBucketNumOfTries = 3
	Configuration.TokenBucketInterval = 10
}

func parseJSON(filePath string, destination *config) error {
	jsonFile, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer func(jsonFile *os.File) {
		err := jsonFile.Close()
		if err != nil {

		}
	}(jsonFile)

	byteArr, _ := ioutil.ReadAll(jsonFile)

	err = json.Unmarshal(byteArr, destination)
	if err != nil {
		return err
	}
	return nil
}

func ParseTxtData(dataFilePath string) []pair.KVPair {
	data, err := ioutil.ReadFile(dataFilePath)
	if err != nil {
		fmt.Println("File reading error", err)
	}
	var result []pair.KVPair
	brojac := 0
	var temp []byte
	var key string
	for i := 0; i < len(data); i++ {
		c := string(data[i])
		if c == "|" || c == "\n" {
			if brojac%2 == 0 {
				key = string(temp)

			} else {
				currentTime := time.Now()
				timestamp := currentTime.UnixNano()
				result = append(result, pair.KVPair{key, temp[0 : len(temp)-1], 0, uint64(timestamp)})
			}
			temp = nil
			brojac++
		} else {
			temp = append(temp, data[i])
		}
	}
	return result
}

func Exists(name string) bool {
	_, err := os.Stat(name)
	if err == nil {
		return true
	}
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	return false
}
