package configurationManager

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
)

type config struct {
	MemTableThreshold uint32 `json:"MemTableThreshold"`
	WalSegmentSize    uint64 `json:"WalSegmentSize"`
	WalDirectory      string `json:"WalDirectory"`
	LowWaterMark      uint32 `json:"LowWaterMark"`
}

func (c *config) GetMemTableThreshold() uint32 { return c.MemTableThreshold }
func (c *config) GetWalSegmentSize() uint64    { return c.WalSegmentSize }
func (c *config) GetWalDirectory() string      { return c.WalDirectory }
func (c *config) GetLowWaterMark() uint32      { return c.LowWaterMark }

var UserConfiguration config
var DefaultConfiguration config

func LoadUserConfiguration(filePath string) {
	parseJSON(filePath, &UserConfiguration)
}

func LoadDefaultConfiguration(filePath string) {
	parseJSON(filePath, &DefaultConfiguration)
}

func parseJSON(filePath string, destination *config) {
	jsonFile, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer jsonFile.Close()

	byteArr, _ := ioutil.ReadAll(jsonFile)

	err = json.Unmarshal(byteArr, destination)
	if err != nil {
		log.Fatal(err)
	}
}
