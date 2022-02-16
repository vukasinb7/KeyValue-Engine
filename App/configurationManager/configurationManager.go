package configurationManager

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"pair"
)

type config struct {
	MemTableThreshold uint32 `json:"MemTableThreshold"`
	MemTableCapacity  uint32 `json:"MemTableCapacity"`
	WalSegmentSize    uint64 `json:"WalSegmentSize"`
	WalDirectory      string `json:"WalDirectory"`
	LowWaterMark      uint32 `json:"LowWaterMark"`
	DataFile          string `json:"DataFile"`
	CacheCapacity     uint32 `json:"CacheCapacity"`
	SSTableDirectory  string `json:"SSTableDirectory"`
}

func (c *config) GetMemTableThreshold() uint32 { return c.MemTableThreshold }
func (c *config) GetMemTableCapacity() uint32  { return c.MemTableCapacity }
func (c *config) GetWalSegmentSize() uint64    { return c.WalSegmentSize }
func (c *config) GetWalDirectory() string      { return c.WalDirectory }
func (c *config) GetLowWaterMark() uint32      { return c.LowWaterMark }
func (c *config) GetDataFile() string          { return c.DataFile }
func (c *config) GetSSTableDirectory() string  { return c.SSTableDirectory }

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

func ParseData(dataFilePath string) []pair.KVPair {
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
				result = append(result, pair.KVPair{key, temp, 0})
			}
			temp = nil
			brojac++
		} else {
			temp = append(temp, data[i])
		}
	}
	return result
}
