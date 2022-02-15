package run

import (
	"configMng"
	"wal"
)

func main() {
	configurationManager.LoadDefaultConfiguration("Data/defaultConfiguration.json")
	w := wal.CreateWal(configurationManager.DefaultConfiguration.WalSegmentSize, configurationManager.DefaultConfiguration.WalDirectory, configurationManager.DefaultConfiguration.LowWaterMark)
}
