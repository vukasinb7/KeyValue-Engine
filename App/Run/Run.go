package main

import (
	"Engine"
	"LSMTree"
	"bufio"
	"configurationManager"
	"fmt"
	"log"
	"lru"
	"memTable"
	"os"
	"pair"
	"strconv"
	"strings"
	"tokenBucket"
	"wal"
)

func insertTestData() {
	data := configurationManager.ParseTxtData(configurationManager.Configuration.DataFile)
	for _, val := range data {
		newPair := pair.KVPair{val.Key, val.Value, val.Tombstone, val.Timestamp}

		err := Engine.DefWal.PushRecord(newPair)
		if err != nil {
			log.Fatal(err)
		}
		Engine.DefMemtable.Insert(newPair)
		Engine.DefLRUCache.Set(val.Key, val.Value, val.Tombstone)
		if Engine.DefMemtable.Size() > Engine.DefMemtable.Threshold() {
			Engine.DefLSM.CreateLevelTables(Engine.DefMemtable.Flush())
			Engine.DefWal.ResetWAL()
		}
	}
}

func main() {
	configurationManager.LoadUserConfiguration("Data/userConfiguration.json")
	Engine.DefWal = wal.CreateWal(configurationManager.Configuration.WalSegmentSize, configurationManager.Configuration.WalDirectory, configurationManager.Configuration.LowWaterMark)
	Engine.DefMemtable = memTable.NewMemTable(configurationManager.Configuration.MemTableThreshold, configurationManager.Configuration.MemTableCapacity)
	Engine.DefLSM = LSMTree.NewLSM(configurationManager.Configuration.GetLSMlevelNum(), configurationManager.Configuration.GetLSMDirectory())
	Engine.DefLRUCache = lru.NewLRU(configurationManager.Configuration.GetCacheCapacity())
	Engine.DefTB = tokenBucket.NewTokenBucket(configurationManager.Configuration.GetTokenBucketNumOfTries(), configurationManager.Configuration.GetTokenBucketInterval())
	mainMenu()
}

func mainMenu() {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Println("\nMain menu")
		fmt.Println("------------------------------")
		fmt.Println("1. Insert data from file")
		fmt.Println("2. Put")
		fmt.Println("3. Get")
		fmt.Println("4. Delete")
		fmt.Println("5. Compactions")
		fmt.Println("6. HLL")
		fmt.Println("7. CMS")
		fmt.Println("8. BF")
		fmt.Println("9. Exit")
		fmt.Println("------------------------------")
		fmt.Print("Enter option: ")
		var option string
		scanner.Scan()
		option = scanner.Text()
		if option == "1" {
			insertTestData()

		} else if option == "2" {
			fmt.Print("Enter key: ")
			var key string
			scanner.Scan()
			key = scanner.Text()
			fmt.Print("Enter value: ")
			var value []byte
			scanner.Scan()
			value = scanner.Bytes()
			Engine.Put(key, value)

		} else if option == "3" {
			fmt.Print("Enter key: ")
			var key string
			scanner.Scan()
			key = scanner.Text()
			value := Engine.Get(key)
			if value != nil {
				fmt.Println("\nValue: ", value)
			} else {
				fmt.Println("\nRecord not found!")
			}

		} else if option == "4" {
			fmt.Print("Enter key: ")
			var key string
			scanner.Scan()
			key = scanner.Text()
			Engine.Delete(key)

		} else if option == "5" {
			head := Engine.DefLSM.LsmLevels()
			if head.Size() > head.Threshold() {
				if head.NextLevel() != nil {
					head.Compaction()
					fmt.Println("\nCompactions done successfully!")
				}
			} else {
				fmt.Println("\nCompactions unsuccessful!")
			}

		} else if option == "6" {
			for {
				fmt.Println("\nHLL menu")
				fmt.Println("------------------------------")
				fmt.Println("1. Create new HLL")
				fmt.Println("2. Insert data in existing HLL")
				fmt.Println("3. Get Cardinality")
				fmt.Println("4. Return to main menu")
				fmt.Println("------------------------------")
				fmt.Print("Enter option: ")
				var secondOption string
				scanner.Scan()
				secondOption = scanner.Text()
				if secondOption == "1" {
					fmt.Print("Enter key: ")
					var key string
					scanner.Scan()
					key = scanner.Text()
					fmt.Print("Enter values: ")
					var valuesString string
					scanner.Scan()
					valuesString = scanner.Text()
					fmt.Print("Enter precision: ")
					var pString string
					scanner.Scan()
					pString = scanner.Text()
					values := strings.Split(valuesString, ",")
					p, _ := strconv.Atoi(pString)

					var bytesArray [][]byte
					for _, value := range values {
						bytesArray = append(bytesArray, []byte(value))
					}

					err := Engine.CreateHll(key, bytesArray, uint(p))
					if err != nil {
						fmt.Println("\nError:", err)
					}

				} else if secondOption == "2" {
					fmt.Print("Enter key: ")
					var key string
					scanner.Scan()
					key = scanner.Text()
					fmt.Print("Enter values: ")
					var valuesString string
					scanner.Scan()
					valuesString = scanner.Text()
					values := strings.Split(valuesString, ",")

					var bytesArray [][]byte
					for _, value := range values {
						bytesArray = append(bytesArray, []byte(value))
					}

					err := Engine.InsertIntoHll(key, bytesArray)
					if err != nil {
						fmt.Println("\nError:", err)
					}

				} else if secondOption == "3" {
					fmt.Print("Enter key: ")
					var key string
					scanner.Scan()
					key = scanner.Text()
					value, err := Engine.GetCardinality(key)
					if err != nil {
						fmt.Println("\nError:", err)
					} else {
						fmt.Println("\nCardinality: ", value)
					}
				} else if secondOption == "4" {
					break
				} else {
					fmt.Println("\nInvalid input!")
				}
			}

		} else if option == "7" {
			for {
				fmt.Println("\nCMS menu")
				fmt.Println("------------------------------")
				fmt.Println("1. Create new CMS")
				fmt.Println("2. Insert data in existing CMS")
				fmt.Println("3. Get number of appearances")
				fmt.Println("4. Return to main menu")
				fmt.Println("------------------------------")
				fmt.Print("Enter option: ")
				var secondOption string
				scanner.Scan()
				secondOption = scanner.Text()
				if secondOption == "1" {
					fmt.Print("Enter key: ")
					var key string
					scanner.Scan()
					key = scanner.Text()
					fmt.Print("Enter value: ")
					var valuesString string
					scanner.Scan()
					valuesString = scanner.Text()
					fmt.Print("Enter precision: ")
					var precision string
					scanner.Scan()
					precision = scanner.Text()
					fmt.Print("Enter accuracy: ")
					var accuracy string
					scanner.Scan()
					accuracy = scanner.Text()
					values := strings.Split(valuesString, ",")

					var bytesArray [][]byte
					for _, value := range values {
						bytesArray = append(bytesArray, []byte(value))
					}
					prs, _ := strconv.ParseFloat(precision, 64)
					acc, _ := strconv.ParseFloat(accuracy, 64)

					err := Engine.CreateCms(key, bytesArray, prs, acc)
					if err != nil {
						fmt.Println("\nError:", err)
					}

				} else if secondOption == "2" {
					fmt.Print("Enter key: ")
					var key string
					scanner.Scan()
					key = scanner.Text()
					fmt.Print("Enter value: ")
					var valuesString string
					scanner.Scan()
					valuesString = scanner.Text()
					values := strings.Split(valuesString, ",")

					var bytesArray [][]byte
					for _, value := range values {
						bytesArray = append(bytesArray, []byte(value))
					}

					err := Engine.InsertIntoCms(key, bytesArray)
					if err != nil {
						fmt.Println("\nError:", err)
					}

				} else if secondOption == "3" {
					fmt.Print("Enter key: ")
					var key string
					scanner.Scan()
					key = scanner.Text()
					fmt.Print("Enter Value: ")
					var value string
					scanner.Scan()
					value = scanner.Text()
					num, err := Engine.CmsNumOfAppearances(key, []byte(value))
					if err != nil {
						fmt.Println("\nError:", err)
					} else {
						fmt.Println("\nNum of appearances: ", num)
					}

				} else if secondOption == "4" {
					break
				} else {
					fmt.Println("\nInvalid input!")
				}
			}
		} else if option == "8" {
			for {
				fmt.Println("\nBF menu")
				fmt.Println("------------------------------")
				fmt.Println("1. Create new BF")
				fmt.Println("2. Insert data in existing BF")
				fmt.Println("3. Contains")
				fmt.Println("4. Return to main menu")
				fmt.Println("------------------------------")
				fmt.Print("Enter option: ")
				var secondOption string
				scanner.Scan()
				secondOption = scanner.Text()
				if secondOption == "1" {
					fmt.Print("Enter key: ")
					var key string
					scanner.Scan()
					key = scanner.Text()
					fmt.Print("Enter values: ")
					var valuesString string
					scanner.Scan()
					valuesString = scanner.Text()
					fmt.Print("Enter precision: ")
					var pString string
					scanner.Scan()
					pString = scanner.Text()
					fmt.Print("Enter size: ")
					var nString string
					scanner.Scan()
					nString = scanner.Text()
					values := strings.Split(valuesString, ",")
					p, _ := strconv.ParseFloat(pString, 64)
					n, _ := strconv.Atoi(nString)

					var bytesArray [][]byte
					for _, value := range values {
						bytesArray = append(bytesArray, []byte(value))
					}

					err := Engine.CreateBloomFilter(key, bytesArray, p, n)
					if err != nil {
						fmt.Println("\nError:", err)
					}

				} else if secondOption == "2" {
					fmt.Print("Enter key: ")
					var key string
					scanner.Scan()
					key = scanner.Text()
					fmt.Print("Enter values: ")
					var valuesString string
					scanner.Scan()
					valuesString = scanner.Text()
					values := strings.Split(valuesString, ",")

					var bytesArray [][]byte
					for _, value := range values {
						bytesArray = append(bytesArray, []byte(value))
					}

					err := Engine.InsertIntoBloomFilter(key, bytesArray)
					if err != nil {
						fmt.Println("\nError:", err)
					}

				} else if secondOption == "3" {
					fmt.Print("Enter key: ")
					var key string
					scanner.Scan()
					key = scanner.Text()
					fmt.Print("Enter value: ")
					var value string
					scanner.Scan()
					value = scanner.Text()
					contains, err := Engine.BloomFilterContains(key, []byte(value))
					if err != nil {
						fmt.Println("\nError:", err)
					} else {
						fmt.Println("\nContains: ", contains)
					}
				} else if secondOption == "4" {
					break
				} else {
					fmt.Println("\nInvalid input!")
				}
			}

		} else if option == "9" {
			records := Engine.DefMemtable.Flush()
			if len(records) > 0 {
				Engine.DefLSM.CreateLevelTables(records)
			}
			os.Exit(0)
		} else {
			fmt.Println("\nInvalid input!")
		}
	}
}
