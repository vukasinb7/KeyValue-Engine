package merkleTree

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"os"
)

type MerkleRoot struct {
	root *Node
}

func (mr *MerkleRoot) String() string {
	return mr.root.String()
}

type Node struct {
	data  []byte
	left  *Node
	right *Node
}

func (n *Node) String() string {
	return hex.EncodeToString(n.data[:])
}

func Hash(data []byte) []byte {
	hash := sha1.Sum(data)
	return hash[:]
}
func NewMerkleNode(left, right *Node, data []byte) *Node {
	newNode := Node{}
	if left == nil && right == nil {
		newNode.data = Hash(data)
	} else {
		prevHashValue := append(left.data, right.data...)
		newNode.data = Hash(prevHashValue)
	}
	newNode.left = left
	newNode.right = right
	return &newNode

}
func NewMerkleTree(data [][]byte) *MerkleRoot {
	var nodes []Node

	// kreiranje listova
	for _, dat := range data {
		newNode := NewMerkleNode(nil, nil, dat)
		nodes = append(nodes, *newNode)
	}

	for len(nodes) > 1 {
		var treeLevel []Node

		// ukoliko je potrebno dotati jos jedan podataka da bude paran broj
		if len(nodes)%2 != 0 {
			newNode := NewMerkleNode(nil, nil, []byte(""))
			nodes = append(nodes, *newNode)
		}

		for j := 0; j < len(nodes); j += 2 {
			node := NewMerkleNode(&nodes[j], &nodes[j+1], nil)
			treeLevel = append(treeLevel, *node)
		}
		nodes = treeLevel
	}
	tree := MerkleRoot{&nodes[0]}
	return &tree
}
func (mr *MerkleRoot) SerializeMerkleTree(file *os.File) {
	f := file

	defer f.Close()
	var nodes []Node
	nodes = append(nodes, *mr.root)
	for len(nodes) > 0 {
		var newNodes []Node
		for i := 0; i < len(nodes); i++ {
			f.Write([]byte(nodes[i].String()))
			f.Write([]byte(";"))
			if nodes[i].left != nil {
				newNodes = append(newNodes, *nodes[i].left)
			}
			if nodes[i].right != nil {
				newNodes = append(newNodes, *nodes[i].right)
			}
		}
		f.Write([]byte("\n"))
		nodes = newNodes
	}
}

func main() {
	var list [][]byte
	list = append(list, []byte("vule"))
	list = append(list, []byte("jole"))
	list = append(list, []byte("dule"))
	list = append(list, []byte("sule"))
	list = append(list, []byte("dule1"))
	list = append(list, []byte("sule1"))
	mr := NewMerkleTree(list)
	fmt.Println(mr.root)
	//mr.SerializeMerkleTree()

}
