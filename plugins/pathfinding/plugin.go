package pathfinding

import (
	"sort"

	"github.com/gohornet/hornet/pkg/model/hornet"
	"github.com/gohornet/hornet/pkg/model/tangle"
	"github.com/iotaledger/hive.go/events"
	"github.com/iotaledger/hive.go/logger"
	"github.com/iotaledger/hive.go/node"
	"github.com/iotaledger/iota.go/trinary"
)

var (
	PLUGIN = node.NewPlugin("PathFinding", node.Enabled, configure, run)
	log    *logger.Logger

	// config options

)

func FindPaths(start trinary.Hash, endpoints []trinary.Hash) ([]string, [][]int, [][]int, string) {
	cachedStart := tangle.GetCachedTransactionOrNil(start)
	startTx := *cachedStart.GetTransaction()
	sortedEndpoints := make([]ApproveeStep, 0)
	for i := 0; i < len(endpoints); i++ {
		cachedTx := tangle.GetCachedTransactionOrNil(endpoints[i])
		tx := *cachedTx.GetTransaction()
		sortedEndpoints = insertSorted(sortedEndpoints, ApproveeStep{
			0,
			"",
			newestTimestamp(&tx),
			newestTimestamp(&tx),
			tx.GetHash(),
			tx.GetBranch(),
			tx.GetTrunk(),
		})
		cachedTx.Release(true)

	}

	resultIndex := make(map[string]int)
	edgeIndex := make(map[string]bool)
	resultTxList := make([]string, 0)
	branchesList := make([][]int, 0)
	trunkList := make([][]int, 0)

	localTangle := make(map[string]PathReference)
	overReachQueue := make([]ApproveeStep, 0)
	callQueue := make([]ApproveeStep, 0)
	overReachQueue = insertDescSorted(overReachQueue, ApproveeStep{
		0,
		"",
		newestTimestamp(&startTx),
		newestTimestamp(&startTx),
		startTx.GetHash(),
		startTx.GetBranch(),
		startTx.GetTrunk(),
	})

	indexCounter := 0
	err := ""
	for _, sortedEndpoint := range sortedEndpoints {
		for _, overReachTx := range overReachQueue {
			callQueue = insertSorted(callQueue, overReachTx)
		}
		overReachQueue = make([]ApproveeStep, 0)
		callQueue, overReachQueue, localTangle, err = WalkTangle(callQueue, overReachQueue, sortedEndpoint, localTangle)
		if err != "" {
			return nil, nil, nil, err
		}
		currentRef := localTangle[sortedEndpoint.TX]
		for currentRef.Step != 0 {
			if _, ok := resultIndex[currentRef.TxID]; !ok {
				resultIndex[currentRef.TxID] = indexCounter
				resultTxList = append(resultTxList, currentRef.TxID)
				indexCounter++
			}

			if _, ok := resultIndex[currentRef.ShortestPath]; !ok {
				resultIndex[currentRef.ShortestPath] = indexCounter
				resultTxList = append(resultTxList, currentRef.ShortestPath)
				indexCounter++

			}
			_, edgeFound := edgeIndex[currentRef.TxID+currentRef.ShortestPath]
			if currentRef.Step != 0 && !edgeFound {
				edgeIndex[currentRef.TxID+currentRef.ShortestPath] = true
				if currentRef.ToB {
					trunkList = append(trunkList, []int{resultIndex[currentRef.ShortestPath], resultIndex[currentRef.TxID]})
				} else {
					branchesList = append(branchesList, []int{resultIndex[currentRef.ShortestPath], resultIndex[currentRef.TxID]})

				}
			}

			currentRef = localTangle[currentRef.ShortestPath]
		}
	}

	// call_queue := make(sortedmap.SortedMap[int]hornet.TransactionMetadata)

	return resultTxList, branchesList, trunkList, ""
}

func WalkTangle(callQueue []ApproveeStep, overReachQueue []ApproveeStep, endpoint ApproveeStep, localTangle map[string]PathReference) ([]ApproveeStep, []ApproveeStep, map[string]PathReference, string) {
	lowestTimestamp := endpoint.Timestamp

	for _, ok := localTangle[endpoint.TX]; !ok && len(callQueue) > 0; {
		currentTx := ApproveeStep{}
		currentTx, callQueue = callQueue[0], callQueue[1:]
		currentStep := currentTx.Step
		trunkAndBranch := []trinary.Hash{currentTx.Trunk, currentTx.Branch}

		for i := 0; i < 2; i++ {
			if foundTx, txFound := localTangle[trunkAndBranch[i]]; txFound {
				if currentStep < foundTx.Step {
					foundTx.ShortestPath = currentTx.TX
					foundTx.ToB = i == 0
				}
			} else {

				cachedTx := tangle.GetCachedTransactionOrNil(trunkAndBranch[i])
				if cachedTx != nil {
					tx := *cachedTx.GetTransaction()

					stepTx := ApproveeStep{
						currentStep + 1,
						currentTx.TX,
						newestTimestamp(&tx),
						newestTimestamp(&tx) - (currentTx.Timestamp - newestTimestamp(&tx)),
						tx.GetHash(),
						tx.GetBranch(),
						tx.GetTrunk(),
					}

					if stepTx.Timestamp > lowestTimestamp-300 {
						callQueue = insertSorted(callQueue, stepTx)
					} else {
						overReachQueue = insertSorted(overReachQueue, stepTx)
					}

					pathRef := PathReference{
						currentTx.TX,
						tx.GetHash(),
						i == 0,
						currentStep + 1,
						tx.GetBranch(),
						tx.GetTrunk(),
					}
					localTangle[tx.GetHash()] = pathRef

					cachedTx.Release(true)

				}
			}
		}
	}

	if _, pathFound := localTangle[endpoint.TX]; !pathFound {
		return nil, nil, nil, "Not found"
	}
	return callQueue, overReachQueue, localTangle, ""
}

type PathReference struct {
	ShortestPath trinary.Hash
	TxID         trinary.Hash
	ToB          bool
	Step         int64
	Branch       trinary.Hash
	Trunk        trinary.Hash
}

type ApproveeStep struct {
	Step       int64
	Approvee   trinary.Hash
	Timestamp  int64
	Projection int64
	TX         trinary.Hash
	Branch     trinary.Hash
	Trunk      trinary.Hash
}

func newestTimestamp(tx *hornet.Transaction) int64 {
	if tx.GetAttachmentTimestamp() == 0 {
		return tx.GetTimestamp()
	}
	if tx.GetAttachmentTimestamp()/1000 > tx.GetTimestamp() {
		return tx.GetAttachmentTimestamp() / 1000
	}
	return tx.GetTimestamp()
}

func insertSorted(data []ApproveeStep, entry ApproveeStep) []ApproveeStep {
	i := sort.Search(len(data), func(i int) bool { return data[i].Projection >= entry.Projection })
	data = append(data, ApproveeStep{})
	copy(data[i+1:], data[i:])
	data[i] = entry
	return data
}

func insertDescSorted(data []ApproveeStep, entry ApproveeStep) []ApproveeStep {
	i := sort.Search(len(data), func(i int) bool { return data[i].Projection <= entry.Projection })
	data = append(data, ApproveeStep{})
	copy(data[i+1:], data[i:])
	data[i] = entry
	return data
}

var Events = tipselevents{}

type tipselevents struct {
	TipSelPerformed *events.Event
}

func configure(plugin *node.Plugin) {
	log = logger.NewLogger(plugin.Name)
}

func run(_ *node.Plugin) {
	// nothing
}
