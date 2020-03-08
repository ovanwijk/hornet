package tangle

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/iotaledger/hive.go/bitmask"
	"github.com/iotaledger/hive.go/objectstorage"
	"github.com/iotaledger/iota.go/trinary"

	"github.com/gohornet/hornet/packages/database"
	"github.com/gohornet/hornet/packages/metrics"
	"github.com/gohornet/hornet/packages/model/hornet"
	"github.com/gohornet/hornet/packages/model/milestone_index"
	"github.com/gohornet/hornet/packages/profile"
)

var (
	bundleStorage *objectstorage.ObjectStorage
)

func databaseKeyForBundle(tailTxHash trinary.Hash) []byte {
	return trinary.MustTrytesToBytes(tailTxHash)[:49]
}

func bundleFactory(key []byte) objectstorage.StorableObject {
	return &Bundle{
		tailTx: trinary.MustBytesToTrytes(key[:49], 81),
		txs:    make(map[trinary.Hash]struct{}),
	}
}

func GetBundleStorageSize() int {
	return bundleStorage.GetSize()
}

func configureBundleStorage() {

	opts := profile.GetProfile().Caches.Bundles

	bundleStorage = objectstorage.New(
		database.GetHornetBadgerInstance(),
		[]byte{DBPrefixBundles},
		bundleFactory,
		objectstorage.CacheTime(time.Duration(opts.CacheTimeMs)*time.Millisecond),
		objectstorage.PersistenceEnabled(true),
		objectstorage.LeakDetectionEnabled(opts.LeakDetectionOptions.Enabled,
			objectstorage.LeakDetectionOptions{
				MaxConsumersPerObject: opts.LeakDetectionOptions.MaxConsumersPerObject,
				MaxConsumerHoldTime:   time.Duration(opts.LeakDetectionOptions.MaxConsumerHoldTimeSec) * time.Second,
			}),
	)
}

// ObjectStorage interface
func (bundle *Bundle) Update(other objectstorage.StorableObject) {
	panic("Bundle should never be updated")
}

func (bundle *Bundle) GetStorageKey() []byte {
	return databaseKeyForBundle(bundle.tailTx)
}

func (bundle *Bundle) MarshalBinary() (data []byte, err error) {

	/*
		 1 byte  	   				metadata
		 8 bytes uint64 			lastIndex
		 8 bytes uint64 			txCount
		 8 bytes uint64 			ledgerChangesCount
		49 bytes					bundleHash
		49 bytes					headTx
		49 bytes                 	txHashes		(x txCount)
		49 bytes + 8 bytes uint64 	ledgerChanges	(x ledgerChangesCount)
	*/

	txCount := len(bundle.txs)
	ledgerChangesCount := len(bundle.ledgerChanges)

	value := make([]byte, 172+txCount*49+57*ledgerChangesCount)

	value[0] = byte(bundle.metadata)
	binary.LittleEndian.PutUint64(value[1:], bundle.lastIndex)
	binary.LittleEndian.PutUint64(value[9:], uint64(txCount))
	binary.LittleEndian.PutUint64(value[17:], uint64(ledgerChangesCount))
	copy(value[25:74], trinary.MustTrytesToBytes(bundle.hash))
	copy(value[74:123], trinary.MustTrytesToBytes(bundle.headTx))

	offset := 123
	for txHash := range bundle.txs {
		copy(value[offset:offset+49], trinary.MustTrytesToBytes(txHash))
		offset += 49
	}

	for addr, change := range bundle.ledgerChanges {
		copy(value[offset:offset+49], trinary.MustTrytesToBytes(addr))
		offset += 49
		binary.LittleEndian.PutUint64(value[offset:], uint64(change))
		offset += 8
	}

	return value, nil
}

func (bundle *Bundle) UnmarshalBinary(data []byte) error {

	/*
		 1 byte  	   				metadata
		 8 bytes uint64 			lastIndex
		 8 bytes uint64 			txCount
		 8 bytes uint64 			ledgerChangesCount
		49 bytes					bundleHash
		49 bytes					headTx
		49 bytes                 	txHashes		(x txCount)
		49 bytes + 8 bytes uint64 	ledgerChanges	(x ledgerChangesCount)
	*/

	bundle.metadata = bitmask.BitMask(data[0])
	bundle.lastIndex = binary.LittleEndian.Uint64(data[1:9])
	txCount := int(binary.LittleEndian.Uint64(data[9:17]))
	ledgerChangesCount := int(binary.LittleEndian.Uint64(data[17:25]))
	bundle.hash = trinary.MustBytesToTrytes(data[25:74], 81)
	bundle.headTx = trinary.MustBytesToTrytes(data[74:123], 81)

	offset := 123
	for i := 0; i < txCount; i++ {
		bundle.txs[trinary.MustBytesToTrytes(data[offset:offset+49], 81)] = struct{}{}
		offset += 49
	}

	if ledgerChangesCount > 0 {
		bundle.ledgerChanges = make(map[trinary.Trytes]int64, ledgerChangesCount)
	}

	for i := 0; i < ledgerChangesCount; i++ {
		address := trinary.MustBytesToTrytes(data[offset:offset+49], 81)
		offset += 49
		balance := int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
		offset += 8
		bundle.ledgerChanges[address] = balance
	}

	return nil
}

// Cached Object
type CachedBundle struct {
	objectstorage.CachedObject
}

type CachedBundles []*CachedBundle

func (cachedBundles CachedBundles) Retain() CachedBundles {
	cachedResult := CachedBundles{}
	for _, cachedBundle := range cachedBundles {
		cachedResult = append(cachedResult, cachedBundle.Retain())
	}
	return cachedResult
}

func (cachedBundles CachedBundles) Release() {
	for _, cachedBundle := range cachedBundles {
		cachedBundle.Release()
	}
}

func (c *CachedBundle) Retain() *CachedBundle {
	return &CachedBundle{c.CachedObject.Retain()}
}

func (c *CachedBundle) ConsumeBundle(consumer func(*Bundle)) {

	c.Consume(func(object objectstorage.StorableObject) {
		consumer(object.(*Bundle))
	})
}

func (c *CachedBundle) GetBundle() *Bundle {
	return c.Get().(*Bundle)
}

// bundle +1
func GetCachedBundleOrNil(tailTxHash trinary.Hash) *CachedBundle {
	cachedBundle := bundleStorage.Load(databaseKeyForBundle(tailTxHash)) // bundle +1
	if !cachedBundle.Exists() {
		cachedBundle.Release() // bundle -1
		return nil
	}
	return &CachedBundle{CachedObject: cachedBundle}
}

// bundle +-0
func ContainsBundle(tailTxHash trinary.Hash) bool {
	return bundleStorage.Contains(databaseKeyForBundle(tailTxHash))
}

// bundle +-0
func DeleteBundle(tailTxHash trinary.Hash) {
	bundleStorage.Delete(databaseKeyForBundle(tailTxHash))
}

func ShutdownBundleStorage() {
	bundleStorage.Shutdown()
}

////////////////////////////////////////////////////////////////////////////////////

// GetBundles returns all existing bundle instances for that bundle hash
// bundle +1
func GetBundles(bundleHash trinary.Hash) CachedBundles {

	var cachedBndls CachedBundles

	for _, txTailHash := range GetBundleTailTransactionHashes(bundleHash) {
		cachedBndl := GetCachedBundleOrNil(txTailHash) // bundle +1
		if cachedBndl == nil {
			continue
		}

		cachedBndls = append(cachedBndls, cachedBndl)
	}

	if len(cachedBndls) == 0 {
		return nil
	}

	return cachedBndls
}

// GetCachedBundleOfTailTransactionOrNil gets the bundle this tail transaction is present in or nil.
// bundle +1
func GetCachedBundleOfTailTransactionOrNil(tailTxHash trinary.Hash) *CachedBundle {
	return GetCachedBundleOrNil(tailTxHash) // bundle +1
}

// GetBundlesOfTransactionOrNil gets all bundle instances in which this transaction is present.
// A transaction can be in multiple bundle instances simultaneously
// due to the nature of reattached transactions being able to form infinite amount of bundles
// which attach to the same underlying bundle transaction. For example it is possible to reattach
// a bundle's tail transaction directly "on top" of the origin one.
// bundle +1
func GetBundlesOfTransactionOrNil(txHash trinary.Hash) CachedBundles {

	var cachedBndls CachedBundles

	cachedTx := GetCachedTransactionOrNil(txHash) // tx +1
	if cachedTx == nil {
		return nil
	}
	defer cachedTx.Release() // tx -1

	if cachedTx.GetTransaction().IsTail() {
		cachedBndl := GetCachedBundleOfTailTransactionOrNil(txHash) // bundle +1
		if cachedBndl == nil {
			return nil
		}
		return append(cachedBndls, cachedBndl)
	}

	tailTxHashes := getTailApproversOfSameBundle(cachedTx.GetTransaction().Tx.Bundle, txHash)
	for _, tailTxHash := range tailTxHashes {
		cachedBndl := GetCachedBundleOfTailTransactionOrNil(tailTxHash) // bundle +1
		if cachedBndl == nil {
			continue
		}
		cachedBndls = append(cachedBndls, cachedBndl)
	}

	if len(cachedBndls) == 0 {
		return nil
	}

	return cachedBndls
}

////////////////////////////////////////////////////////////////////////////////

// tx +1
func AddTransactionToStorage(hornetTx *hornet.Transaction, firstSeenLatestMilestoneIndex milestone_index.MilestoneIndex, requested bool) (cachedTx *CachedTransaction, alreadyAdded bool) {

	cachedTx, isNew := StoreTransactionIfAbsent(hornetTx) // tx +1
	if !isNew {
		return cachedTx, true
	}

	// Store the tx in the bundleTransactionsStorage
	StoreBundleTransaction(cachedTx.GetTransaction().Tx.Bundle, cachedTx.GetTransaction().GetHash(), cachedTx.GetTransaction().IsTail()).Release()

	StoreApprover(cachedTx.GetTransaction().GetTrunk(), cachedTx.GetTransaction().GetHash()).Release()
	if cachedTx.GetTransaction().GetTrunk() != cachedTx.GetTransaction().GetBranch() {
		StoreApprover(cachedTx.GetTransaction().GetBranch(), cachedTx.GetTransaction().GetHash()).Release()
	}

	StoreTag(cachedTx.GetTransaction().Tx.Tag, cachedTx.GetTransaction().GetHash()).Release()

	StoreAddress(cachedTx.GetTransaction().Tx.Address, cachedTx.GetTransaction().GetHash()).Release()

	// Store only non-requested transactions, since all requested transactions are confirmed by a milestone anyway
	// This is only used to delete unconfirmed transactions from the database at pruning
	if !requested {
		StoreFirstSeenTx(firstSeenLatestMilestoneIndex, cachedTx.GetTransaction().GetHash()).Release()
	}

	// If the transaction is part of a milestone, the bundle must be created here
	// Otherwise, bundles are created if tailTx becomes solid
	if IsMaybeMilestoneTx(cachedTx.Retain()) { // tx pass +1
		tryConstructBundle(cachedTx.Retain(), false)
	}

	return cachedTx, false
}

func tryConstructBundle(cachedTx *CachedTransaction, isSolidTail bool) {
	defer cachedTx.Release() // tx -1

	if !isSolidTail && !cachedTx.GetTransaction().IsTail() {
		// If Tx is not a tail, search all tailTx that reference this tx and try to create the bundles
		tailTxHashes := getTailApproversOfSameBundle(cachedTx.GetTransaction().Tx.Bundle, cachedTx.GetTransaction().GetHash())
		for _, tailTxHash := range tailTxHashes {
			cachedTailTx := GetCachedTransactionOrNil(tailTxHash) // tx +1
			if cachedTailTx == nil {
				continue
			}

			tryConstructBundle(cachedTailTx.Retain(), false) // tx pass +1
			cachedTailTx.Release()                           // tx -1
		}
		return
	}

	if ContainsBundle(cachedTx.GetTransaction().GetHash()) {
		// Bundle already exists
		return
	}

	// create a new bundle instance
	bndl := &Bundle{
		tailTx:    cachedTx.GetTransaction().GetHash(),
		metadata:  bitmask.BitMask(0),
		lastIndex: cachedTx.GetTransaction().Tx.LastIndex,
		hash:      cachedTx.GetTransaction().Tx.Bundle,
		txs:       make(map[trinary.Hash]struct{}),
	}

	bndl.txs[cachedTx.GetTransaction().GetHash()] = struct{}{}

	// check whether it is a bundle with only one transaction
	if cachedTx.GetTransaction().Tx.CurrentIndex == cachedTx.GetTransaction().Tx.LastIndex {
		bndl.headTx = cachedTx.GetTransaction().GetHash()
	} else {
		// lets try to complete the bundle by assigning txs into this bundle
		if !constructBundle(bndl, cachedTx.Retain()) { // tx pass +1
			if isSolidTail {
				panic("Can't create bundle, but tailTx is solid")
			}
			return
		}
	}

	newlyAdded := false
	wasMilestone := false
	var spentAddresses []trinary.Hash
	var invalidMilestoneErr error
	cachedBndl := bundleStorage.ComputeIfAbsent(bndl.GetStorageKey(), func(key []byte) objectstorage.StorableObject { // bundle +1

		newlyAdded = true
		if bndl.validate() {
			metrics.SharedServerMetrics.IncrValidatedBundlesCount()

			bndl.calcLedgerChanges()

			if !bndl.IsValueSpam() {
				spentAddressesEnabled := GetSnapshotInfo().IsSpentAddressesEnabled()
				for addr, change := range bndl.GetLedgerChanges() {
					if change < 0 {
						if spentAddressesEnabled && MarkAddressAsSpent(addr) {
							metrics.SharedServerMetrics.IncrSeenSpentAddrCount()
						}
						spentAddresses = append(spentAddresses, addr)
					}
				}
			}

			if IsMaybeMilestone(bndl.GetTail()) { // tx pass +1
				if isMilestone, err := CheckIfMilestone(bndl); err != nil {
					invalidMilestoneErr = err
				} else {
					if isMilestone {
						wasMilestone = true
						StoreMilestone(bndl).Release() // bundle pass +1, milestone +-0
					}
				}
			}
		}

		bndl.Persist()
		bndl.SetModified()

		return bndl
	})

	if newlyAdded {
		if invalidMilestoneErr != nil {
			Events.ReceivedInvalidMilestone.Trigger(fmt.Errorf("Invalid milestone detected! Err: %s", invalidMilestoneErr.Error()))
		}

		for _, addr := range spentAddresses {
			Events.AddressSpent.Trigger(addr)
		}

		if wasMilestone {
			Events.ReceivedValidMilestone.Trigger(&CachedBundle{CachedObject: cachedBndl}) // bundle pass +1
		}
	}

	cachedBndl.Release() // bundle -1
}

// Remaps transactions into the given bundle by traversing from the given start transaction through the trunk.
func constructBundle(bndl *Bundle, cachedStartTx *CachedTransaction) bool {

	cachedCurrentTx := cachedStartTx

	// iterate as long as the bundle isn't complete and prevent cyclic transactions (such as the genesis)
	for cachedCurrentTx.GetTransaction().GetHash() != cachedCurrentTx.GetTransaction().GetTrunk() && !bndl.isComplete() && !cachedCurrentTx.GetTransaction().IsHead() {

		// check whether the trunk transaction is known to the transaction storage.
		if !ContainsTransaction(cachedCurrentTx.GetTransaction().GetTrunk()) {
			cachedCurrentTx.Release() // tx -1
			return false
		}

		trunkTx := loadBundleTxIfExistsOrPanic(cachedCurrentTx.GetTransaction().GetTrunk(), bndl.hash) // tx +1

		// check whether trunk is in bundle instance already
		if _, trunkAlreadyInBundle := bndl.txs[cachedCurrentTx.GetTransaction().GetTrunk()]; trunkAlreadyInBundle {
			cachedCurrentTx.Release() // tx -1
			cachedCurrentTx = trunkTx
			continue
		}

		if trunkTx.GetTransaction().Tx.Bundle != cachedStartTx.GetTransaction().Tx.Bundle {
			trunkTx.Release() // tx -1

			// Tx has invalid structure, but is "complete"
			break
		}

		// assign as head if last tx
		if trunkTx.GetTransaction().IsHead() {
			bndl.headTx = trunkTx.GetTransaction().GetHash()
		}

		// assign trunk tx to this bundle
		bndl.txs[trunkTx.GetTransaction().GetHash()] = struct{}{}

		// modify and advance to perhaps complete the bundle
		bndl.SetModified(true)
		cachedCurrentTx.Release() // tx -1
		cachedCurrentTx = trunkTx
	}

	cachedCurrentTx.Release() // tx -1
	return true
}

// Create a new bundle instance as soon as a tailTx gets solid
func OnTailTransactionSolid(cachedTx *CachedTransaction) {
	tryConstructBundle(cachedTx, true) // tx +-0 (it has +1 and will be released in tryConstructBundle)
}