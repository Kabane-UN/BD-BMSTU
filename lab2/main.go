package main

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Узел распределенной системы
type Node struct {
	id   int
	mu   sync.RWMutex
	data map[string]string

	// Блокировки 2PL: key -> TxID
	lockMu sync.Mutex
	locks  map[string]string

	// Буфер транзакции: TxID -> map[key]pendingOp
	// pendingOp хранит и значение, и тип (был ли это Put или Delete)
	pendingMu sync.Mutex
	pending   map[string]map[string]pendingChange
}

// Значение и тип операции
type pendingChange struct {
	val   string
	isDel bool // Является ли операция удалением
}

func NewNode(id int) *Node {
	return &Node{
		id:      id,
		data:    make(map[string]string),
		locks:   make(map[string]string),
		pending: make(map[string]map[string]pendingChange),
	}
}

// Задержка сети
func networkDelay() {
	time.Sleep(time.Duration(10+rand.Intn(90)) * time.Millisecond)
}

// Prepare вызывается параллельно из горутин координатора
func (n *Node) PrepareOp(txID string, op Operation) (string, error) {
	networkDelay()

	n.lockMu.Lock()
	// Strict 2PL: Проверяем, не занят ли ключ другой транзакцией
	owner, busy := n.locks[op.Key]
	if busy && owner != txID {
		n.lockMu.Unlock()
		return "", errors.New("lock conflict: key is held by " + owner)
	}
	// Захватываем лок (No-wait)
	n.locks[op.Key] = txID
	n.lockMu.Unlock()

	switch op.Type {
	// Если чтение, то просто читаем
	case OpRead:
		// Сначала смотрим в свой буфер (Read Your Own Writes)
		n.pendingMu.Lock()
		myChanges, exists := n.pending[txID]
		if exists {
			if change, ok := myChanges[op.Key]; ok {
				n.pendingMu.Unlock()
				if change.isDel {
					return "", nil
				} // Мы сами это удалили
				return change.val, nil // Мы сами это записали
			}
		}
		n.pendingMu.Unlock()

		// Если у себя не нашли, читаем из общего хранилища
		n.mu.RLock()
		val := n.data[op.Key]
		n.mu.RUnlock()
		return val, nil

	// Если изменение, то записываем в буффер
	case OpPut, OpDelete:
		n.pendingMu.Lock()
		if n.pending[txID] == nil {
			n.pending[txID] = make(map[string]pendingChange)
		}
		n.pending[txID][op.Key] = pendingChange{
			val:   op.Value,
			isDel: (op.Type == OpDelete),
		}
		n.pendingMu.Unlock()
	}
	return "", nil
}

func (n *Node) Commit(txID string) {
	networkDelay()

	n.pendingMu.Lock()
	changes, ok := n.pending[txID]
	n.pendingMu.Unlock()

	// Записываем буфер на "диск"
	if ok {
		n.mu.Lock()
		for key, change := range changes {
			if change.isDel {
				delete(n.data, key)
			} else {
				n.data[key] = change.val
			}
		}
		n.mu.Unlock()
	}

	// Освобождаем локи
	n.lockMu.Lock()
	for k, owner := range n.locks {
		if owner == txID {
			delete(n.locks, k)
		}
	}
	n.lockMu.Unlock()

	// Освобождаем буфер
	n.pendingMu.Lock()
	delete(n.pending, txID)
	n.pendingMu.Unlock()
}

func (n *Node) Rollback(txID string) {
	// 1. Сначала находим все ключи, которые эта транзакция пыталась изменить
	n.pendingMu.Lock()
	changes, hasPending := n.pending[txID]
	delete(n.pending, txID) // Сразу удаляем черновик
	n.pendingMu.Unlock()

	n.lockMu.Lock()
	defer n.lockMu.Unlock()

	// 2. Если были изменения в буфере, снимаем локи с этих ключей
	if hasPending {
		for key := range changes {
			if owner, busy := n.locks[key]; busy && owner == txID {
				delete(n.locks, key)
			}
		}
	}

	// 3. Дополнительная проверка: транзакция могла взять лок на Read,
	// но ничего не записать в pending. Проходим по всем локам ноды.
	for key, owner := range n.locks {
		if owner == txID {
			delete(n.locks, key)
		}
	}
}

type OpType int

const (
	OpPut OpType = iota
	OpDelete
	OpRead
)

type Operation struct {
	Type    OpType
	NodeIdx int
	Key     string
	Value   string // Только для Put
}

type Coordinator struct {
	nodes []*Node
}

func (c *Coordinator) Execute(txID string, ops []Operation) ([]string, error) {
	// Вывод для операции Read
	results := make([]string, len(ops))

	// Все учавствующие ноды
	involvedNodes := make(map[int]bool)

	fmt.Printf("[Tx %s] Phase 1: Executing operations...\n", txID)
	for i, op := range ops {
		// Помечаем ноду как вовлеченную и выполняем фазу подготовки
		involvedNodes[op.NodeIdx] = true
		val, err := c.nodes[op.NodeIdx].PrepareOp(txID, op)

		// В случае конфоликта - откат
		if err != nil {
			fmt.Printf("[Tx %s] Failed on op %d. Rolling back...\n", txID, i)
			c.Rollback(txID, involvedNodes)
			return nil, err
		}
		results[i] = val
	}

	fmt.Printf("[Tx %s] Phase 2: Committing on all nodes...\n", txID)
	for nIdx := range involvedNodes {
		c.nodes[nIdx].Commit(txID)
	}
	return results, nil
}

// Откат транзакции - сигнал на откат всем вовлеченным нодам
func (c *Coordinator) Rollback(txID string, involvedNodes map[int]bool) {
	fmt.Printf("[Tx %s] Rolling back on involved nodes...\n", txID)
	for nIdx := range involvedNodes {
		c.nodes[nIdx].Rollback(txID)
	}
}

func main() {
	// Сценарий: Конфликт транзакций
	// Tx1 занимает ключ, Tx2 пытается его взять во время Prepare
	fmt.Println("--- Test 1: Basic Conflict ---")
	nodes := []*Node{NewNode(0), NewNode(1), NewNode(2)}
	coord := &Coordinator{nodes: nodes}

	wg := sync.WaitGroup{}

	wg.Go(func() {
		// Транзакция 1: пишет в Node 0, key "balance"
		_, err := coord.Execute("TX-1", []Operation{
			{Type: OpPut, NodeIdx: 0, Key: "balance", Value: "100"},
		})
		if err == nil {
			fmt.Println("TX-1 success")
		}
	})

	time.Sleep(20 * time.Millisecond) // Даем TX-1 успеть захватить лок

	wg.Go(func() {
		// Транзакция 2: пытается писать в тот же ключ на Node 0
		_, err := coord.Execute("TX-2", []Operation{
			{Type: OpPut, NodeIdx: 0, Key: "balance", Value: "200"},
		})
		if err != nil {
			fmt.Printf("TX-2 failed as expected: %v\n", err)
		}
	})

	wg.Wait()
	TestMultiNodeRollback()
	TestReadWriteConflict()
	TestParallelIndependent()
	TestReadYourOwnWrites()
	TestFandomRead()
}

func TestMultiNodeRollback() {
	// Тоже самое, но нод несколько (частичное пересечение)

	nodes := []*Node{NewNode(0), NewNode(1), NewNode(2)}
	coord := &Coordinator{nodes: nodes}
	fmt.Println("\n--- Running Test: Multi-Node Atomic Rollback ---")

	// 1. TX-X захватывает ключ на Узле 2
	wg := sync.WaitGroup{}
	wg.Go(func() {
		coord.Execute("TX-X",
			[]Operation{
				{Type: OpPut, NodeIdx: 2, Key: "storage", Value: "busy"},
			})
	})
	time.Sleep(10 * time.Millisecond)

	// 2. TX-Y пытается обновить Узел 0, 1 и 2.
	// На Узле 2 она встретит конфликт.
	_, err := coord.Execute("TX-Y",
		[]Operation{
			{Type: OpPut, NodeIdx: 2, Key: "storage", Value: "update-attempt"},
			{Type: OpPut, NodeIdx: 1, Key: "data", Value: "new-0"},
			{Type: OpPut, NodeIdx: 0, Key: "data", Value: "new-1"},
		})

	wg.Wait()

	if err != nil {
		fmt.Println("TX-Y aborted as expected. Checking consistency...")
		fmt.Printf("Node 0 data (should be empty): '%s'\n", nodes[0].data["data"])
		fmt.Printf("Node 1 data (should be empty): '%s'\n", nodes[1].data["data"])
		fmt.Printf("Node 2 storage (not empty): '%s'\n", nodes[2].data["storage"])
	}
}

func TestReadWriteConflict() {
	// Read uncommited
	nodes := []*Node{NewNode(0), NewNode(1), NewNode(2)}
	coord := &Coordinator{nodes: nodes}
	fmt.Println("\n--- Running Test: Read-Write Conflict ---")

	// TX-1 начинает запись
	go coord.Execute("TX-WRITE",
		[]Operation{
			{Type: OpPut, NodeIdx: 0, Key: "config", Value: "v2"},
		})

	time.Sleep(20 * time.Millisecond)

	// TX-READ читает тот же ключ
	res, err := coord.Execute("TX-READ",
		[]Operation{
			{Type: OpRead, NodeIdx: 0, Key: "config"},
		})

	if err != nil {
		fmt.Println("TX-READ was rejected because TX-WRITE is in progress. Correct!")
	}
	fmt.Printf("TX-READ res: %#v \n", res)
}

func TestParallelIndependent() {
	// Нормальное независимое изменение
	nodes := []*Node{NewNode(0), NewNode(1)}
	coord := &Coordinator{nodes: nodes}
	fmt.Println("\n--- Running Test: Parallel Independent Transactions ---")
	start := time.Now()
	wg := sync.WaitGroup{}

	// Эти транзакции работают с разными ключами
	wg.Go(func() {
		coord.Execute("TX-A", []Operation{
			{Type: OpPut, NodeIdx: 0, Key: "key1", Value: "val1"},
		})

	})
	wg.Go(func() {
		coord.Execute("TX-B", []Operation{
			{Type: OpPut, NodeIdx: 0, Key: "key2", Value: "val2"},
		})
	})

	wg.Wait()
	fmt.Printf("Both finished in %v (should be fast, ~100-200ms)\n", time.Since(start))
}

func TestReadYourOwnWrites() {
	// Проверка чтения в рамках одной транзакции
	nodes := []*Node{NewNode(0)}
	coord := &Coordinator{nodes: nodes}
	fmt.Println("\n--- Running Test: Read Your Own Writes ---")
	wg := sync.WaitGroup{}

	// Чтение изменений в одной транзакиции
	wg.Go(func() {
		res, err := coord.Execute("TX-READ", []Operation{
			{Type: OpPut, NodeIdx: 0, Key: "key1", Value: "val1"},
			{Type: OpRead, NodeIdx: 0, Key: "key1"},
			{Type: OpPut, NodeIdx: 0, Key: "key1", Value: "val2"},
			{Type: OpRead, NodeIdx: 0, Key: "key1"},
			{Type: OpDelete, NodeIdx: 0, Key: "key1"},
			{Type: OpRead, NodeIdx: 0, Key: "key1"},
		})
		if err == nil {
			fmt.Printf("TX-READ res: %#v \n", res)
		}

	})
	wg.Wait()
}

func TestFandomRead() {
	// Проверка на фонтомное чтение
	nodes := []*Node{NewNode(0)}
	coord := &Coordinator{nodes: nodes}
	fmt.Println("\n--- Running Test: Read Your Own Writes ---")
	wg := sync.WaitGroup{}

	// Чтение изменений в одной транзакиции
	wg.Go(func() {
		res, err := coord.Execute("TX-READ", []Operation{
			{Type: OpPut, NodeIdx: 0, Key: "key1", Value: "val1"},
			{Type: OpRead, NodeIdx: 0, Key: "key1"},
			{Type: OpRead, NodeIdx: 0, Key: "key2"},
			{Type: OpRead, NodeIdx: 0, Key: "key1"},
			{Type: OpRead, NodeIdx: 0, Key: "key2"},
		})
		if err == nil {
			fmt.Printf("TX-READ res: %#v \n", res)
		}
	})
	time.Sleep(200 * time.Millisecond)

	// Вторая транзакция добавляет ключь
	wg.Go(func() {
		_, err := coord.Execute("TX-UPDATE", []Operation{
			{Type: OpPut, NodeIdx: 0, Key: "key2", Value: "val2"},
		})
		if err != nil {
			fmt.Println("TX-UPDATE was rejected because TX-READ is in progress. Correct!")
		}
	})
	wg.Wait()
}
