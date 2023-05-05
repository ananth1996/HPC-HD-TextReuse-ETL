package main

import (
	"context"
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/apache/arrow/go/v10/parquet/file"
	"github.com/apache/arrow/go/v10/parquet/pqarrow"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"
)

func readTable(filename string) arrow.Table {
	rdr, err := file.OpenParquetFile(filename, true)
	if err != nil {
		log.Println("Can't open file", err)
		return nil
	}
	defer rdr.Close()
	arrowRdr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		log.Println("Can't open file", err)
		return nil
	}
	tbl, err := arrowRdr.ReadTable(context.Background())
	if err != nil {
		log.Println("Can't read file", err)
		return nil
	}
	return tbl
}

func chunkSlice(slice []int64, chunkCount int) [][]int64 {
	var chunks [][]int64
	chunkSize := len(slice)/chunkCount + 1
	for i := 0; i < len(slice); i += chunkSize {
		end := i + chunkSize

		// necessary check to avoid slicing beyond
		// slice capacity
		if end > len(slice) {
			end = len(slice)
		}

		chunks = append(chunks, slice[i:end])
	}
	return chunks
}

type Ret struct {
	chunkNumber int
	changed     int
}

func chineseWhispersSlice(myOrder []int64) int { //, chunkNumber int, r frand.RNG) int {
	//r.Shuffle(len(myOrder), func(i, j int) { myOrder[i], myOrder[j] = myOrder[j], myOrder[i] })
	/*	bar := p.AddBar(int64(len(myOrder)),
		mpb.BarRemoveOnComplete(),
		mpb.PrependDecorators(
			decor.Name(fmt.Sprintf("%v: ", chunkNumber)),
			decor.CountersNoUnit("%d / %d")),
		mpb.AppendDecorators(
			decor.Elapsed(decor.ET_STYLE_GO),
			decor.Name("<"),
			decor.AverageETA(decor.ET_STYLE_GO),
			decor.Name(", "),
			decor.AverageSpeed(0, "%.fnode/s"),
		),
	)*/
	changed := 0
	//top := make([]int64, 0)
	for _, e := range myOrder {
		if !active[e] {
			//bar.Increment()
			continue
		}
		active[e] = false
		beg := edgeOffsets[e]
		end := edgeOffsets[e+1]
		var nc int64
		if end == beg+1 {
			nc = clusters[edgeVals[beg]]
		} else if end == beg+2 {
			nc1 := clusters[edgeVals[beg]]
			nc2 := clusters[edgeVals[beg+1]]
			if nc1 > nc2 {
				nc = nc1
			} else {
				nc = nc2
			}
			/*			if nc1 != nc2 {
							active[e] = true
							if r.Intn(2) == 0 {
								nc = nc1
							} else {
								nc = nc2
							}
						} else {
							nc = nc1
						} */
		} else {
			topc := 0
			cc := make(map[int64]int)
			for j := beg; j < end; j++ {
				cc[clusters[edgeVals[j]]]++
			}
			/*			if len(cc) == 1 {
						for cl := range cc {
							nc = cl
						}
					} else {*/
			for cl, c := range cc {
				if c > topc {
					topc = c
					nc = cl
					/*						top = top[:0]
											top = append(top, cl) */
				} else if c == topc {
					//						top = append(top, cl)
					if cl > nc {
						nc = cl
					}
				}
			}
			/*				if len(top) > 1 {
									active[e] = true
								}
								nc = top[r.Intn(len(top))]
			} */
		}
		if clusters[e] != nc {
			clusters[e] = nc
			for j := beg; j < end; j++ {
				active[edgeVals[j]] = true
			}
			changed++
		}
		//bar.Increment()
	}
	return changed
}

func worker(chunkNumbers <-chan int, changed chan<- *Ret, wg *sync.WaitGroup) {
	var ok bool
	chunkNumber := -1
	//r := frand.New()
	defer wg.Done()
	for {
		chunkNumber, ok = <-chunkNumbers
		if !ok {
			break
		}
		ret := new(Ret)
		ret.chunkNumber = chunkNumber
		myOrder := orderChunks[chunkNumber]
		ret.changed = chineseWhispersSlice(myOrder) //, chunkNumber, *r)
		changed <- ret
	}
}

var p *mpb.Progress
var clusters []int64
var edgeOffsets []int32
var edgeVals []int64
var orderChunks [][]int64
var active []bool

type Cluster struct {
	Cluster int64 `parquet:"name=cluster, type=INT64"`
}

func writeClusters(numThreads int) {
	fw, err := local.NewLocalFileWriter("../data/clusters.parquet")
	if err != nil {
		log.Println("Can't create local file", err)
		return
	}

	//write
	pw, err := writer.NewParquetWriter(fw, new(Cluster), int64(numThreads))
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}
	pw.CompressionType = parquet.CompressionCodec_ZSTD
	for _, c := range clusters {
		clu := Cluster{
			Cluster: c,
		}
		if err = pw.Write(clu); err != nil {
			log.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		return
	}
	err = fw.Close()
	if err != nil {
		log.Println("Close error", err)
		return
	}
}

func main() {
	numThreads := runtime.GOMAXPROCS(0)
	log.Println("Reading graph from parquet")
	edgesTbl := readTable("../data/defrag_graph_adj_list.parquet")
	log.Println("Graph read")
	defer edgesTbl.Release()
	edgesColumn := edgesTbl.Column(0)
	if len(edgesColumn.Data().Chunks()) != 1 {
		log.Println("Expected parquet file to only have one chunk")
		return
	}
	edgeLists := edgesColumn.Data().Chunk(0).(*array.List)
	edgeOffsets = edgeLists.Offsets()
	edgeVals = edgeLists.ListValues().(*array.Int64).Int64Values()
	_, err := os.Stat("../data/clusters.parquet")
	order := make([]int64, edgeLists.Len())
	active = make([]bool, edgeLists.Len())
	for i := range active {
		order[i] = int64(i)
		active[i] = true
	}
	clusters = make([]int64, edgeLists.Len())
	if os.IsNotExist(err) {
		log.Println("No saved clusters exist, initializing as new")
		copy(clusters, order)
	} else {
		clc := readTable("../data/clusters.parquet").Column(0).Data().Chunks()
		log.Println("Reading clusters from saved data")
		i := 0
		for _, chunk := range clc {
			for _, v := range chunk.(*array.Int64).Int64Values() {
				clusters[i] = v
				i++
			}
		}
	}
	/*log.Println("Shuffling nodes")
	r := frand.New()
	r.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })*/
	var wg sync.WaitGroup
	p = mpb.New(mpb.WithWaitGroup(&wg))
	maxIters := 100000
	lastChanged := 0
	log.Println("Doing chinese whispers")
	bar := p.AddBar(int64(maxIters),
		mpb.PrependDecorators(
			decor.CountersNoUnit("%d / %d, changed: "),
			decor.Any(func(st decor.Statistics) string {
				return strconv.FormatInt(int64(lastChanged), 10)
			}),
		),
		mpb.AppendDecorators(
			decor.Elapsed(decor.ET_STYLE_GO),
			decor.Name("<"),
			decor.AverageETA(decor.ET_STYLE_GO),
			decor.Name(", "),
			decor.AverageSpeed(0, "%.2f it/s"),
		))
	orderChunks = chunkSlice(order, numThreads)
	chunkChan := make(chan int, numThreads)
	changedChan := make(chan *Ret, numThreads)
	defer close(changedChan)
	wg.Add(numThreads)
	for i := 0; i < numThreads; i++ {
		go worker(chunkChan, changedChan, &wg)
	}
	for j := range orderChunks {
		chunkChan <- j
	}
	iters := 0
	currentChanged := 0
	for {
		ret := <-changedChan
		currentChanged += ret.changed
		iters++
		if iters%numThreads == 0 {
			lastChanged = currentChanged
			bar.Increment()
			/*if iters%(numThreads*100) == 0 {
				writeClusters(numThreads)
			}*/
			if iters == maxIters || currentChanged == 0 {
				close(chunkChan)
				break
			}
			currentChanged = 0
		}
		chunkChan <- ret.chunkNumber
	}
	if iters < maxIters {
		/*
			for i := range active {
				active[i] = true
			}*/
		for {
			lastChanged = chineseWhispersSlice(order) //, -1, *r)
			bar.Increment()
			iters++
			if iters == maxIters {
				break
			}
			if lastChanged == 0 {
				bar.IncrInt64(int64(maxIters) - bar.Current())
				break
			}
		}
	}
	p.Wait()
	log.Printf("Done after %v iterations\n", iters)
	log.Println("Writing clusters")
	writeClusters(numThreads)
	log.Println("Write Finished")
}
