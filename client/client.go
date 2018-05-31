package main

import (
	"redisProxy/gen-go/proxy"
	"git.apache.org/thrift.git/lib/go/thrift"
	"log"
	"fmt"
	"sync"
	"time"
	"sync/atomic"
	"strconv"
	"github.com/samuel/go-zookeeper/zk"
	"golang.org/x/net/context"
	"github.com/montanaflynn/stats"
	"compress/zlib"
)

func main() {
	var num int
	fmt.Scanln(&num)
	setTest(num)
	getTrueTest(num)
	getFalseTest(num)
	hsetTest(num)
	hgetTest(num)
	hmsetTest(num)
	hmgetTest(num)
}

func getAddr() ([]string, error) {
	//server端host
	conn, _, err := zk.Connect([]string{"localhost:2181"}, time.Second * 5)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	b, _, err := conn.Children("/proxy")

	return b, err
}

func setTest(num int)  {
	transportFactory := thrift.NewTZlibTransportFactoryWithFactory(zlib.BestSpeed, thrift.NewTBufferedTransportFactory(8192))
	protocolFactory := thrift.NewTCompactProtocolFactory()

	var wg sync.WaitGroup

	n := 1000
	d := make([][]int64, n, n)
	m := num

	wg.Add(n * m)

	var trans uint64
	var transOK uint64

	addr, err := getAddr()
	if err != nil {
		return
	}

	totalT := time.Now().UnixNano()
	for i := 0; i < n; i++ {
		dt := make([]int64, 0, m)
		d = append(d, dt)

		go func(i int) {
			transport, err := thrift.NewTSocket(addr[i%len(addr)])
			if err != nil {
				log.Fatalf("did not connect: %v", err)
			}

			useTransport, _ := transportFactory.GetTransport(transport)
			client := proxy.NewServiceClientFactory(useTransport, protocolFactory)
			if err := transport.Open(); err != nil {
				log.Println(err)
			}
			defer transport.Close()

			for j := 0; j < m; j++ {
				k := strconv.Itoa(j)
				reqBody := proxy.SetRequest{Table: "ATable", Key: []byte(k), Value: []byte(k + "p5wj1xc11kNxyQGoGUY3CYVZAvyC3vzT1wC6L4dUwB23hL7dWoBv4Vp54cBEYh3t09cu6ZMbspiArPVtIH7CJdgIZvkrC5bb9n8YJA53mbxb97iYKtKStBecMPvGhhG6eZvvbt16YR9Szky93ZF3C5RVRlcPC7pJRJzmpZyUqETV8C3xsQZrJ7QOvT81vJNhjqUudufCOm8POvtpCLPcYmC8Ccfu5xyR7pQB6dDqlqfUzydzSBr9LsBr8qb8mRUekZk0jKlzmgKPQaWuKgkDOlV55r1crUbYmmjtU0sfHKcC4oVcYakgfpccng0SUcScYVid0QJqcVjW3fJbNNE48qdpFBedKHlo1nreLiUYLEP818FiUtYkTpP2wYtgzFMxfrxWH2Ju0Pyuvcb1ROiKCPE6e6vryz8iN1qXkxqWnsy3vTZ9JR3cCrpW9U8VJ5rlcTLOhgAsShrhBimwMKyMxVkYbi7P1bbjCUfXCswWyuTIpM5w2XtfbAuNdH7V9dENgXpoSxAg5CBsEbguqcDOink2JR6h0bwTJKWShT7Qf84gOMDuysTIH6wbjU5a1pGDVpgSZ4lgpPLg1aUgRsTVhDEA8KsuaQyLniFubFF1imD2lo5DutcAlhiYxzgQX9WePjzlirBfe37duVVGWdYzWOLVkt7IRRnqjOnRcQKWotyTNlDS913pfB9S7qLObnsD99HCrMpHxXOM0uavK0tZVrCt1W5dnEZVAf9nh4h5uAuy88glpJYibFrhUUKNdiGWXysZcte6RYKvbPGjKdE8VM5YDZ9G4U37itbayYGGKgvhXr4KcxM3aJFazDt6Q0Rx96ODPe1CqW56vBMSSc137icrNA6NbpqNTImB0zs1W1wNQCo4b76Ep18F1dZ7i07xTHqBY0ST5RjVBT6TXY5bLe62hUq02Uw4Z6wfmlozxmDSuUdliJ39xAqEAOypd1L0LWN9T1NsL5q2uvyDJIt8BJSbXw4Hd7Q0srWUaYiv916")}
				//reqBody := proxy.SetRequest{Table: "ATable", Key: []byte(k), Value: []byte(k + "ppp")}
				t := time.Now().UnixNano()
				reply, _ := client.SetUserFeature(context.Background(), &reqBody)
				t = time.Now().UnixNano() - t

				d[i] = append(d[i], t)

				if reply.Code == 0 && reply.Msg == "" {
					atomic.AddUint64(&transOK, 1)
				} else {
					fmt.Println(reply.Msg)
				}

				atomic.AddUint64(&trans, 1)
				wg.Done()
			}
		}(i)
	}

	wg.Wait()
	totalT = time.Now().UnixNano() - totalT
	totalT = totalT / 1000000
	log.Printf("took %d ms for %d requests\n", totalT, n*m)

	totalD := make([]int64, 0, n*m)
	for _, k := range d {
		totalD = append(totalD, k...)
	}
	totalD2 := make([]float64, 0, n*m)
	for _, k := range totalD {
		totalD2 = append(totalD2, float64(k))
	}

	mean, _ := stats.Mean(totalD2)
	median, _ := stats.Median(totalD2)
	max, _ := stats.Max(totalD2)
	min, _ := stats.Min(totalD2)
	p95, _ := stats.Percentile(totalD2, 95)

	log.Println("set")
	log.Printf("sent     requests    : %d\n", n*m)
	log.Printf("received requests    : %d\n", atomic.LoadUint64(&trans))
	log.Printf("received requests_OK : %d\n", atomic.LoadUint64(&transOK))
	log.Printf("throughput  (TPS)    : %d\n", int64(n*m)*1000/totalT)
	log.Printf("mean: %.f ns, median: %.f ns, max: %.f ns, min: %.f ns, p95: %.f ns\n", mean, median, max, min, p95)
}

func getTrueTest(num int)  {
	transportFactory := thrift.NewTZlibTransportFactoryWithFactory(zlib.BestSpeed, thrift.NewTBufferedTransportFactory(8192))
	protocolFactory := thrift.NewTCompactProtocolFactory()

	var wg sync.WaitGroup

	n := 1000
	d := make([][]int64, n, n)
	m := num

	wg.Add(n * m)

	var trans uint64
	var transOK uint64

	addr, err := getAddr()
	if err != nil {
		return
	}

	totalT := time.Now().UnixNano()
	for i := 0; i < n; i++ {
		dt := make([]int64, 0, m)
		d = append(d, dt)

		go func(i int) {
			transport, err := thrift.NewTSocket(addr[i%len(addr)])
			if err != nil {
				log.Fatalf("did not connect: %v", err)
			}

			useTransport, _ := transportFactory.GetTransport(transport)
			client := proxy.NewServiceClientFactory(useTransport, protocolFactory)
			if err := transport.Open(); err != nil {
				log.Println(err)
			}
			defer transport.Close()

			for j := 0; j < m; j++ {
				k := strconv.Itoa(j)
				reqBody := proxy.GetRequest{Table: "ATable", Key: []byte(k), ForceMaster:true}

				t := time.Now().UnixNano()
				reply, _ := client.GetUserFeature(context.Background(), &reqBody)
				t = time.Now().UnixNano() - t

				d[i] = append(d[i], t)

				if reply.Code == 0 && reply.Msg == "" && string(reply.Value) == k + "p5wj1xc11kNxyQGoGUY3CYVZAvyC3vzT1wC6L4dUwB23hL7dWoBv4Vp54cBEYh3t09cu6ZMbspiArPVtIH7CJdgIZvkrC5bb9n8YJA53mbxb97iYKtKStBecMPvGhhG6eZvvbt16YR9Szky93ZF3C5RVRlcPC7pJRJzmpZyUqETV8C3xsQZrJ7QOvT81vJNhjqUudufCOm8POvtpCLPcYmC8Ccfu5xyR7pQB6dDqlqfUzydzSBr9LsBr8qb8mRUekZk0jKlzmgKPQaWuKgkDOlV55r1crUbYmmjtU0sfHKcC4oVcYakgfpccng0SUcScYVid0QJqcVjW3fJbNNE48qdpFBedKHlo1nreLiUYLEP818FiUtYkTpP2wYtgzFMxfrxWH2Ju0Pyuvcb1ROiKCPE6e6vryz8iN1qXkxqWnsy3vTZ9JR3cCrpW9U8VJ5rlcTLOhgAsShrhBimwMKyMxVkYbi7P1bbjCUfXCswWyuTIpM5w2XtfbAuNdH7V9dENgXpoSxAg5CBsEbguqcDOink2JR6h0bwTJKWShT7Qf84gOMDuysTIH6wbjU5a1pGDVpgSZ4lgpPLg1aUgRsTVhDEA8KsuaQyLniFubFF1imD2lo5DutcAlhiYxzgQX9WePjzlirBfe37duVVGWdYzWOLVkt7IRRnqjOnRcQKWotyTNlDS913pfB9S7qLObnsD99HCrMpHxXOM0uavK0tZVrCt1W5dnEZVAf9nh4h5uAuy88glpJYibFrhUUKNdiGWXysZcte6RYKvbPGjKdE8VM5YDZ9G4U37itbayYGGKgvhXr4KcxM3aJFazDt6Q0Rx96ODPe1CqW56vBMSSc137icrNA6NbpqNTImB0zs1W1wNQCo4b76Ep18F1dZ7i07xTHqBY0ST5RjVBT6TXY5bLe62hUq02Uw4Z6wfmlozxmDSuUdliJ39xAqEAOypd1L0LWN9T1NsL5q2uvyDJIt8BJSbXw4Hd7Q0srWUaYiv916" {
					//if reply.Code == 0 && reply.Msg == "" && string(reply.Value) == k + "ppp" {
					atomic.AddUint64(&transOK, 1)
				}

				atomic.AddUint64(&trans, 1)
				wg.Done()
			}
		}(i)

	}

	wg.Wait()
	totalT = time.Now().UnixNano() - totalT
	totalT = totalT / 1000000
	log.Printf("took %d ms for %d requests\n", totalT, n*m)

	totalD := make([]int64, 0, n*m)
	for _, k := range d {
		totalD = append(totalD, k...)
	}
	totalD2 := make([]float64, 0, n*m)
	for _, k := range totalD {
		totalD2 = append(totalD2, float64(k))
	}

	mean, _ := stats.Mean(totalD2)
	median, _ := stats.Median(totalD2)
	max, _ := stats.Max(totalD2)
	min, _ := stats.Min(totalD2)
	p95, _ := stats.Percentile(totalD2, 95)

	log.Println("getTrue")
	log.Printf("sent     requests    : %d\n", n*m)
	log.Printf("received requests    : %d\n", atomic.LoadUint64(&trans))
	log.Printf("received requests_OK : %d\n", atomic.LoadUint64(&transOK))
	log.Printf("throughput  (TPS)    : %d\n", int64(n*m)*1000/totalT)
	log.Printf("mean: %.f ns, median: %.f ns, max: %.f ns, min: %.f ns, p95: %.f ns\n", mean, median, max, min, p95)
}

func getFalseTest(num int)  {
	transportFactory := thrift.NewTZlibTransportFactoryWithFactory(zlib.BestSpeed, thrift.NewTBufferedTransportFactory(8192))
	protocolFactory := thrift.NewTCompactProtocolFactory()

	var wg sync.WaitGroup

	n := 1000
	d := make([][]int64, n, n)
	m := num

	wg.Add(n * m)

	var trans uint64
	var transOK uint64

	addr, err := getAddr()
	if err != nil {
		return
	}

	totalT := time.Now().UnixNano()
	for i := 0; i < n; i++ {
		dt := make([]int64, 0, m)
		d = append(d, dt)

		go func(i int) {
			transport, err := thrift.NewTSocket(addr[i%len(addr)])
			if err != nil {
				log.Fatalf("did not connect: %v", err)
			}

			useTransport, _ := transportFactory.GetTransport(transport)
			client := proxy.NewServiceClientFactory(useTransport, protocolFactory)
			if err := transport.Open(); err != nil {
				log.Println(err)
			}
			defer transport.Close()

			for j := 0; j < m; j++ {
				k := strconv.Itoa(j)
				reqBody := proxy.GetRequest{Table: "ATable", Key: []byte(k), ForceMaster:false}

				t := time.Now().UnixNano()
				reply, _ := client.GetUserFeature(context.Background(), &reqBody)
				t = time.Now().UnixNano() - t

				d[i] = append(d[i], t)

				if reply.Code == 0 && reply.Msg == "" && string(reply.Value) == k + "p5wj1xc11kNxyQGoGUY3CYVZAvyC3vzT1wC6L4dUwB23hL7dWoBv4Vp54cBEYh3t09cu6ZMbspiArPVtIH7CJdgIZvkrC5bb9n8YJA53mbxb97iYKtKStBecMPvGhhG6eZvvbt16YR9Szky93ZF3C5RVRlcPC7pJRJzmpZyUqETV8C3xsQZrJ7QOvT81vJNhjqUudufCOm8POvtpCLPcYmC8Ccfu5xyR7pQB6dDqlqfUzydzSBr9LsBr8qb8mRUekZk0jKlzmgKPQaWuKgkDOlV55r1crUbYmmjtU0sfHKcC4oVcYakgfpccng0SUcScYVid0QJqcVjW3fJbNNE48qdpFBedKHlo1nreLiUYLEP818FiUtYkTpP2wYtgzFMxfrxWH2Ju0Pyuvcb1ROiKCPE6e6vryz8iN1qXkxqWnsy3vTZ9JR3cCrpW9U8VJ5rlcTLOhgAsShrhBimwMKyMxVkYbi7P1bbjCUfXCswWyuTIpM5w2XtfbAuNdH7V9dENgXpoSxAg5CBsEbguqcDOink2JR6h0bwTJKWShT7Qf84gOMDuysTIH6wbjU5a1pGDVpgSZ4lgpPLg1aUgRsTVhDEA8KsuaQyLniFubFF1imD2lo5DutcAlhiYxzgQX9WePjzlirBfe37duVVGWdYzWOLVkt7IRRnqjOnRcQKWotyTNlDS913pfB9S7qLObnsD99HCrMpHxXOM0uavK0tZVrCt1W5dnEZVAf9nh4h5uAuy88glpJYibFrhUUKNdiGWXysZcte6RYKvbPGjKdE8VM5YDZ9G4U37itbayYGGKgvhXr4KcxM3aJFazDt6Q0Rx96ODPe1CqW56vBMSSc137icrNA6NbpqNTImB0zs1W1wNQCo4b76Ep18F1dZ7i07xTHqBY0ST5RjVBT6TXY5bLe62hUq02Uw4Z6wfmlozxmDSuUdliJ39xAqEAOypd1L0LWN9T1NsL5q2uvyDJIt8BJSbXw4Hd7Q0srWUaYiv916" {
					//if reply.Code == 0 && reply.Msg == "" && string(reply.Value) == k + "ppp" {
					atomic.AddUint64(&transOK, 1)
				}

				atomic.AddUint64(&trans, 1)
				wg.Done()
			}
		}(i)

	}

	wg.Wait()
	totalT = time.Now().UnixNano() - totalT
	totalT = totalT / 1000000
	log.Printf("took %d ms for %d requests\n", totalT, n*m)

	totalD := make([]int64, 0, n*m)
	for _, k := range d {
		totalD = append(totalD, k...)
	}
	totalD2 := make([]float64, 0, n*m)
	for _, k := range totalD {
		totalD2 = append(totalD2, float64(k))
	}

	mean, _ := stats.Mean(totalD2)
	median, _ := stats.Median(totalD2)
	max, _ := stats.Max(totalD2)
	min, _ := stats.Min(totalD2)
	p95, _ := stats.Percentile(totalD2, 95)

	log.Println("getFalse")
	log.Printf("sent     requests    : %d\n", n*m)
	log.Printf("received requests    : %d\n", atomic.LoadUint64(&trans))
	log.Printf("received requests_OK : %d\n", atomic.LoadUint64(&transOK))
	log.Printf("throughput  (TPS)    : %d\n", int64(n*m)*1000/totalT)
	log.Printf("mean: %.f ns, median: %.f ns, max: %.f ns, min: %.f ns, p95: %.f ns\n", mean, median, max, min, p95)
}

func hsetTest(num int)  {
	transportFactory := thrift.NewTZlibTransportFactoryWithFactory(zlib.BestSpeed, thrift.NewTBufferedTransportFactory(8192))
	protocolFactory := thrift.NewTCompactProtocolFactory()

	var wg sync.WaitGroup

	n := 1000
	d := make([][]int64, n, n)
	m := num

	wg.Add(n * m)

	var trans uint64
	var transOK uint64

	addr, err := getAddr()
	if err != nil {
		return
	}

	totalT := time.Now().UnixNano()
	for i := 0; i < n; i++ {
		dt := make([]int64, 0, m)
		d = append(d, dt)

		go func(i int) {
			transport, err := thrift.NewTSocket(addr[i%len(addr)])
			if err != nil {
				log.Fatalf("did not connect: %v", err)
			}

			useTransport, _ := transportFactory.GetTransport(transport)
			client := proxy.NewServiceClientFactory(useTransport, protocolFactory)
			if err := transport.Open(); err != nil {
				log.Println(err)
			}
			defer transport.Close()

			for j := 0; j < m; j++ {
				k := strconv.Itoa(j)
				reqBody := proxy.HsetRequest{Table: "ATable", Key: []byte(k), Field: []byte(k + "p5wj1xc11kNxyQGoGUY3CYVZAvyC3vzT1wC6L4dUwB23hL7dWoBv4Vp54cBEYh3t09cu6ZMbspiArPVtIH7CJdgIZvkrC5bb9n8YJA53mbxb97iYKtKStBecMPvGhhG6eZvvbt16YR9Szky93ZF3C5RVRlcPC7pJRJzmpZyUqETV8C3xsQZrJ7QOvT81vJNhjqUudufCOm8POvtpCLPcYmC8Ccfu5xyR7pQB6dDqlqfUzydzSBr9LsBr8qb8mRUekZk0jKlzmgKPQaWuKgkDOlV55r1crUbYmmjtU0sfHKcC4oVcYakgfpccng0SUcScYVid0QJqcVjW3fJbNNE48qdpFBedKHlo1nreLiUYLEP818FiUtYkTpP2wYtgzFMxfrxWH2Ju0Pyuvcb1ROiKCPE6e6vryz8iN1qXkxqWnsy3vTZ9JR3cCrpW9U8VJ5rlcTLOhgAsShrhBimwMKyMxVkYbi7P1bbjCUfXCswWyuTIpM5w2XtfbAuNdH7V9dENgXpoSxAg5CBsEbguqcDOink2JR6h0bwTJKWShT7Qf84gOMDuysTIH6wbjU5a1pGDVpgSZ4lgpPLg1aUgRsTVhDEA8KsuaQyLniFubFF1imD2lo5DutcAlhiYxzgQX9WePjzlirBfe37duVVGWdYzWOLVkt7IRRnqjOnRcQKWotyTNlDS913pfB9S7qLObnsD99HCrMpHxXOM0uavK0tZVrCt1W5dnEZVAf9nh4h5uAuy88glpJYibFrhUUKNdiGWXysZcte6RYKvbPGjKdE8VM5YDZ9G4U37itbayYGGKgvhXr4KcxM3aJFazDt6Q0Rx96ODPe1CqW56vBMSSc137icrNA6NbpqNTImB0zs1W1wNQCo4b76Ep18F1dZ7i07xTHqBY0ST5RjVBT6TXY5bLe62hUq02Uw4Z6wfmlozxmDSuUdliJ39xAqEAOypd1L0LWN9T1NsL5q2uvyadsfafdasdfasfaf"), Value: []byte(k + "p5wj1xc11kNxyQGoGUY3CYVZAvyC3vzT1wC6L4dUwB23hL7dWoBv4Vp54cBEYh3t09cu6ZMbspiArPVtIH7CJdgIZvkrC5bb9n8YJA53mbxb97iYKtKStBecMPvGhhG6eZvvbt16YR9Szky93ZF3C5RVRlcPC7pJRJzmpZyUqETV8C3xsQZrJ7QOvT81vJNhjqUudufCOm8POvtpCLPcYmC8Ccfu5xyR7pQB6dDqlqfUzydzSBr9LsBr8qb8mRUekZk0jKlzmgKPQaWuKgkDOlV55r1crUbYmmjtU0sfHKcC4oVcYakgfpccng0SUcScYVid0QJqcVjW3fJbNNE48qdpFBedKHlo1nreLiUYLEP818FiUtYkTpP2wYtgzFMxfrxWH2Ju0Pyuvcb1ROiKCPE6e6vryz8iN1qXkxqWnsy3vTZ9JR3cCrpW9U8VJ5rlcTLOhgAsShrhBimwMKyMxVkYbi7P1bbjCUfXCswWyuTIpM5w2XtfbAuNdH7V9dENgXpoSxAg5CBsEbguqcDOink2JR6h0bwTJKWShT7Qf84gOMDuysTIH6wbjU5a1pGDVpgSZ4lgpPLg1aUgRsTVhDEA8KsuaQyLniFubFF1imD2lo5DutcAlhiYxzgQX9WePjzlirBfe37duVVGWdYzWOLVkt7IRRnqjOnRcQKWotyTNlDS913pfB9S7qLObnsD99HCrMpHxXOM0uavK0tZVrCt1W5dnEZVAf9nh4h5uAuy88glpJYibFrhUUKNdiGWXysZcte6RYKvbPGjKdE8VM5YDZ9G4U37itbayYGGKgvhXr4KcxM3aJFazDt6Q0Rx96ODPe1CqW56vBMSSc137icrNA6NbpqNTImB0zs1W1wNQCo4b76Ep18F1dZ7i07xTHqBY0ST5RjVBT6TXY5bLe62hUq02Uw4Z6wfmlozxmDSuUdliJ39xAqEAOypd1L0LWN9T1NsL5q2uvyDJIt8BJSbXw4Hd7Q0srWUaYiv916")}

				t := time.Now().UnixNano()
				reply, _ := client.HsetUserFeature(context.Background(), &reqBody)
				t = time.Now().UnixNano() - t

				d[i] = append(d[i], t)

				if reply.Code == 0 && reply.Msg == "" {
					atomic.AddUint64(&transOK, 1)
				}

				atomic.AddUint64(&trans, 1)
				wg.Done()
			}
		}(i)

	}

	wg.Wait()
	totalT = time.Now().UnixNano() - totalT
	totalT = totalT / 1000000
	log.Printf("took %d ms for %d requests\n", totalT, n*m)

	totalD := make([]int64, 0, n*m)
	for _, k := range d {
		totalD = append(totalD, k...)
	}
	totalD2 := make([]float64, 0, n*m)
	for _, k := range totalD {
		totalD2 = append(totalD2, float64(k))
	}

	mean, _ := stats.Mean(totalD2)
	median, _ := stats.Median(totalD2)
	max, _ := stats.Max(totalD2)
	min, _ := stats.Min(totalD2)
	p95, _ := stats.Percentile(totalD2, 95)

	log.Println("hset")
	log.Printf("sent     requests    : %d\n", n*m)
	log.Printf("received requests    : %d\n", atomic.LoadUint64(&trans))
	log.Printf("received requests_OK : %d\n", atomic.LoadUint64(&transOK))
	log.Printf("throughput  (TPS)    : %d\n", int64(n*m)*1000/totalT)
	log.Printf("mean: %.f ns, median: %.f ns, max: %.f ns, min: %.f ns, p95: %.f ns\n", mean, median, max, min, p95)
}

func hgetTest(num int)  {
	transportFactory := thrift.NewTZlibTransportFactoryWithFactory(zlib.BestSpeed, thrift.NewTBufferedTransportFactory(8192))
	protocolFactory := thrift.NewTCompactProtocolFactory()

	var wg sync.WaitGroup

	n := 1000
	d := make([][]int64, n, n)
	m := num

	wg.Add(n * m)

	var trans uint64
	var transOK uint64

	addr, err := getAddr()
	if err != nil {
		return
	}

	totalT := time.Now().UnixNano()
	for i := 0; i < n; i++ {
		dt := make([]int64, 0, m)
		d = append(d, dt)

		go func(i int) {
			transport, err := thrift.NewTSocket(addr[i%len(addr)])
			if err != nil {
				log.Fatalf("did not connect: %v", err)
			}

			useTransport, _ := transportFactory.GetTransport(transport)
			client := proxy.NewServiceClientFactory(useTransport, protocolFactory)
			if err := transport.Open(); err != nil {
				log.Println(err)
			}
			defer transport.Close()

			for j := 0; j < m; j++ {
				k := strconv.Itoa(j)
				reqBody := proxy.HgetRequest{Table: "ATable", Key: []byte(k), Field: []byte(k + "p5wj1xc11kNxyQGoGUY3CYVZAvyC3vzT1wC6L4dUwB23hL7dWoBv4Vp54cBEYh3t09cu6ZMbspiArPVtIH7CJdgIZvkrC5bb9n8YJA53mbxb97iYKtKStBecMPvGhhG6eZvvbt16YR9Szky93ZF3C5RVRlcPC7pJRJzmpZyUqETV8C3xsQZrJ7QOvT81vJNhjqUudufCOm8POvtpCLPcYmC8Ccfu5xyR7pQB6dDqlqfUzydzSBr9LsBr8qb8mRUekZk0jKlzmgKPQaWuKgkDOlV55r1crUbYmmjtU0sfHKcC4oVcYakgfpccng0SUcScYVid0QJqcVjW3fJbNNE48qdpFBedKHlo1nreLiUYLEP818FiUtYkTpP2wYtgzFMxfrxWH2Ju0Pyuvcb1ROiKCPE6e6vryz8iN1qXkxqWnsy3vTZ9JR3cCrpW9U8VJ5rlcTLOhgAsShrhBimwMKyMxVkYbi7P1bbjCUfXCswWyuTIpM5w2XtfbAuNdH7V9dENgXpoSxAg5CBsEbguqcDOink2JR6h0bwTJKWShT7Qf84gOMDuysTIH6wbjU5a1pGDVpgSZ4lgpPLg1aUgRsTVhDEA8KsuaQyLniFubFF1imD2lo5DutcAlhiYxzgQX9WePjzlirBfe37duVVGWdYzWOLVkt7IRRnqjOnRcQKWotyTNlDS913pfB9S7qLObnsD99HCrMpHxXOM0uavK0tZVrCt1W5dnEZVAf9nh4h5uAuy88glpJYibFrhUUKNdiGWXysZcte6RYKvbPGjKdE8VM5YDZ9G4U37itbayYGGKgvhXr4KcxM3aJFazDt6Q0Rx96ODPe1CqW56vBMSSc137icrNA6NbpqNTImB0zs1W1wNQCo4b76Ep18F1dZ7i07xTHqBY0ST5RjVBT6TXY5bLe62hUq02Uw4Z6wfmlozxmDSuUdliJ39xAqEAOypd1L0LWN9T1NsL5q2uvyadsfafdasdfasfaf")}

				t := time.Now().UnixNano()
				reply, _ := client.HgetUserFeature(context.Background(), &reqBody)
				t = time.Now().UnixNano() - t

				d[i] = append(d[i], t)

				if reply.Code == 0 && reply.Msg == "" && string(reply.Value) == k + "p5wj1xc11kNxyQGoGUY3CYVZAvyC3vzT1wC6L4dUwB23hL7dWoBv4Vp54cBEYh3t09cu6ZMbspiArPVtIH7CJdgIZvkrC5bb9n8YJA53mbxb97iYKtKStBecMPvGhhG6eZvvbt16YR9Szky93ZF3C5RVRlcPC7pJRJzmpZyUqETV8C3xsQZrJ7QOvT81vJNhjqUudufCOm8POvtpCLPcYmC8Ccfu5xyR7pQB6dDqlqfUzydzSBr9LsBr8qb8mRUekZk0jKlzmgKPQaWuKgkDOlV55r1crUbYmmjtU0sfHKcC4oVcYakgfpccng0SUcScYVid0QJqcVjW3fJbNNE48qdpFBedKHlo1nreLiUYLEP818FiUtYkTpP2wYtgzFMxfrxWH2Ju0Pyuvcb1ROiKCPE6e6vryz8iN1qXkxqWnsy3vTZ9JR3cCrpW9U8VJ5rlcTLOhgAsShrhBimwMKyMxVkYbi7P1bbjCUfXCswWyuTIpM5w2XtfbAuNdH7V9dENgXpoSxAg5CBsEbguqcDOink2JR6h0bwTJKWShT7Qf84gOMDuysTIH6wbjU5a1pGDVpgSZ4lgpPLg1aUgRsTVhDEA8KsuaQyLniFubFF1imD2lo5DutcAlhiYxzgQX9WePjzlirBfe37duVVGWdYzWOLVkt7IRRnqjOnRcQKWotyTNlDS913pfB9S7qLObnsD99HCrMpHxXOM0uavK0tZVrCt1W5dnEZVAf9nh4h5uAuy88glpJYibFrhUUKNdiGWXysZcte6RYKvbPGjKdE8VM5YDZ9G4U37itbayYGGKgvhXr4KcxM3aJFazDt6Q0Rx96ODPe1CqW56vBMSSc137icrNA6NbpqNTImB0zs1W1wNQCo4b76Ep18F1dZ7i07xTHqBY0ST5RjVBT6TXY5bLe62hUq02Uw4Z6wfmlozxmDSuUdliJ39xAqEAOypd1L0LWN9T1NsL5q2uvyDJIt8BJSbXw4Hd7Q0srWUaYiv916" {
					atomic.AddUint64(&transOK, 1)
				}

				atomic.AddUint64(&trans, 1)
				wg.Done()
			}
		}(i)

	}

	wg.Wait()
	totalT = time.Now().UnixNano() - totalT
	totalT = totalT / 1000000
	log.Printf("took %d ms for %d requests\n", totalT, n*m)

	totalD := make([]int64, 0, n*m)
	for _, k := range d {
		totalD = append(totalD, k...)
	}
	totalD2 := make([]float64, 0, n*m)
	for _, k := range totalD {
		totalD2 = append(totalD2, float64(k))
	}

	mean, _ := stats.Mean(totalD2)
	median, _ := stats.Median(totalD2)
	max, _ := stats.Max(totalD2)
	min, _ := stats.Min(totalD2)
	p95, _ := stats.Percentile(totalD2, 95)

	log.Println("hget")
	log.Printf("sent     requests    : %d\n", n*m)
	log.Printf("received requests    : %d\n", atomic.LoadUint64(&trans))
	log.Printf("received requests_OK : %d\n", atomic.LoadUint64(&transOK))
	log.Printf("throughput  (TPS)    : %d\n", int64(n*m)*1000/totalT)
	log.Printf("mean: %.f ns, median: %.f ns, max: %.f ns, min: %.f ns, p95: %.f ns\n", mean, median, max, min, p95)
}

func hmsetTest(num int)  {
	transportFactory := thrift.NewTZlibTransportFactoryWithFactory(zlib.BestSpeed, thrift.NewTBufferedTransportFactory(8192))
	protocolFactory := thrift.NewTCompactProtocolFactory()

	var wg sync.WaitGroup

	n := 1000
	d := make([][]int64, n, n)
	m := num

	wg.Add(n * m)

	var trans uint64
	var transOK uint64

	addr, err := getAddr()
	if err != nil {
		return
	}

	totalT := time.Now().UnixNano()
	for i := 0; i < n; i++ {
		dt := make([]int64, 0, m)
		d = append(d, dt)

		go func(i int) {
			transport, err := thrift.NewTSocket(addr[i%len(addr)])
			if err != nil {
				log.Fatalf("did not connect: %v", err)
			}

			useTransport, _ := transportFactory.GetTransport(transport)
			client := proxy.NewServiceClientFactory(useTransport, protocolFactory)
			if err := transport.Open(); err != nil {
				log.Println(err)
			}
			defer transport.Close()

			for j := 0; j < m; j++ {
				k := strconv.Itoa(j)
				values := make(map[string][]byte, 3)

				values["0ppp"] = []byte("0zzz")
				values["1ppp"] = []byte("1zzz")
				values["2ppp"] = []byte("2zzz")

				reqBody := proxy.HmsetRequest{Table: "ATable", Key: []byte(k), Values: values}

				t := time.Now().UnixNano()
				reply, _ := client.HmsetUserFeatures(context.Background(), &reqBody)
				t = time.Now().UnixNano() - t

				d[i] = append(d[i], t)

				if reply.Code == 0 && reply.Msg == "" {
					atomic.AddUint64(&transOK, 1)
				} else {
					log.Println("hmset err:", reply.Msg)
				}

				atomic.AddUint64(&trans, 1)
				wg.Done()
			}
		}(i)

	}

	wg.Wait()
	totalT = time.Now().UnixNano() - totalT
	totalT = totalT / 1000000
	log.Printf("took %d ms for %d requests\n", totalT, n*m)

	totalD := make([]int64, 0, n*m)
	for _, k := range d {
		totalD = append(totalD, k...)
	}
	totalD2 := make([]float64, 0, n*m)
	for _, k := range totalD {
		totalD2 = append(totalD2, float64(k))
	}

	mean, _ := stats.Mean(totalD2)
	median, _ := stats.Median(totalD2)
	max, _ := stats.Max(totalD2)
	min, _ := stats.Min(totalD2)
	p95, _ := stats.Percentile(totalD2, 95)

	log.Println("hmset")
	log.Printf("sent     requests    : %d\n", n*m)
	log.Printf("received requests    : %d\n", atomic.LoadUint64(&trans))
	log.Printf("received requests_OK : %d\n", atomic.LoadUint64(&transOK))
	log.Printf("throughput  (TPS)    : %d\n", int64(n*m)*1000/totalT)
	log.Printf("mean: %.f ns, median: %.f ns, max: %.f ns, min: %.f ns, p95: %.f ns\n", mean, median, max, min, p95)
}

func hmgetTest(num int)  {
	transportFactory := thrift.NewTZlibTransportFactoryWithFactory(zlib.BestSpeed, thrift.NewTBufferedTransportFactory(8192))
	protocolFactory := thrift.NewTCompactProtocolFactory()

	var wg sync.WaitGroup

	n := 1000
	d := make([][]int64, n, n)
	m := num

	wg.Add(n * m)

	var trans uint64
	var transOK uint64

	addr, err := getAddr()
	if err != nil {
		return
	}

	totalT := time.Now().UnixNano()
	for i := 0; i < n; i++ {
		dt := make([]int64, 0, m)
		d = append(d, dt)

		go func(i int) {
			transport, err := thrift.NewTSocket(addr[i%len(addr)])
			if err != nil {
				log.Fatalf("did not connect: %v", err)
			}

			useTransport, _ := transportFactory.GetTransport(transport)
			client := proxy.NewServiceClientFactory(useTransport, protocolFactory)
			if err := transport.Open(); err != nil {
				log.Println(err)
			}
			defer transport.Close()

			for j := 0; j < m; j++ {
				k := strconv.Itoa(j)
				values := make([][]byte, 3)

				values[0] = []byte("0ppp")
				values[1] = []byte("1ppp")
				values[2] = []byte("2ppp")

				reqBody := proxy.HmgetRequest{Table: "ATable", Key: []byte(k), Fields: values}

				t := time.Now().UnixNano()
				reply, _ := client.HmgetUserFeatures(context.Background(), &reqBody)
				t = time.Now().UnixNano() - t

				d[i] = append(d[i], t)

				if reply.Code == 0 && reply.Msg == "" {
					for y := 0; y < 3; y++ {
						if string(reply.Value[y]) == strconv.Itoa(y)+"zzz" {
							atomic.AddUint64(&transOK, 1)
						} else {
							log.Println("hmget err:", reply.Msg)
						}
					}
				}

				atomic.AddUint64(&trans, 1)
				wg.Done()
			}
		}(i)

	}

	wg.Wait()
	totalT = time.Now().UnixNano() - totalT
	totalT = totalT / 1000000
	log.Printf("took %d ms for %d requests\n", totalT, n*m)

	totalD := make([]int64, 0, n*m)
	for _, k := range d {
		totalD = append(totalD, k...)
	}
	totalD2 := make([]float64, 0, n*m)
	for _, k := range totalD {
		totalD2 = append(totalD2, float64(k))
	}

	mean, _ := stats.Mean(totalD2)
	median, _ := stats.Median(totalD2)
	max, _ := stats.Max(totalD2)
	min, _ := stats.Min(totalD2)
	p95, _ := stats.Percentile(totalD2, 95)

	log.Println("hmget")
	log.Printf("sent     requests    : %d\n", n*m)
	log.Printf("received requests    : %d\n", atomic.LoadUint64(&trans))
	log.Printf("received requests_OK : %d\n", atomic.LoadUint64(&transOK))
	log.Printf("throughput  (TPS)    : %d\n", int64(n*m)*1000/totalT)
	log.Printf("mean: %.f ns, median: %.f ns, max: %.f ns, min: %.f ns, p95: %.f ns\n", mean, median, max, min, p95)
}