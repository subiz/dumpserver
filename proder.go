package dumpserver

import (
	"context"
	"crypto/md5"
	"fmt"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/subiz/header"
	"github.com/subiz/log"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type DumpProderMgr struct {
	header.UnimplementedProderServer
	lock      *sync.Mutex
	productM  map[string]*header.Product  // product id -> product
	discountM map[string]*header.Discount // discount id -> discount

	responseM map[string][]*header.Response
}

func NewDumpProderMgr() *DumpProderMgr {
	grpcServer := grpc.NewServer()
	mgr := &DumpProderMgr{
		lock:      &sync.Mutex{},
		productM:  map[string]*header.Product{},
		discountM: map[string]*header.Discount{},
		responseM: map[string][]*header.Response{},
	}
	header.RegisterProderServer(grpcServer, mgr)

	lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", 21827))
	if err != nil {
		panic(err)
	}
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			panic(err)
		}
	}()
	return mgr
}

func (me *DumpProderMgr) ReadProduct(ctx context.Context, p *header.Product) (*header.Product, error) {
	me.lock.Lock()
	defer me.lock.Unlock()
	product := me.productM[p.GetId()]
	if product == nil {
		panic("MISSING")
	}
	return product, nil
}

func (me *DumpProderMgr) CreateDiscount(ctx context.Context, p *header.Discount) (*header.Response, error) {
	me.lock.Lock()
	defer me.lock.Unlock()
	p = proto.Clone(p).(*header.Discount)
	p.Updated = time.Now().UnixMilli()
	if p.Created == 0 {
		p.Created = p.Updated
	}
	me.discountM[p.GetId()] = p
	return &header.Response{Discount: p}, nil
}

func (me *DumpProderMgr) UpdateDiscount(ctx context.Context, p *header.Discount) (*header.Response, error) {
	me.lock.Lock()
	defer me.lock.Unlock()
	p = proto.Clone(p).(*header.Discount)
	p.Updated = time.Now().UnixMilli()
	if p.Created == 0 {
		p.Created = p.Updated
	}
	me.discountM[p.GetId()] = p
	return &header.Response{Discount: p}, nil
}

func (me *DumpProderMgr) GetDiscount(ctx context.Context, p *header.Id) (*header.Response, error) {
	me.lock.Lock()
	defer me.lock.Unlock()
	discount := me.discountM[p.GetId()]
	if discount == nil {
		return &header.Response{Discount: &header.Discount{Id: p.Id}}, nil
	}
	return &header.Response{Discount: discount}, nil
}

func (me *DumpProderMgr) FilterProduct2(ctx context.Context, p *header.FilterProductRequest) (*header.Response, error) {
	start := time.Now()
	for time.Since(start) < 10*time.Second {
		me.lock.Lock()
		if len(me.responseM["filter_product2"]) == 0 {
			fmt.Println("CHECKLEN", len(me.responseM["filter_product2"]))
			me.lock.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		me.lock.Unlock()
		res := me.responseM["filter_product2"][0]
		fmt.Println("LLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLL", res)
		me.responseM["filter_product2"] = me.responseM["filter_product2"][1:]
		return res, nil
	}
	return nil, log.ETimeout()
}

func (me *DumpProderMgr) UpdateProduct(ctx context.Context, p *header.Product) (*header.Product, error) {
	me.lock.Lock()
	defer me.lock.Unlock()
	p = proto.Clone(p).(*header.Product)
	p.Updated = time.Now().UnixMilli()
	p.Modified = time.Now().UnixMilli()
	if p.Created == 0 {
		p.Created = p.Updated
	}
	me.productM[p.GetId()] = p
	return p, nil
}

func (me *DumpProderMgr) DeleteProduct(ctx context.Context, p *header.Id) (*header.Empty, error) {
	me.lock.Lock()
	defer me.lock.Unlock()
	delete(me.productM, p.GetId())
	return &header.Empty{}, nil
}

func (me *DumpProderMgr) ListAllProductIds(ctx context.Context, req *header.ProductsRequest) (*header.Ids, error) {
	out := []*header.Product{}
	for _, p := range me.productM {
		out = append(out, p)
	}
	str := ""
	sort.Slice(out, func(i, j int) bool {
		return out[i].Id < out[j].Id
	})

	ids := []string{}
	modifieds := []int64{}
	for _, p := range out {
		str += p.Id + "." + strconv.Itoa(int(p.Modified)) + "\n"
		ids = append(ids, p.Id)
		modifieds = append(modifieds, p.Modified)
	}
	etag := fmt.Sprintf("%x", md5.Sum([]byte(str)))
	if etag == req.ETag {
		return &header.Ids{ETag: etag}, nil
	}
	return &header.Ids{LastModifieds: modifieds, Ids: ids, ETag: etag}, nil
}

func (me *DumpProderMgr) ListAllProductDiscountIds(ctx context.Context, req *header.Id) (*header.Ids, error) {
	out := []*header.Discount{}
	for _, p := range me.discountM {
		out = append(out, p)
	}
	str := ""
	sort.Slice(out, func(i, j int) bool {
		return out[i].Id < out[j].Id
	})

	ids := []string{}
	modifieds := []int64{}
	for _, p := range out {
		str += p.Id + "." + strconv.Itoa(int(p.Updated)) + "\n"
		ids = append(ids, p.Id)
		modifieds = append(modifieds, p.Updated)
	}
	etag := fmt.Sprintf("%x", md5.Sum([]byte(str)))
	if etag == req.ETag {
		return &header.Ids{ETag: etag}, nil
	}
	return &header.Ids{LastModifieds: modifieds, Ids: ids, ETag: etag}, nil
}
