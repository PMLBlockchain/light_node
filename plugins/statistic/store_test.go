package statistic

import (
	"context"
	"github.com/PMLBlockchain/light_node/registry"
	"os"
	"testing"
	"time"
)

func createTestMetricStore() *metricDbStore {
	dbUrl := os.Getenv("DATABASE_URL")
	log.Infof("DATABASE_URL=%s", dbUrl)
	if len(dbUrl) < 1 {
		return nil
	}
	return newMetricDbStore(dbUrl)
}

func TestMetricDbStore_LogServiceDown(t *testing.T) {
	store := createTestMetricStore()
	if store == nil {
		return
	}
	err := store.Init()
	if err != nil {
		t.Error(err)
		return
	}
	ctx := context.Background()
	service := &registry.Service{
		Name: "test",
		Url:  "http://test:1234/service" + time.Now().String(),
		Host: "127.0.0.1",
	}
	store.LogServiceDown(ctx, service)
	log.Infof("LogServiceDown service %s", service.String())

	list, err := store.QueryServiceDownLogs(ctx, 0, 10)
	if err != nil {
		t.Error(err)
		return
	}
	if len(list.Items) < 1 || list.Items[0].Url != service.Url {
		t.Errorf("serivce down log query not match, query result is %s", list.Items[0].Url)
		return
	}
	log.Infof("QueryServiceDownLogs find %d logs", list.Total)
}

func TestMetricDbStore_QueryServiceDownLogs(t *testing.T) {
	store := createTestMetricStore()
	if store == nil {
		return
	}
	err := store.Init()
	if err != nil {
		t.Error(err)
		return
	}
	ctx := context.Background()
	list, err := store.QueryServiceDownLogs(ctx, 0, 10)
	if err != nil {
		t.Error(err)
		return
	}
	log.Infof("QueryServiceDownLogs find %d logs", list.Total)
}

func TestMetricDbStore_UpdateServiceHostPing(t *testing.T) {
	store := createTestMetricStore()
	if store == nil {
		return
	}
	err := store.Init()
	if err != nil {
		t.Error(err)
		return
	}
	ctx := context.Background()
	service := &registry.Service{
		Name: "test",
		Url:  "http://test:1234/service" + time.Now().String(),
		Host: "127.0.0.1",
	}
	store.UpdateServiceHostPing(ctx, service, 1*time.Second, true)
	log.Infof("UpdateServiceHostPing service %s", service.String())

	healthRecord, err := store.QueryServiceHealthByUrl(ctx, service)
	if err != nil {
		t.Error(err)
		return
	}
	if healthRecord == nil {
		t.Errorf("query no service health record of host %s", service.Host)
		return
	}
	log.Infof("found health record rtt = %d ms", healthRecord.Rtt.Milliseconds())
}
