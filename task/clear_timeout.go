package task

import (
	"github.com/prometheus/pushgateway/storage"
	"time"
	"fmt"
	"strings"
	"net/http"
	"io/ioutil"
)

// start clear task
func StartClear(ms storage.MetricStore, listenAddress string) {
	go loopMetrics(ms, listenAddress)
}

func loopMetrics(ms storage.MetricStore, listenAddress string) {
	var (
		job       string
		instance  string
		timeStamp string
	)

	if !strings.HasPrefix(listenAddress, ":") {
		listenAddress = ":" + listenAddress
	}

	for {
		time.Sleep(time.Second * 30)
		MetricGroups := ms.GetMetricFamiliesMap()
		for _, metricGroup := range MetricGroups {
			job = metricGroup.Labels["job"]
			instance = metricGroup.Labels["instance"]
			timeStamp = metricGroup.Metrics["push_time_seconds"].Timestamp.String()
			if ! validateState(timestampToUnix(timeStamp), time.Now().Unix()) && len(metricGroup.Metrics) > 1 {
				fakePut(job, instance, listenAddress, timeStamp)
			}
		}
	}
}

func fakePut(job string, instance string, listenAddress string, timeStamp string) {
	url := fmt.Sprintf("http://127.0.0.1%s/metrics/jobs/%s/instances/%s?time=%d",
		listenAddress, job, instance, timestampToUnix(timeStamp))
	client := &http.Client{}
	req, err := http.NewRequest(http.MethodPut, url, strings.NewReader(""))
	if err != nil {
		fmt.Println("update job state err: ", err)
		return
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("update job state err: ", err)
		return
	}
	defer resp.Body.Close()
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("update job state err: ", err)
		return
	}

}

func timestampToUnix(s string) int64 {
	return timestampToTime(s).Unix()
}

func timestampToTime(s string) time.Time {
	s = strings.Replace(s, " +0800 CST", "", -1)
	strTime := strings.Split(s, ".")[0]
	timeLayout := "2006-01-02 15:04:05"
	loc, _ := time.LoadLocation("Local")
	theTime, _ := time.ParseInLocation(timeLayout, strTime, loc)
	return theTime
}

func validateState(t1, t2 int64) bool {
	return t2 < t1+60
}
