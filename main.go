package main

import (
	"bytes"
	"compress/gzip"
	"container/list"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/push"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/yaml.v2"
)

//配置文件yaml
type RConfig struct {
	Gw struct {
		Addr           string `yaml:"addr"`
		HttpListenPort int    `yaml:"httpListenPort"`
		DBaddr         string `yaml:"dbaddr"`
		DBname         string `yaml:"dbname"`
		Tablename      string `yaml:"tablename"`
		Tablename2     string `yaml:"tablename2"`
	}
	Output struct {
		Prometheus      bool   `yaml:"prometheus"`
		PushGateway     bool   `yaml:"pushGateway"`
		PushGatewayAddr string `yaml:"pushGatewayAddr"`
		MonitorID       string `yaml:"monitorID"`
		Period          int    `yaml:"period"`
	}
	P2P struct {
		HistogramOptsparam map[string]float64  `yaml:"histogramOptsparam"`
		SummaryOptsparam   map[float64]float64 `yaml:"summaryOptsparam"`
	}
}

var globeCfg *RConfig

//mongodb 数据mode
type SidLog struct {
	Reporttype  int64  `bson:"reporttype"`
	SidReporter string `bson:"sidReporter"`
	Starttime   int64  `bson:"starttime"`
	Endtime     int64  `bson:"endtime"`
}
type getpath struct {
	CallGetpath string `bson:"callGetpath"`
}

type itime struct {
	InsertTime int64 `bson:"insertTime"`
}
type BaseLog struct {
	CallBaseLog map[string]string `bson:"callBaseLog"`
}

//HistogramOpt参数
var HistogramOptsparamMap = make(map[string]float64)

//SummaryOpt参数
var SummaryOptsparamMap = make(map[float64]float64)

//上一次的数据库的最后插入时间
var lasttime int64 = 0

//prometheus var
var (
	nodeh    *(prometheus.HistogramVec)
	nodes    *(prometheus.SummaryVec)
	nodehisp *(prometheus.HistogramVec)
	nodesisp *(prometheus.SummaryVec)
	nodehnet *(prometheus.HistogramVec)
	nodesnet *(prometheus.SummaryVec)
	nodehdev *(prometheus.HistogramVec)
	nodesdev *(prometheus.SummaryVec)
)

//从mongedb获取以解码的callBaseLog数据数组
func mongodbToBaselog(ip string, db string, table1 string, table2 string) {

	looptime := int64(globeCfg.Output.Period)
	session, err := mgo.Dial(ip)

	if err != nil {
		panic(err)
	}
	defer session.Close()

	collection := session.DB(db).C(table1)
	collection2 := session.DB(db).C(table2)

	var nowtime itime
	err = collection.Find(bson.M{}).Sort("-insertTime").Limit(1).Select(bson.M{"insertTime": 1}).One(&nowtime)

	var min10time int64
	if lasttime == 0 {
		min10time = nowtime.InsertTime - looptime*1000
	} else {
		min10time = lasttime
	}
	var BaseLogresult []SidLog

	//通过InsertTime获取日志中sid和 通话类型(音频/视频)
	err = collection.Find(bson.M{"insertTime": bson.M{"$gte": min10time, "$lt": nowtime.InsertTime}}).Select(bson.M{"reporttype": 1, "sidReporter": 1, "starttime": 1, "endtime": 1}).All(&BaseLogresult)
	if err != nil {
		panic(err)
	}
	fmt.Println("通话个数", len(BaseLogresult))
	a := 1
	for _, v := range BaseLogresult {
		calltime := v.Endtime - v.Starttime
		if calltime >= (60*60*24*1000) || calltime <= 0 {
			continue
		}
		sidmap := make(map[string]string)
		sid := v.SidReporter
		sidmap["sid"] = sid
		if v.Reporttype == 2 {
			sidmap["aorv"] = "video"
		} else {
			sidmap["aorv"] = "audio"
		}
		var getpathone getpath

		err = collection2.Find(bson.M{"sid": sid, "callGetpath": bson.M{"$exists": true}}).Select(bson.M{"callGetpath": 1}).One(&getpathone)
		if err != nil {
			continue
		}

		//探测path的延迟map[pid]delay
		getpathpathmap := getpathToMap(getpathToString(getpathone))

		//获取path的cid和eventtype=ortp sub_type=CE2E_L2R的delay_aver
		var BaseLogs []BaseLog
		fmt.Println(sid)
		err = collection2.Find(bson.M{"sid": sid, "callBaseLog": bson.M{"$exists": true}}).Select(bson.M{"callBaseLog": 1}).All(&BaseLogs)
		if err != nil {
			panic(err)
		}
		fmt.Println(len(BaseLogs), "log")

		//cid分组后的map map["cid"]list<"delay_aver">
		cidgroup := make(map[string]list.List)
		//path map["pid"]"cid"
		pathmap := make(map[string]string)
		//终端信息map
		userinfo := make(map[string]string)
		cidgroup, pathmap, userinfo = baselogTomap(BaseLogs)
		//map["cid"]"delay"  探测延迟对应cid
		ciddelaymap := make(map[string]string)
		for pid, cid := range pathmap {
			delay := getpathpathmap[pid]
			ciddelaymap[cid] = delay
		}

		toPromtheus(cidgroup, ciddelaymap, userinfo, v.Reporttype)
		fmt.Println(a)
		a++
	}

}
func toPromtheus(cidgroup map[string]list.List, ciddelaymap map[string]string, userinfo map[string]string, aorv int64) {
	cidnumber := float64(len(ciddelaymap))
	var param float64 = 0
	for k, v := range ciddelaymap {
		cidlist := cidgroup[k]
		if cidlist.Len() == 0 {
			cidnumber--
			continue
		}
		var intsub float64 = 0
		for e := cidlist.Front(); e != nil; e = e.Next() {
			if e.Value != nil {
				intab := absolute(e.Value.(string), v)
				intsub += intab
			}
		}

		avgsub := intsub / float64(cidlist.Len())
		floatv, _ := strconv.ParseFloat(v, 64)
		fmt.Println(intsub, float64(cidlist.Len()), floatv)
		if floatv != 0 {
			param += (floatv + avgsub) / floatv
		} else {
			cidnumber--
		}

	}
	if param == 0 {
		Observe(float64(-2), aorv, userinfo)
	} else {
		if cidnumber > 0 {

			avgcidparam := param / cidnumber
			aa := math.Pow10(2)

			Observe(math.Trunc(avgcidparam*aa)/aa, aorv, userinfo)
		}
	}

}
func Observe(avgcidparam float64, aorv int64, userinfo map[string]string) {
	if avgcidparam > 10 {
		avgcidparam = float64(10)
	}
	var AorV string
	var isp string
	var net string
	var dev string
	if aorv == 2 {
		AorV = "video"
	} else {
		AorV = "audio"
	}
	fmt.Println(userinfo["loc_isp"], "iiisssppp")
	switch userinfo["loc_isp"] {
	case "1":
		isp = "电信"
	case "2":
		isp = "联通"
	case "3":
		isp = "移动"
	case "4":
		isp = "歌华有线"
	case "5":
		isp = "长城宽带"
	case "6":
		isp = "方正宽带"
	case "7":
		isp = "广电宽带"
	case "8":
		isp = "中电飞华"
	case "9":
		isp = "中电华通"
	case "10":
		isp = "教育网"
	case "11":
		isp = "华宇宽带"
	case "12":
		isp = "科技网"
	case "13":
		isp = "天威宽带"
	case "14":
		isp = "盈联宽带"
	case "15":
		isp = "台湾网络"
	case "16":
		isp = "澳门网络"
	case "17":
		isp = "电讯盈科"
	case "18":
		isp = "瀛海威"
	case "19":
		isp = "有线通"
	case "20":
		isp = "铁通"
	case "21":
		isp = "西部数码"
	case "22":
		isp = "中科网"
	case "23":
		isp = "华数宽带"
	case "24":
		isp = "油田宽带"
	case "25":
		isp = "视讯宽带"
	case "26":
		isp = "有线宽带"
	case "27":
		isp = "北龙中网"
	case "28":
		isp = "宽带通"
	case "29":
		isp = "比通联合网络"
	case "30":
		isp = "鹏博士宽带"
	case "31":
		isp = "广电网"
	case "32":
		isp = "森华通信"
	case "33":
		isp = "世纪互联"
	case "34":
		isp = "歌华宽带"
	case "35":
		isp = "歌华网络"
	case "36":
		isp = "天地网联"
	case "37":
		isp = "电信通"
	default:
		isp = "未知"
	}
	switch userinfo["loc_net"] {
	case "0":
		net = "wifi网络"
	case "1":
		net = "有线网络"
	case "2":
		net = "3G网络"
	case "3":
		net = "GPRS网络"
	case "4":
		net = "移动3G"
	case "5":
		net = "电信3G"
	case "6":
		net = "联通3G"
	case "7":
		net = "Android4G|IOS移动4G"
	case "8":
		net = "联通4G/电信4G"
	case "9":
		net = "Server walker,2Mb and No upload detect"

	default:
		net = "未知"
	}

	switch userinfo["loc_dev"] {
	case "0":
		dev = "N8"
	case "1":
		dev = "PC"
	case "2":
		dev = "手机"
	case "3":
		dev = "苹果手机"
	case "4":
		dev = "TV"
	case "5":
		dev = "N7"
	default:
		dev = "未知"
	}
	//hfmt.Println(isp, net, dev)
	nodeh.WithLabelValues(AorV).Observe(avgcidparam)
	nodes.WithLabelValues(AorV).Observe(avgcidparam)

	nodehisp.WithLabelValues(isp).Observe(avgcidparam)
	nodesisp.WithLabelValues(isp).Observe(avgcidparam)

	nodehnet.WithLabelValues(net).Observe(avgcidparam)
	nodesnet.WithLabelValues(net).Observe(avgcidparam)

	nodehdev.WithLabelValues(dev).Observe(avgcidparam)
	nodesdev.WithLabelValues(dev).Observe(avgcidparam)

}

//取绝对值
func absolute(a string, b string) float64 {
	inta, _ := strconv.ParseFloat(a, 64)
	intb, _ := strconv.ParseFloat(b, 64)
	if inta > intb {
		return (inta - intb)
	} else {
		return (intb - inta)
	}
}

// 将getpath中加密的数据变成明码
func getpathToString(result2 getpath) string {
	var zsrcs []byte = nil
	//获取加密过得getpath后按\n分开后分别进行base64解码；后组合在进行gzip解码；后变为明码
	var b64str []string = strings.Split(result2.CallGetpath, "\n")
	for _, str := range b64str {
		strc, _ := base64.StdEncoding.DecodeString(str)
		zsrcs = append(zsrcs, strc...)
	}
	ugzip := DoGzipUnCompress(zsrcs)
	return string(ugzip)
}

//gzip解压
func DoGzipUnCompress(compressSrc []byte) []byte {
	b := bytes.NewReader(compressSrc)
	var out bytes.Buffer
	r, _ := gzip.NewReader(b)
	io.Copy(&out, r)
	return out.Bytes()
}

// 解析解码后的getpath获取探测延迟
func getpathToMap(str string) map[string]string {

	var jsonmap map[string]interface{}
	json.Unmarshal([]byte(str), &jsonmap)
	//存放探测的各个路的信息
	pathmap := make(map[string]string)
	if v, ok := jsonmap["resp_cmd_path_array"]; ok {
		if v != nil {
			resp_cmd_path_array := v.([]interface{})
			for _, rv := range resp_cmd_path_array {
				var strpid string
				var strdelay string
				vmap := rv.(map[string]interface{})
				if pid, ok := vmap["path_id"]; ok {
					strpid = pid.(string)
				}
				if subpath, ok := vmap["sub_path_array"]; ok {
					sub_path_array := subpath.([]interface{})[0]
					if quality, ok := sub_path_array.(map[string]interface{})["quality"]; ok {
						mapquality := quality.(map[string]interface{})
						if delay, ok := mapquality["delay"]; ok {

							strdelay = strconv.FormatInt(int64(delay.(float64)), 10)
						}
					}
				}
				pathmap[strpid] = strdelay
			}
		}
	}
	return pathmap
}
func baselogTomap(logs []BaseLog) (map[string]list.List, map[string]string, map[string]string) {

	cidgroup := make(map[string]list.List)
	pathmap := make(map[string]string)
	userinfo := make(map[string]string)
	for _, v := range logs {
		for _, mapv := range v.CallBaseLog {
			if mapv != "" && strings.Index(mapv, "=") >= 0 {
				strrune := []rune(mapv)
				if string(strrune[10:14]) == "ortp" {

					smap := make(map[string]string)
					strarrays := strings.Split(mapv, " ")
					for _, v := range strarrays {
						if v != "" && strings.Index(v, "=") >= 0 {
							strarray := strings.Split(v, "=")
							smap[strarray[0]] = strarray[1]
						}

					}
					//去除延迟大于20000的数据
					if d, _ := strconv.ParseFloat(smap["delay_aver"], 64); d > 20000 {
						continue
					}
					//去除延迟等于0的数据
					if d, _ := strconv.ParseFloat(smap["delay_aver"], 64); d == 0 {
						continue
					}

					if smap["sub_type"] == "CE2E_L2R" {
						if l := cidgroup[smap["cid"]]; l.Len() == 0 {
							cidlist := list.New()
							cidlist.PushBack(smap["delay_aver"])
							cidgroup[smap["cid"]] = *cidlist

						} else {
							cidlist := cidgroup[smap["cid"]]
							cidlist.PushBack(smap["delay_aver"])
							cidgroup[smap["cid"]] = cidlist

						}
					}

				}
				if string(strrune[10:14]) == "path" {
					smap := make(map[string]string)
					strarrays := strings.Split(mapv, " ")
					for _, v := range strarrays {
						if v != "" && strings.Index(v, "=") >= 0 {
							strarray := strings.Split(v, "=")
							smap[strarray[0]] = strarray[1]
						}

					}
					if smap["path_type"] == "lpath" {
						pathmap[smap["pid"]] = smap["cid"]
					}

				}
				if string(strrune[10:22]) == "ext_dev_info" {
					strarrays := strings.Split(mapv, " ")
					for _, v := range strarrays {
						if v != "" && strings.Index(v, "=") >= 0 {
							strarray := strings.Split(v, "=")
							if len(strarray) == 2 {
								userinfo[strarray[0]] = strarray[1]
							}
						}
					}

				}

			}
		}

	}
	return cidgroup, pathmap, userinfo

}
func loadConfig() {
	cfgbuf, err := ioutil.ReadFile("cfg.yaml")
	if err != nil {
		panic("not found cfg.yaml")
	}
	rfig := RConfig{}
	err = yaml.Unmarshal(cfgbuf, &rfig)
	if err != nil {
		panic("invalid cfg.yaml")
	}
	globeCfg = &rfig
	fmt.Println("Load config -'cfg.yaml'- ok...")
}
func init() {
	loadConfig() //加载配置文件

	HistogramOptsparamMap = globeCfg.P2P.HistogramOptsparam
	SummaryOptsparamMap = globeCfg.P2P.SummaryOptsparam

	nodeh = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "rt",
		Subsystem: "H",
		Name:      "probeReliable",
		Help:      "call",
		Buckets:   prometheus.LinearBuckets(HistogramOptsparamMap["start"], HistogramOptsparamMap["width"], int(HistogramOptsparamMap["count"])),
	},
		[]string{
			"AorV",
		})

	nodes = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  "rt",
		Subsystem:  "S",
		Name:       "probeReliable",
		Help:       "call",
		Objectives: SummaryOptsparamMap,
	},
		[]string{
			"AorV",
		})
	nodehisp = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "rt",
		Subsystem: "Hisp",
		Name:      "probeReliable",
		Help:      "call",
		Buckets:   prometheus.LinearBuckets(HistogramOptsparamMap["start"], HistogramOptsparamMap["width"], int(HistogramOptsparamMap["count"])),
	},
		[]string{
			"isp",
		})

	nodesisp = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  "rt",
		Subsystem:  "Sisp",
		Name:       "probeReliable",
		Help:       "call",
		Objectives: SummaryOptsparamMap,
	},
		[]string{
			"isp",
		})
	nodehnet = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "rt",
		Subsystem: "Hnet",
		Name:      "probeReliable",
		Help:      "call",
		Buckets:   prometheus.LinearBuckets(HistogramOptsparamMap["start"], HistogramOptsparamMap["width"], int(HistogramOptsparamMap["count"])),
	},
		[]string{
			"net",
		})

	nodesnet = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  "rt",
		Subsystem:  "Snet",
		Name:       "probeReliable",
		Help:       "call",
		Objectives: SummaryOptsparamMap,
	},
		[]string{
			"net",
		})
	nodehdev = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "rt",
		Subsystem: "Hdev",
		Name:      "probeReliable",
		Help:      "call",
		Buckets:   prometheus.LinearBuckets(HistogramOptsparamMap["start"], HistogramOptsparamMap["width"], int(HistogramOptsparamMap["count"])),
	},
		[]string{
			"dev",
		})

	nodesdev = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  "rt",
		Subsystem:  "Sdev",
		Name:       "probeReliable",
		Help:       "call",
		Objectives: SummaryOptsparamMap,
	},
		[]string{
			"dev",
		})

	prometheus.MustRegister(nodeh)
	prometheus.MustRegister(nodes)
	prometheus.MustRegister(nodehisp)
	prometheus.MustRegister(nodesisp)
	prometheus.MustRegister(nodehnet)
	prometheus.MustRegister(nodesnet)
	prometheus.MustRegister(nodehdev)
	prometheus.MustRegister(nodesdev)

}
func main() {

	ip := globeCfg.Gw.DBaddr //"103.25.23.89:60013"
	db := globeCfg.Gw.DBname //"dataAnalysis_new"
	table1 := globeCfg.Gw.Tablename
	table2 := globeCfg.Gw.Tablename2
	//loop
	go func() {
		fmt.Println("Program startup ok...")
		//获取callBaseLog数据
		for {
			mongodbToBaselog(ip, db, table1, table2)
			fmt.Println("ok")
			//是否推送数据给PushGatway
			if globeCfg.Output.PushGateway {
				var info = make(map[string]string)
				info["monitorID"] = globeCfg.Output.MonitorID
				if err := push.FromGatherer("rt", info, globeCfg.Output.PushGatewayAddr, prometheus.DefaultGatherer); err != nil {
					fmt.Println("FromGatherer:", err)
				}
			}
			fmt.Println(time.Now(), "ok")
			time.Sleep(time.Duration(globeCfg.Output.Period) * time.Second)

		}
	}()
	//设置prometheus监听的ip和端口
	if globeCfg.Output.Prometheus {
		go func() {
			fmt.Println("ip", globeCfg.Gw.Addr)
			fmt.Println("port", globeCfg.Gw.HttpListenPort)
			http.Handle("/metrics", promhttp.Handler())
			http.ListenAndServe(fmt.Sprintf("%s:%d", globeCfg.Gw.Addr, globeCfg.Gw.HttpListenPort), nil)

		}()
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c)
	//	signal.Notify(c, os.Interrupt, os.Kill)
	s := <-c
	fmt.Println("asd")
	fmt.Println("exitss", s)

}
