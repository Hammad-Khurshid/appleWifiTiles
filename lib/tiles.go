package lib

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
	"wloc/lib/mac"
	"wloc/lib/morton"
	"wloc/lib/shapefiles"
	"wloc/pb"

	"google.golang.org/protobuf/proto"
)

var httpClient = &http.Client{
	Transport: &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   30 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		ExpectContinueTimeout: 10 * time.Second,
	},
	Timeout: 2 * time.Minute,
}

func GetTile(tileKey int64) ([]AP, error) {
	var tileURL string = "https://gspe85-ssl.ls.apple.com"

	lat, lon, _ := morton.Decode(tileKey)
	if shapefiles.IsInChina(lat, lon) {
		tileURL = "https://gspe85-cn-ssl.ls.apple.com"
	}
	tileURL = tileURL + "/wifi_request_tile"
	req, err := http.NewRequest("GET", tileURL, nil)
	if err != nil {
		return nil, err
	}
	for key, val := range map[string]string{
		"Accept":          "*/*",
		"Connection":      "keep-alive",
		"X-tilekey":       fmt.Sprintf("%d", tileKey),
		"User-Agent":      "geod/1 CFNetwork/1496.0.7 Darwin/23.5.0",
		"Accept-Language": "en-US,en-GB;q=0.9,en;q=0.8",
		"X-os-version":    "17.5.21F79",
	} {
		req.Header.Set(key, val)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	wifuTile := &pb.WifiTile{}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(b, wifuTile)
	if err != nil {
		return nil, err
	}
	aps := make([]AP, 0)
	max := 0
	for _, region := range wifuTile.GetRegion() {
		aps = append(aps, make([]AP, len(region.GetDevices()))...)
		for _, device := range region.GetDevices() {
			if device == nil || device.Bssid == 0 {
				continue
			}
			aps[max] = AP{
				BSSID: mac.Decode(device.GetBssid()),
				Location: Location{
					Lat:  CoordFromInt(int64(device.GetEntry().GetLat()), -7),
					Long: CoordFromInt(int64(device.GetEntry().GetLong()), -7),
				},
			}
			max++
		}
	}
	return aps[:max], nil
}
