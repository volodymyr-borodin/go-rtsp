package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/volodymyr-borodin/go-rtsp"
	"log/slog"
	"net/url"
	"os"
	"time"
)

func main() {
	u, err := url.Parse(os.Getenv("RTSP_URL"))
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := rtsp.NewClient(u, rtsp.WithTransport(rtsp.TransportModeTCP))
	if err != nil {
		panic(err)
	}

	defer func() {
		err = client.Close(context.Background())
		if err != nil {
			panic(err)
		}
	}()

	supportedMethods, err := client.Options(ctx)
	if err != nil {
		panic(err)
	}

	slog.Info("RTSP options", slog.String("methods", fmt.Sprintf("%+v", supportedMethods)))
	_ = supportedMethods

	sdp, err := client.Describe(ctx)
	if err != nil {
		panic(err)
	}

	for _, media := range sdp.MediaDescriptions {
		err = client.Setup(ctx, media)
		if err != nil {
			panic(err)
		}
	}

	err = client.Play(ctx)
	if err != nil {
		panic(err)
	}

	go func(ctx context.Context) {
		time.Sleep(5 * time.Second)
		err := client.Teardown(ctx)
		if err != nil {
			panic(err)
		}
	}(ctx)

	for {
		select {
		case p := <-client.RTPPackets():
			slog.Info("RTP packet received",
				slog.Int("payload length", len(p.Payload)),
				slog.Int("ssrc", int(p.SSRC)))
		case p := <-client.RTCPPackets():
			slog.Info("RTCP packet received",
				slog.String("pkg", fmt.Sprintf("%+v", p)))
		case e := <-client.Errors():
			if errors.Is(e, rtsp.ErrClientTeardown) {
				return
			}

			if e != nil {
				panic(e)
			}
		}
	}
}
