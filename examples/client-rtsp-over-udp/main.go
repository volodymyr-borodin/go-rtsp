package main

import (
	"context"
	"fmt"
	"github.com/pion/rtp"
	"github.com/pion/sdp/v3"
	"github.com/synapti-co/go-rtsp"
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

	client, err := rtsp.NewClientWithOptions(u, rtsp.WithTransport(rtsp.TransportModeUDP))
	if err != nil {
		panic(err)
	}

	defer func() {
		err = client.Teardown(context.Background())
		if err != nil {
			panic(err)
		}
	}()

	client.OnRTPPacket(func(media *sdp.MediaDescription, pkt *rtp.Packet) {
		slog.Info("RTP packet received",
			slog.String("media-name", media.MediaName.String()),
			slog.Int("payload length", len(pkt.Payload)),
			slog.Int("ssrc", int(pkt.SSRC)),
			slog.String("attributes", fmt.Sprintf("%+v", media.Attributes)))
	})

	supportedMethods, err := client.Options(ctx)
	if err != nil {
		panic(err)
	}

	slog.Info("RTSP options", slog.String("methods", fmt.Sprintf("%+v", supportedMethods)))

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

	time.Sleep(5 * time.Second)

	err = client.Teardown(context.Background())
	if err != nil {
		panic(err)
	}
}
