package dispatcher

import (
	"context"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"golang.org/x/time/rate"
)

type RateLimitWriter struct {
	Writer  buf.Writer
	Limiter *rate.Limiter
	Context context.Context
}

func (w *RateLimitWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	if w.Limiter != nil {
		ctx := w.Context
		if ctx == nil {
			ctx = context.Background()
		}
		// WaitN errors permanently when n exceeds the burst; split large
		// buffers into burst-sized chunks so small limits can't kill the link.
		burst := w.Limiter.Burst()
		if burst <= 0 {
			burst = 1
		}
		for n := int(mb.Len()); n > 0; {
			c := n
			if c > burst {
				c = burst
			}
			if err := w.Limiter.WaitN(ctx, c); err != nil {
				buf.ReleaseMulti(mb)
				return err
			}
			n -= c
		}
	}
	return w.Writer.WriteMultiBuffer(mb)
}

func (w *RateLimitWriter) Close() error { return common.Close(w.Writer) }
func (w *RateLimitWriter) Interrupt()   { common.Interrupt(w.Writer) }
