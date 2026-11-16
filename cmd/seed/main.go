// Command seed creates the demo transaction table and fills it with a large,
// realistic dataset in Postgres.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"payment-archive/internal/config"
	"payment-archive/internal/source"
)

func main() {
	rows := flag.Int64("rows", 30_000_000, "number of transaction rows")
	years := flag.Int("years", 7, "time span of the dataset")
	dsn := flag.String("dsn", "", "postgres DSN (defaults to PAYARCHIVE_SOURCE_DSN)")
	flag.Parse()
	if *dsn == "" {
		*dsn = os.Getenv("PAYARCHIVE_SOURCE_DSN")
	}
	if *dsn == "" {
		*dsn = config.Default().SourceDSN
	}
	ctx := context.Background()
	src, err := source.Open(ctx, *dsn)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer src.Close()
	if err := src.EnsureSchema(ctx); err != nil {
		log.Fatalf("schema: %v", err)
	}
	start := time.Now()
	log.Printf("seeding %d rows over %d years into %s", *rows, *years, *dsn)
	if err := src.Seed(ctx, *rows, *years); err != nil {
		log.Fatalf("seed: %v", err)
	}
	st, err := src.Stats(ctx)
	if err != nil {
		log.Fatalf("stats: %v", err)
	}
	fmt.Printf("seeded in %s\n", time.Since(start).Round(time.Millisecond))
	fmt.Printf("rows=%d total=%dMB heap=%dMB index=%dMB range=[%s .. %s]\n",
		st.Rows, st.TotalBytes>>20, st.HeapBytes>>20, st.IndexBytes>>20,
		st.MinTime.Format(time.RFC3339), st.MaxTime.Format(time.RFC3339))
}
