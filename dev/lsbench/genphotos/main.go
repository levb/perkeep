/*
Copyright 2026 The Perkeep Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// genphotos writes N noise JPEGs of roughly the given size into a directory,
// as a stand-in for a photo library. Each file is unique. Usage:
//
//	go run ./genphotos -dir /path -n 300 -mb 3
package main

import (
	"bytes"
	"flag"
	"fmt"
	"image"
	"image/jpeg"
	"log"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
)

func main() {
	dir := flag.String("dir", "", "output directory")
	n := flag.Int("n", 300, "number of files")
	mb := flag.Float64("mb", 3, "approximate size of each file in MB")
	flag.Parse()
	if *dir == "" {
		log.Fatal("-dir is required")
	}
	if err := os.MkdirAll(*dir, 0700); err != nil {
		log.Fatal(err)
	}
	// Noise at quality 90 compresses to about 0.75 byte per pixel (4:2:0).
	pixels := int(*mb * (1 << 20) / 0.75)
	w := 4 * intSqrt(pixels/12)
	h := 3 * intSqrt(pixels/12)
	rng := rand.New(rand.NewPCG(1, 2))
	var total int64
	for i := range *n {
		img := image.NewYCbCr(image.Rect(0, 0, w, h), image.YCbCrSubsampleRatio420)
		fill(rng, img.Y)
		fill(rng, img.Cb)
		fill(rng, img.Cr)
		var buf bytes.Buffer
		if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: 90}); err != nil {
			log.Fatal(err)
		}
		total += int64(buf.Len())
		name := filepath.Join(*dir, fmt.Sprintf("IMG_%05d.jpg", i))
		if err := os.WriteFile(name, buf.Bytes(), 0600); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Printf("%d files, %dx%d, %.1f MB total, %.2f MB each\n", *n, w, h, float64(total)/(1<<20), float64(total)/float64(*n)/(1<<20))
}

func fill(rng *rand.Rand, b []byte) {
	for i := range b {
		b[i] = byte(rng.Uint32())
	}
}

// intSqrt returns the least r >= 1 with r*r >= n.
func intSqrt(n int) int { return max(1, int(math.Ceil(math.Sqrt(float64(n))))) }
