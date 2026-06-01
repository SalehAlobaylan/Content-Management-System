package utils

import (
	"math"
	"math/rand"
)

// KMeans runs k-means++ (Lloyd's algorithm) over dense float32 vectors and
// returns the cluster index for each input plus the final centroids. All
// vectors must share a dimension. Euclidean distance on (near-unit-norm) BGE
// embeddings approximates cosine grouping well enough for topic clustering.
func KMeans(vectors [][]float32, k, maxIter int, seed int64) (assignments []int, centroids [][]float32) {
	n := len(vectors)
	if n == 0 || k <= 0 {
		return nil, nil
	}
	if k > n {
		k = n
	}
	dim := len(vectors[0])
	rng := rand.New(rand.NewSource(seed))

	// ── k-means++ initialization ──
	centroids = make([][]float32, k)
	centroids[0] = cloneVec(vectors[rng.Intn(n)])
	dist2 := make([]float64, n)
	for i := range dist2 {
		dist2[i] = math.Inf(1)
	}
	for c := 1; c < k; c++ {
		var sum float64
		for i, v := range vectors {
			if d := sqDist(v, centroids[c-1]); d < dist2[i] {
				dist2[i] = d
			}
			sum += dist2[i]
		}
		// Choose the next seed with probability proportional to squared distance.
		target := rng.Float64() * sum
		idx := 0
		var acc float64
		for i := range vectors {
			acc += dist2[i]
			if acc >= target {
				idx = i
				break
			}
		}
		centroids[c] = cloneVec(vectors[idx])
	}

	assignments = make([]int, n)
	for i := range assignments {
		assignments[i] = -1
	}

	for iter := 0; iter < maxIter; iter++ {
		changed := false

		// Assignment step.
		for i, v := range vectors {
			best, bestD := 0, math.Inf(1)
			for c := 0; c < k; c++ {
				if d := sqDist(v, centroids[c]); d < bestD {
					bestD, best = d, c
				}
			}
			if assignments[i] != best {
				assignments[i] = best
				changed = true
			}
		}

		// Update step.
		sums := make([][]float64, k)
		counts := make([]int, k)
		for c := 0; c < k; c++ {
			sums[c] = make([]float64, dim)
		}
		for i, v := range vectors {
			c := assignments[i]
			counts[c]++
			for d := 0; d < dim; d++ {
				sums[c][d] += float64(v[d])
			}
		}
		for c := 0; c < k; c++ {
			if counts[c] == 0 {
				// Re-seed an emptied cluster onto a random point.
				centroids[c] = cloneVec(vectors[rng.Intn(n)])
				continue
			}
			nc := make([]float32, dim)
			for d := 0; d < dim; d++ {
				nc[d] = float32(sums[c][d] / float64(counts[c]))
			}
			centroids[c] = nc
		}

		if !changed {
			break
		}
	}

	return assignments, centroids
}

func cloneVec(v []float32) []float32 {
	out := make([]float32, len(v))
	copy(out, v)
	return out
}

func sqDist(a, b []float32) float64 {
	var s float64
	for i := range a {
		d := float64(a[i] - b[i])
		s += d * d
	}
	return s
}
