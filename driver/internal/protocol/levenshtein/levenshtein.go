// Package levenshtein includes the levenshtein distance algorithm plus additional helper functions.
// The algorithm is taken from https://en.wikibooks.org/wiki/Algorithm_Implementation/Strings/Levenshtein_distance#Go.
package levenshtein

import (
	"math"
	"strings"
	"unicode/utf8"
)

// Distance returns the Lewenshtein distance.
func Distance(caseSensitive bool, a, b string) int {
	if caseSensitive {
		return distance(a, b)
	}
	return distance(strings.ToLower(a), strings.ToLower(b))
}

// MinDistance returns the string out of string list l with the minimal Lewenshtein distance.
func MinDistance(caseSensitive bool, l []string, s string) (rv string) {
	min := math.MaxInt
	for _, si := range l {
		if d := Distance(caseSensitive, si, s); d < min {
			rv = si
			min = d
		}
	}
	return
}

func distance(a, b string) int {
	f := make([]int, utf8.RuneCountInString(b)+1)

	for j := range f {
		f[j] = j
	}

	for _, ca := range a {
		j := 1
		fj1 := f[0] // fj1 is the value of f[j - 1] in last iteration
		f[0]++
		for _, cb := range b {
			mn := min(f[j]+1, f[j-1]+1) // delete & insert
			if cb != ca {
				mn = min(mn, fj1+1) // change
			} else {
				mn = min(mn, fj1) // matched
			}

			fj1, f[j] = f[j], mn // save f[j] to fj1(j is about to increase), update f[j] to mn
			j++
		}
	}

	return f[len(f)-1]
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}
