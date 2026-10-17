// Package economics projects owner evidence into bounded, explainable account costs.
package economics

import (
	"fmt"
	"math/big"
	"regexp"
	"sort"
	"strings"
)

// Amount uses twelve decimal places. APIs and SQL keep decimal strings, never floats.
type Amount string

var decimalPattern = regexp.MustCompile(`^-?[0-9]{1,18}(\.[0-9]{1,12})?$`)
var scale = new(big.Int).Exp(big.NewInt(10), big.NewInt(12), nil)

const Zero Amount = "0.000000000000"

func ParseAmount(s string) (Amount, error) {
	if !decimalPattern.MatchString(s) {
		return "", fmt.Errorf("amount must be a decimal with at most 12 fractional digits")
	}
	negative := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	parts := strings.SplitN(s, ".", 2)
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
	}
	fraction += strings.Repeat("0", 12-len(fraction))
	n, _ := new(big.Int).SetString(parts[0]+fraction, 10)
	if negative {
		n.Neg(n)
	}
	return fromInt(n), nil
}
func fromInt(n *big.Int) Amount {
	s := n.String()
	negative := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	if len(s) <= 12 {
		s = strings.Repeat("0", 13-len(s)) + s
	}
	s = s[:len(s)-12] + "." + s[len(s)-12:]
	if negative {
		s = "-" + s
	}
	return Amount(s)
}
func integer(a Amount) *big.Int {
	s := strings.ReplaceAll(string(a), ".", "")
	n, ok := new(big.Int).SetString(s, 10)
	if !ok {
		panic("invalid internal amount")
	}
	return n
}
func Add(a, b Amount) Amount  { return fromInt(new(big.Int).Add(integer(a), integer(b))) }
func Sub(a, b Amount) Amount  { return fromInt(new(big.Int).Sub(integer(a), integer(b))) }
func Compare(a, b Amount) int { return integer(a).Cmp(integer(b)) }

// Allocate conserves even sub-cent and negative corrections. Sorted keys make residuals reproducible.
func Allocate(total Amount, weights map[string]int64) (map[string]Amount, error) {
	keys := make([]string, 0, len(weights))
	denom := new(big.Int)
	for k, w := range weights {
		if w < 0 {
			return nil, fmt.Errorf("negative weight")
		}
		if w > 0 {
			keys = append(keys, k)
			denom.Add(denom, big.NewInt(w))
		}
	}
	sort.Strings(keys)
	result := map[string]Amount{}
	if denom.Sign() == 0 {
		return result, nil
	}
	remaining := integer(total)
	for _, k := range keys {
		n := new(big.Int).Mul(integer(total), big.NewInt(weights[k]))
		n.Quo(n, denom)
		result[k] = fromInt(n)
		remaining.Sub(remaining, n)
	}
	// Truncation leaves less than one tick per nonzero recipient.
	step := big.NewInt(1)
	if remaining.Sign() < 0 {
		step.Neg(step)
	}
	for i := 0; remaining.Sign() != 0; i++ {
		k := keys[i%len(keys)]
		result[k] = fromInt(new(big.Int).Add(integer(result[k]), step))
		remaining.Sub(remaining, step)
	}
	return result, nil
}
func Proportion(total Amount, numerator, denominator int64) Amount {
	if denominator <= 0 || numerator <= 0 {
		return Zero
	}
	return fromInt(new(big.Int).Quo(new(big.Int).Mul(integer(total), big.NewInt(numerator)), big.NewInt(denominator)))
}

// AllocateAmounts accepts arbitrary precision monetary weights, including very large provider totals.
func AllocateAmounts(total Amount, weights map[string]Amount) map[string]Amount {
	keys := []string{}
	denom := new(big.Int)
	for k, a := range weights {
		if integer(a).Sign() > 0 {
			keys = append(keys, k)
			denom.Add(denom, integer(a))
		}
	}
	sort.Strings(keys)
	out := map[string]Amount{}
	if denom.Sign() == 0 {
		return out
	}
	remaining := integer(total)
	for _, k := range keys {
		n := new(big.Int).Quo(new(big.Int).Mul(integer(total), integer(weights[k])), denom)
		out[k] = fromInt(n)
		remaining.Sub(remaining, n)
	}
	step := big.NewInt(1)
	if remaining.Sign() < 0 {
		step.Neg(step)
	}
	for i := 0; remaining.Sign() != 0; i++ {
		k := keys[i%len(keys)]
		out[k] = fromInt(new(big.Int).Add(integer(out[k]), step))
		remaining.Sub(remaining, step)
	}
	return out
}

// ScaleSigned rebases priced owner charges and their signed corrections onto one invoice total.
func ScaleSigned(total Amount, parts map[string]Amount) (map[string]Amount, error) {
	keys := []string{}
	denom := new(big.Int)
	for k, a := range parts {
		keys = append(keys, k)
		denom.Add(denom, integer(a))
	}
	if denom.Sign() == 0 {
		return nil, fmt.Errorf("zero priced subtotal")
	}
	sort.Strings(keys)
	out := map[string]Amount{}
	remaining := integer(total)
	for _, k := range keys {
		n := new(big.Int).Quo(new(big.Int).Mul(integer(total), integer(parts[k])), denom)
		out[k] = fromInt(n)
		remaining.Sub(remaining, n)
	}
	step := big.NewInt(1)
	if remaining.Sign() < 0 {
		step.Neg(step)
	}
	for i := 0; remaining.Sign() != 0; i++ {
		k := keys[i%len(keys)]
		out[k] = fromInt(new(big.Int).Add(integer(out[k]), step))
		remaining.Sub(remaining, step)
	}
	return out, nil
}
