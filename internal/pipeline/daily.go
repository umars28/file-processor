package pipeline

import (
	"cmp"
	"slices"
	"strings"
	"time"
)

const dateLayout = "2006-01-02"

type DailyKey struct {
	Date      string
	OutletID  string
	ProductID string
}

type DailySales struct {
	DailyKey
	Transactions int64
	Quantity     int64
	Amount       int64
}

type dayKey struct {
	year      int
	month     time.Month
	day       int
	outletID  string
	productID string
}

type sums struct {
	totals     Totals
	daily      map[dayKey]*DailySales
	maxKeys    int
	overflowed bool
}

func newSums(maxKeys int) *sums {
	return &sums{daily: map[dayKey]*DailySales{}, maxKeys: maxKeys}
}

func (s *sums) add(tx Transaction) {
	s.totals.Transactions++
	s.totals.Quantity += tx.Quantity
	s.totals.Amount += tx.Amount

	y, m, day := tx.TransactedAt.Date()
	key := dayKey{year: y, month: m, day: day, outletID: tx.OutletID, productID: tx.ProductID}
	d, ok := s.daily[key]
	if !ok {
		if s.maxKeys > 0 && len(s.daily) >= s.maxKeys {
			s.overflowed = true
			return
		}
		key.outletID = strings.Clone(key.outletID)
		key.productID = strings.Clone(key.productID)
		d = &DailySales{DailyKey: DailyKey{
			Date:      tx.TransactedAt.Format(dateLayout),
			OutletID:  key.outletID,
			ProductID: key.productID,
		}}
		s.daily[key] = d
	}
	d.Transactions++
	d.Quantity += tx.Quantity
	d.Amount += tx.Amount
}

func (s *sums) sortedDaily() []DailySales {
	out := make([]DailySales, 0, len(s.daily))
	for _, d := range s.daily {
		out = append(out, *d)
	}
	slices.SortFunc(out, func(a, b DailySales) int { return compareDailyKeys(a.DailyKey, b.DailyKey) })
	return out
}

func compareDailyKeys(a, b DailyKey) int {
	return cmp.Or(
		cmp.Compare(a.Date, b.Date),
		cmp.Compare(a.OutletID, b.OutletID),
		cmp.Compare(a.ProductID, b.ProductID),
	)
}
