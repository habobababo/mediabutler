package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Cron: 5 Felder wie die Python-Version (Minute Stunde Tag Monat Wochentag), Listen, Bereiche, Schritte.
type Cron struct {
	min, hour, dom, mon, dow map[int]bool
	domStar, dowStar         bool
}

var errCronField = errors.New("Ungültiger Wert. Erlaubt: Zahlen, *, Bereiche (1-5), Listen (1,3) und Schritte (*/15)")

func cronField(txt string, lo, hi int) (map[int]bool, error) {
	out := map[int]bool{}
	for _, part := range strings.Split(txt, ",") {
		step := 1
		if i := strings.Index(part, "/"); i >= 0 {
			st, err := strconv.Atoi(part[i+1:])
			if err != nil || st < 1 {
				return nil, errCronField
			}
			step, part = st, part[:i]
		}
		var a, b int
		var err error
		switch {
		case part == "*":
			a, b = lo, hi
		case strings.Contains(part, "-"):
			p := strings.SplitN(part, "-", 2)
			if a, err = strconv.Atoi(p[0]); err != nil {
				return nil, errCronField
			}
			if b, err = strconv.Atoi(p[1]); err != nil {
				return nil, errCronField
			}
		default:
			if a, err = strconv.Atoi(part); err != nil {
				return nil, errCronField
			}
			b = a
			if step > 1 {
				b = hi
			}
		}
		if !(lo <= a && a <= b && b <= hi) {
			return nil, errCronField
		}
		for x := a; x <= b; x += step {
			out[x] = true
		}
	}
	return out, nil
}

func NewCron(expr string) (*Cron, error) {
	f := strings.Fields(expr)
	if len(f) != 5 {
		return nil, errors.New("Cron braucht 5 Felder: Minute Stunde Tag Monat Wochentag")
	}
	c := &Cron{}
	var err error
	if c.min, err = cronField(f[0], 0, 59); err != nil {
		return nil, err
	}
	if c.hour, err = cronField(f[1], 0, 23); err != nil {
		return nil, err
	}
	if c.dom, err = cronField(f[2], 1, 31); err != nil {
		return nil, err
	}
	if c.mon, err = cronField(f[3], 1, 12); err != nil {
		return nil, err
	}
	dw, err := cronField(f[4], 0, 7)
	if err != nil {
		return nil, err
	}
	c.dow = map[int]bool{}
	for x := range dw {
		c.dow[x%7] = true
	}
	c.domStar, c.dowStar = strings.HasPrefix(f[2], "*"), strings.HasPrefix(f[4], "*")
	return c, nil
}

func (c *Cron) dayOK(t time.Time) bool {
	d, w := c.dom[t.Day()], c.dow[int(t.Weekday())]
	switch {
	case c.domStar && c.dowStar:
		return true
	case c.domStar:
		return w
	case c.dowStar:
		return d
	}
	return d || w
}

func (c *Cron) Match(t time.Time) bool {
	return c.min[t.Minute()] && c.hour[t.Hour()] && c.mon[int(t.Month())] && c.dayOK(t)
}

func (c *Cron) NextAfter(after time.Time) (time.Time, bool) {
	t := after.Truncate(time.Minute).Add(time.Minute)
	for i := 0; i < 20000; i++ {
		switch {
		case !c.mon[int(t.Month())]:
			t = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, t.Location())
		case !c.dayOK(t):
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, t.Location())
		case !c.hour[t.Hour()]:
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, t.Location())
		case !c.min[t.Minute()]:
			t = t.Add(time.Minute)
		default:
			return t, true
		}
	}
	return time.Time{}, false
}

var wdNames = []string{"Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"}

func fmtDt(t time.Time) string {
	return fmt.Sprintf("%s %s", wdNames[(int(t.Weekday())+6)%7], t.Format("02.01.2006 15:04"))
}

func nextRuns(expr string, n int) ([]string, error) {
	c, err := NewCron(expr)
	if err != nil {
		return nil, err
	}
	out, t := []string{}, time.Now()
	for i := 0; i < n; i++ {
		nx, ok := c.NextAfter(t)
		if !ok {
			break
		}
		out = append(out, fmtDt(nx))
		t = nx
	}
	return out, nil
}
