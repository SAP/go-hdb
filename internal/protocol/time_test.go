/*
Copyright 2017 SAP SE

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

package protocol

import (
	"testing"
	"time"
)

type testJulianDay struct {
	jd   int
	time time.Time
}

var testJulianDayData = []testJulianDay{
	testJulianDay{1721424, time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC)},
	testJulianDay{1842713, time.Date(333, time.January, 27, 0, 0, 0, 0, time.UTC)},
	testJulianDay{2299160, time.Date(1582, time.October, 4, 0, 0, 0, 0, time.UTC)},
	testJulianDay{2299161, time.Date(1582, time.October, 15, 0, 0, 0, 0, time.UTC)},
	testJulianDay{2415021, time.Date(1900, time.January, 1, 0, 0, 0, 0, time.UTC)},
	testJulianDay{2447893, time.Date(1990, time.January, 1, 0, 0, 0, 0, time.UTC)},
	testJulianDay{2451545, time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)},
	testJulianDay{2453750, time.Date(2006, time.January, 14, 0, 0, 0, 0, time.UTC)},
	testJulianDay{2455281, time.Date(2010, time.March, 25, 0, 0, 0, 0, time.UTC)},
	testJulianDay{2457188, time.Date(2015, time.June, 14, 0, 0, 0, 0, time.UTC)},

	testJulianDay{2440587, time.Date(1969, time.December, 31, 0, 0, 0, 0, time.UTC)},
	testJulianDay{2440588, time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)},
	testJulianDay{5373484, time.Date(9999, time.December, 31, 0, 0, 0, 0, time.UTC)},
	testJulianDay{2457202, time.Date(2015, time.June, 28, 0, 0, 0, 0, time.UTC)},
}

func TestTimeToJulianDay(t *testing.T) {
	for i, d := range testJulianDayData {
		jd := timeToJulianDay(d.time)
		if jd != d.jd {
			t.Fatalf("Julian Day Number %d value %d - expected %d (date %s)", i, jd, d.jd, d.time)
		}
	}
}

func TestJulianDayToTime(t *testing.T) {
	for i, d := range testJulianDayData {
		time := julianDayToTime(d.jd)
		if !time.Equal(d.time) {
			t.Fatalf("Time %d value %s - expected %s (Julian Day Number %d)", i, time, d.time, d.jd)
		}
	}
}
