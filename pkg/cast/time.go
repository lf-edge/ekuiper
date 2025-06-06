// Copyright 2021-2024 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cast

import (
	"bytes"
	"fmt"
	"time"

	"github.com/jinzhu/now"
)

//var (
//	formats = map[string]string{
//		"MMMM": "January",
//		"MMM":  "Jan",
//		"MM":   "01",
//		"M":    "1",
//		"YYYY": "2006",
//		"yyyy": "2006",
//		"YY":   "06",
//		"yy":   "06",
//		"G":    "AD",
//		"EEEE": "Monday",
//		"EEE":  "Mon",
//		"dd":   "02",
//		"d":    "2",
//		"HH":   "15",
//		"hh":   "03",
//		"h":    "3",
//		"mm":   "04",
//		"m":    "4",
//		"ss":   "05",
//		"s":    "5",
//		"a":    "PM",
//		"S":    ".0",
//		"SS":    ".00",
//		"SSS":   ".000",
//		"SSSN":  ".0000",
//		"SSSNN": ".00000",
//		"SSSNNN": ".000000",
//		"SSSNNNN": ".0000000",
//		"SSSNNNNN":".00000000",
//		"SSSNNNNNN":".000000000",
//		"z":    "MST",
//		"Z":    "-0700",
//		"X":    "-07",
//		"XX":    "-0700",
//		"XXX":  "-07:00",
//	}
//)

const (
	JSISO   = "2006-01-02T15:04:05.000Z07:00"
	ISO8601 = "2006-01-02T15:04:05"
)

func init() {
	now.TimeFormats = append(now.TimeFormats, JSISO, ISO8601)
}

func GetConfiguredTimeZone() *time.Location {
	return localTimeZone
}

var localTimeZone = time.Local

func SetTimeZone(name string) error {
	loc, err := time.LoadLocation(name)
	if err != nil {
		return err
	}
	localTimeZone = loc
	return nil
}

func TimeToUnixMilli(time time.Time) int64 {
	return time.UnixNano() / 1e6
}

func InterfaceToUnixMilli(i interface{}, format string) (int64, error) {
	switch t := i.(type) {
	case int64:
		return t, nil
	case int:
		return int64(t), nil
	case float64:
		return int64(t), nil
	case time.Time:
		return TimeToUnixMilli(t), nil
	case string:
		ti, err := ParseTime(t, format)
		return TimeToUnixMilli(ti), err
	default:
		return 0, fmt.Errorf("unsupported type to convert to timestamp %v", t)
	}
}

func InterfaceToTime(i interface{}, format string) (time.Time, error) {
	switch t := i.(type) {
	case int64:
		return TimeFromUnixMilli(t), nil
	case int:
		return TimeFromUnixMilli(int64(t)), nil
	case float64:
		return TimeFromUnixMilli(int64(t)), nil
	case time.Time:
		return t, nil
	case string:
		return ParseTime(t, format)
	default:
		return time.Now(), fmt.Errorf("unsupported type to convert to timestamp %v", t)
	}
}

func TimeFromUnixMilli(t int64) time.Time {
	return time.Unix(t/1000, (t%1000)*1e6).In(localTimeZone)
}

func ParseTime(t string, f string) (_ time.Time, err error) {
	if f, err = convertFormat(f); err != nil {
		return time.Time{}, err
	}
	return time.Parse(f, t)
}

func ParseTimeByFormats(t string, formats []string) (_ time.Time, err error) {
	c := &now.Config{
		TimeLocation: localTimeZone,
		TimeFormats:  now.TimeFormats,
	}
	if len(formats) > 0 {
		c.TimeFormats = append(formats, c.TimeFormats...)
	}
	return c.Parse(t)
}

func FormatTime(time time.Time, f string) (string, error) {
	if f, err := convertFormat(f); err != nil {
		return "", err
	} else {
		return time.Format(f), nil
	}
}

//func convertFormat(f string) string {
//	re := regexp.MustCompile(`(?m)(M{4})|(M{3})|(M{2})|(M{1})|(Y{4})|(Y{2})|(y{4})|(y{2})|(G{1})|(E{4})|(E{3})|(d{2})|(d{1})|(H{2})|(h{2})|(h{1})|(m{2})|(m{1})|(s{2})|(s{1})|(a{1})|(S{3}N{6})|(S{3}N{5})|(S{3}N{4})|(S{3}N{3})|(S{3}N{2})|(S{3}N{1})|(S{3})|(S{2})|(S{1})|(z{1})|(Z{1})|(X{3})|(X{2})|(X{1})`)
//	for _, match := range re.FindAllString(f, -1) {
//		for key, val := range formats {
//			if match == key {
//				f = strings.Replace(f, match, val, -1)
//			}
//		}
//	}
//	return f
//}

func convertFormat(f string) (string, error) {
	formatRune := []rune(f)
	lenFormat := len(formatRune)
	out := ""
	for i := 0; i < len(formatRune); i++ {
		switch r := formatRune[i]; r {
		case '\\':
			i = i + 1
			if i >= len(formatRune) {
				return "", fmt.Errorf("%s is invalid", f)
			}
			out += string(formatRune[i])
		case 'Y', 'y':
			j := 1
			for ; i+j < lenFormat && j <= 4; j++ {
				if formatRune[i+j] != r {
					break
				}
			}
			i = i + j - 1
			switch j {
			case 4: // YYYY
				out += "2006"
			case 2: // YY
				out += "06"
			default:
				return "", fmt.Errorf("invalid time format %s for Y/y", f)
			}
		case 'G': // era
			out += "AD"
		case 'M': // M MM MMM MMMM month of year
			j := 1
			for ; i+j < lenFormat && j <= 4; j++ {
				if formatRune[i+j] != r {
					break
				}
			}
			i = i + j - 1
			switch j {
			case 1: // M
				out += "1"
			case 2: // MM
				out += "01"
			case 3: // MMM
				out += "Jan"
			case 4: // MMMM
				out += "January"
			}
		case 'd': // d dd day of month
			j := 1
			for ; i+j < lenFormat && j <= 2; j++ {
				if formatRune[i+j] != r {
					break
				}
			}
			i = i + j - 1
			switch j {
			case 1: // d
				out += "2"
			case 2: // dd
				out += "02"
			}
		case 'E': // M MM MMM MMMM month of year
			j := 1
			for ; i+j < lenFormat && j <= 4; j++ {
				if formatRune[i+j] != r {
					break
				}
			}
			i = i + j - 1
			switch j {
			case 3: // EEE
				out += "Mon"
			case 4: // EEEE
				out += "Monday"
			default:
				return "", fmt.Errorf("invalid time format %s for E", f)
			}
		case 'H': // HH
			j := 1
			for ; i+j < lenFormat && j <= 2; j++ {
				if formatRune[i+j] != r {
					break
				}
			}
			i = i + j - 1
			switch j {
			case 2: // HH
				out += "15"
			default:
				return "", fmt.Errorf("invalid time format %s of H, only HH is supported", f)
			}
		case 'h': // h hh
			j := 1
			for ; i+j < lenFormat && j <= 2; j++ {
				if formatRune[i+j] != r {
					break
				}
			}
			i = i + j - 1
			switch j {
			case 1: // h
				out += "3"
			case 2: // hh
				out += "03"
			}
		case 'a': // a
			out += "PM"
		case 'm': // m mm minute of hour
			j := 1
			for ; i+j < lenFormat && j <= 2; j++ {
				if formatRune[i+j] != r {
					break
				}
			}
			i = i + j - 1
			switch j {
			case 1: // m
				out += "4"
			case 2: // mm
				out += "04"
			}
		case 's': // s ss
			j := 1
			for ; i+j < lenFormat && j <= 2; j++ {
				if formatRune[i+j] != r {
					break
				}
			}
			i = i + j - 1
			switch j {
			case 1: // s
				out += "5"
			case 2: // ss
				out += "05"
			}

		case 'S': // S SS SSS....
			j := 0
			for ; i+j < lenFormat; j++ {
				if formatRune[i+j] != 'S' {
					break
				}
			}
			b := bytes.NewBufferString(".")
			for x := 0; x < j; x++ {
				b.WriteString("0")
			}
			out += b.String()
			i = i + j - 1
		case 'z': // z
			out += "MST"
		case 'Z': // Z
			out += "-0700"
		case 'X': // X XX XXX
			j := 1
			for ; i+j < lenFormat && j <= 3; j++ {
				if formatRune[i+j] != r {
					break
				}
			}
			i = i + j - 1
			switch j {
			case 1: // X
				out += "-07"
			case 2: // XX
				out += "-0700"
			case 3: // XXX
				out += "-07:00"
			}
		case '\'': // ' (text delimiter)  or '' (real quote)

			// real quote
			if formatRune[i+1] == r {
				out += "'"
				i = i + 1
				continue
			}

			tmp := []rune{}
			j := 1
			for ; i+j < lenFormat; j++ {
				if formatRune[i+j] != r {
					tmp = append(tmp, formatRune[i+j])
					continue
				}
				break
			}
			i = i + j
			out += string(tmp)
		default:
			out += string(r)
		}
	}
	return out, nil
}

// InterfaceToDuration converts an interface to a time.Duration.
func InterfaceToDuration(i interface{}) (time.Duration, error) {
	duration, err := ToString(i, STRICT)
	if err != nil {
		return 0, fmt.Errorf("given arguments cannot convert to duration: %q", err)
	}
	return time.ParseDuration(duration)
}
