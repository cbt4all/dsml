package varencd

import (
	"fmt"
	"math/bits"
	"regexp"
	"strconv"
	"strings"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

// OneHotK ...
func OneHotK(df dataframe.DataFrame, v []string, colindex int) dataframe.DataFrame {

	// v -> List of uniqe values of a specific variable in the dataset
	// The one which you want to encode to numerical
	// colindex is the column number of instersted column

	var dftmp dataframe.DataFrame
	dfnrow := df.Nrow()

	for _, item := range v {
		var s []bool
		fname := "a" + strings.ReplaceAll(item, ".", "")

		for i := 0; i < dfnrow; i++ {
			if df.Elem(i, colindex).String() == item {
				s = append(s, true)
			} else {
				s = append(s, false)
			}
		}

		dftmp = df.Mutate(
			series.New(s, series.Bool, fname),
		)
		s = nil
		df = dftmp
	}
	return df
}

// BinaryEncode ...
func BinaryEncode(df dataframe.DataFrame, v []string, colindex int) dataframe.DataFrame {

	// v -> List of uniqe values of a specific variable in the dataset
	// The one which you want to encode to numerical
	// colindex is the column number of interested column

	cnum := len(v)
	bit := bits.Len(uint(cnum))

	var result, dftmp dataframe.DataFrame

	// dfnrow means the original df rows number
	// dfncol means the original df column number - is used to update the record
	// dfheaders is the header of the df

	dfnrow := df.Nrow()
	dfncol := df.Ncol()
	dfheaders := df.Names()

	// Adding "bit" number columns to the  dataframe with all 0 as default value

	for i := 0; i < bit; i++ {
		fname := dfheaders[colindex] + strconv.Itoa(i)
		s := make([]int, dfnrow)

		dftmp = df.Mutate(
			series.New(s, series.Int, fname),
		)
		s = nil
		df = dftmp
	}

	// dfheaders now has new headers
	dfheaders = df.Names()

	// For all records in dataframe, check what is the same content in the slice "v"
	// calculate the binary of the value in "bit" number of bit and replace it with
	// the new columns are added above

	for i := 0; i < dfnrow; i++ {
		for j := 0; j < cnum-1; j++ {
			if strings.Compare(df.Elem(i, colindex).String(), v[j]) == 0 {

				// Exchange index to binary
				bits := getBitFromNumber(j, bit)

				// Select current record
				cr := df.Subset(i)

				// Get current record values
				crv := cr.Records()

				// Create a new recored with the chnaged values
				for k := 0; k < len(crv[1]); k++ {
					if k < dfncol {
						continue
					}
					crv[1][k] = bits[k-dfncol]
				}

				// Change the current record
				// dfheaders is the hearder
				dftmp = cr.Set(
					series.Ints(0),
					dataframe.LoadRecords(
						[][]string{
							dfheaders,
							crv[1],
						},
					),
				)

				cr = dftmp
				// Replacing the new record with the old record
				dftmp = df.Set(series.Ints(i), cr)
				result = dftmp
			}
		}
	}
	return result
}

// CountFrequencyEncoding ...
func CountFrequencyEncoding(df dataframe.DataFrame, v []string, colindex int) dataframe.DataFrame {

	// v -> List of uniqe values of a specific variable in the dataset
	// The one which you want to encode to numerical
	// colindex is the column number of interested column

	var result dataframe.DataFrame

	// dfnrow means the original df rows number
	// dfncol means the original df column number - is used to update the record
	// dfheaders is the header of the df
	// dstncer means distincter to
	dfnrow := df.Nrow()
	dfncol := df.Ncol()
	dfheaders := df.Names()

	// Adding a columns to the dataframe with all 0 as default value
	fname := "cfe" + dfheaders[colindex]
	s := make([]int, dfnrow)

	df = df.Mutate(
		series.New(s, series.Int, fname),
	)

	// dfheaders now has new headers
	dfheaders = df.Names()

	// dstncer shows how many times each value is repeated.
	dstncer := dfDistincter(df, colindex)
	//cnum := len(dstncer)

	// For all records in dataframe, check what is the same content in the slice "v"
	// calculate the binary of the value in "bit" number of bit and replace it with
	// the new columns are added above

	for i := 0; i < dfnrow; i++ {

		// Get the number of times each value is repeated.
		fr := dstncer[df.Elem(i, colindex).String()]

		// Select current record
		cr := df.Subset(i)

		// Get current record values
		crv := cr.Records()

		// Create a new recored with the chnaged values
		crv[1][dfncol] = strconv.Itoa(fr)

		// Change the current record
		// dfheaders is the hearder of df
		dftmp := cr.Set(
			series.Ints(0),
			dataframe.LoadRecords(
				[][]string{
					dfheaders,
					crv[1],
				},
			),
		)

		cr = dftmp
		// Replacing the new record with the old record
		dftmp = df.Set(series.Ints(i), cr)

		result = dftmp
	}

	return result
}

// IntegerEncoding ...
func IntegerEncoding(df dataframe.DataFrame, v []string, colindex int) dataframe.DataFrame {
	// v -> List of uniqe values of a specific variable in the dataset
	// The one which you want to encode to numerical
	// colindex is the column number of interested column

	var result dataframe.DataFrame

	// dfnrow means the original df rows number
	// dfncol means the original df column number - is used to update the record
	// dfheaders is the header of the df
	// m is a MAP assiging a number to each item of the slice
	dfnrow := df.Nrow()
	dfncol := df.Ncol()
	dfheaders := df.Names()
	m := sliceIndexShow(v)

	// Adding a columns to the dataframe with all 0 as default value
	fname := "inte" + dfheaders[colindex]
	s := make([]int, dfnrow)

	df = df.Mutate(
		series.New(s, series.Int, fname),
	)

	// dfheaders now has new headers
	dfheaders = df.Names()

	// For all records in dataframe, check what is the same content in the slice "v"
	// calculate the binary of the value in "bit" number of bit and replace it with
	// the new columns are added above

	for i := 0; i < dfnrow; i++ {

		// Select current record
		cr := df.Subset(i)

		// Get current record values
		crv := cr.Records()

		// Create a new recored with the chnaged values
		crv[1][dfncol] = strconv.Itoa(m[df.Elem(i, colindex).String()])

		// Change the current record
		// dfheaders is the hearder of df
		dftmp := cr.Set(
			series.Ints(0),
			dataframe.LoadRecords(
				[][]string{
					dfheaders,
					crv[1],
				},
			),
		)

		cr = dftmp
		// Replacing the new record with the old record
		dftmp = df.Set(series.Ints(i), cr)

		result = dftmp
	}

	return result
}

// getBitFromNumber ....
func getBitFromNumber(idx, n int) []string {

	// GetBitFromNumber gets idx as a number that shold be exchanged to binary
	// and gets n as the number of bits that idx should be represented by

	// idx is the number should be exchanged to binary
	// n is the number of bits that idx is exchanged to

	str := "%d == %0" + strconv.Itoa(n) + "b\n"
	s := fmt.Sprintf(str, n, idx)

	// each row of the slice "slc" is the binary version of the number n
	slc := s[len(s)-n-1 : len(s)-1]

	regx := regexp.MustCompile(`"0"||"1"`)
	result := regx.Split(slc, -1)

	return result
}

// dfDistincter ...
func dfDistincter(df dataframe.DataFrame, colindex int) map[string]int {

	/*
		fil2 := fil.Filter(
			dataframe.F{
				Colname:    "D",
				Comparator: series.Eq,
				Comparando: salam,
			},
		)
	*/

	dfnrow := df.Nrow()
	result := make(map[string]int)
	var dftmp dataframe.DataFrame
	dfheaders := df.Names()

	for i := 0; i < dfnrow; i++ {
		dftmp = df.Filter(
			dataframe.F{
				Colname:    dfheaders[colindex],
				Comparator: series.Eq,
				Comparando: df.Elem(i, colindex).String(),
			},
		)
		result[df.Elem(i, colindex).String()] = dftmp.Nrow()
	}
	return result
}

// sliceIndexShow ...
func sliceIndexShow(slc []string) map[interface{}]int {
	// Be sure the input slice is already sorted and the first
	// item of the slice, is the bigest item

	result := make(map[interface{}]int)

	n := len(slc)

	for i := 0; i < n; i++ {
		result[slc[i]] = n - i
	}

	return result
}
