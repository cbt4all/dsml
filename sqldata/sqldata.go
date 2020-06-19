package sqldata

import (
	"database/sql"
	"log"
	"strconv"

	"github.com/go-gota/gota/dataframe"
)

// TopDistinctOnlySQL ...
func TopDistinctOnlySQL(col, tb string, mt int, db *sql.DB) (s []string) {

	/*
		SELECT destinationip FROM ipdatasetshrink GROUP BY destinationip HAVING
		COUNT (destinationip) > 1000 ORDER BY COUNT (destinationip) DESC
	*/

	// mt = more than
	// if mt = 0, means all records

	var result []string
	var strQuery string

	if mt < 0 {
		log.Fatal("The function needs mt >= 0")
	}

	strQuery = "SELECT " + col + " FROM " + tb
	strQuery = strQuery + " GROUP BY " + col
	strQuery = strQuery + " HAVING COUNT "
	strQuery = strQuery + "(" + col + ") > " + strconv.Itoa(mt)
	strQuery = strQuery + " ORDER BY COUNT " + "(" + col + ") DESC"

	rows, err := db.Query(strQuery)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var SrcTmp string
		if err := rows.Scan(&SrcTmp); err != nil {
			log.Fatal(err)
		}
		result = append(result, SrcTmp)
	}

	return result
}

// TopDistinctCountSQL ...
func TopDistinctCountSQL(col, tb string, mt int, db *sql.DB) (s [][]string) {
	/*
		SELECT destinationip, COUNT (destinationip) AS CountOf FROM ipdatasetshrink
		GROUP BY destinationip HAVING COUNT (destinationip) > 1000  ORDER BY CountOf
	*/

	// mt = more than
	// if mt = 0, means all records

	var result [][]string
	var strQuery string

	if mt < 0 {
		log.Fatal("The function needs mt >= 0")
	}

	strQuery = "SELECT "
	strQuery = strQuery + col
	strQuery = strQuery + ", COUNT (" + col + ") AS CountOf FROM " + tb
	strQuery = strQuery + " GROUP BY " + col
	strQuery = strQuery + " HAVING COUNT (" + col + ") > " + strconv.Itoa(mt)
	strQuery = strQuery + " ORDER BY CountOf DESC"

	rows, err := db.Query(strQuery)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {

		var f1, f2 string
		if err := rows.Scan(&f1, &f2); err != nil {
			log.Fatal(err)
		}
		SrcTmp := []string{f1, f2}
		result = append(result, SrcTmp)
	}

	return result
}

// SelectFields : Like SELECT * FROM table ->
func SelectFields(cols, tb string, db *sql.DB) (df dataframe.DataFrame) {
	// This is Version1
	// The fields must be manually insterted

	/*
	   SELECT sourceip, sourceport, destinationip, destinationport, timestamptime FROM table-name
	   f1, f2, f3, f4
	*/

	SrcTmp := [][]string{{"sourceip", "sourceport", "destinationip", "destinationport", "timestamptime"}}
	var f1, f2, f3, f4, f5 string

	strQuery := "SELECT "
	strQuery = strQuery + cols
	strQuery = strQuery + " FROM "
	strQuery = strQuery + tb

	rows, err := db.Query(strQuery)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {

		if err := rows.Scan(&f1, &f2, &f3, &f4, &f5); err != nil {
			log.Fatal(err)
		}
		slicstr := []string{f1, f2, f3, f4, f5}
		SrcTmp = append(SrcTmp, slicstr)
	}

	return dataframe.LoadRecords(SrcTmp)
}

// SelectWithQuery ...
func SelectWithQuery(sql string, db *sql.DB) (df dataframe.DataFrame) {

	/*
		Example:

		SELECT sourceip, sourceport, destinationip, destinationport, timestamptime FROM ipdatasetshrink WHERE destinationip IN
		(SELECT destinationip FROM ipdatasetshrink GROUP BY destinationip HAVING COUNT (destinationip) > 100000 ORDER BY COUNT (destinationip)) LIMIT 100
	*/
	SrcTmp := [][]string{{"sourceip", "sourceport", "destinationip", "destinationport", "timestamptime"}}
	var f1, f2, f3, f4, f5 string

	rows, err := db.Query(sql)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {

		if err := rows.Scan(&f1, &f2, &f3, &f4, &f5); err != nil {
			log.Fatal(err)
		}
		slicstr := []string{f1, f2, f3, f4, f5}
		SrcTmp = append(SrcTmp, slicstr)
	}

	return dataframe.LoadRecords(SrcTmp)

}
