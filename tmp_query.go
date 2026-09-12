package main

import (
	"database/sql"
	"fmt"
	"os"
	_ "github.com/lib/pq"
)

func main() {
	db, err := sql.Open("postgres", os.Getenv("WQ_DATABASE_URL"))
	if err != nil { panic(err) }
	defer db.Close()
	for _, q := range os.Args[1:] {
		rows, err := db.Query(q)
		if err != nil { fmt.Printf("ERR %v\n", err); continue }
		cols, _ := rows.Columns()
		fmt.Println(cols)
		for rows.Next() {
			vals := make([]any, len(cols)); ptrs := make([]any, len(cols))
			for i := range vals { ptrs[i] = &vals[i] }
			if err := rows.Scan(ptrs...); err != nil { fmt.Printf("SCANERR %v\n", err); break }
			for i, v := range vals {
				if b, ok := v.([]byte); ok { vals[i] = string(b) }
			}
			fmt.Println(vals)
		}
		rows.Close()
	}
}
