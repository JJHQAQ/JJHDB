package main
 
import (
    "JJHDB"
	"fmt"
	// "strconv"
)
 
func main(){
	db:=JJHDB.Make()
	db.Start()
	// db.Put("JJH","QAQ")
	// for i:=0;i< 100;i++{
	// 	x := db.Put("trash","trash-val")
	// 	fmt.Println("seq: ",x)
	// }

	ok,val:= db.Get("JJH",0)

	if (ok) {
		fmt.Println("result:",val)
	}else{
		fmt.Println("None")
	}
	for {
		var key,val string
		fmt.Println("input KV:")
		fmt.Scan(&key,&val) 
		db.Put(key,val)

		// var key string
		// fmt.Println("input K:")
		// fmt.Scan(&key) 
		// ok,val:=db.Get(key,0)
		// if (ok) {
		// 	fmt.Println("result:",val)
		// }else{
		// 	fmt.Println("None")
		// }
	}

}