package main
 
import (
    "JJHDB"
	"fmt"
	// "strconv"
)
 
func main(){
	db:=JJHDB.Make()
	db.Start()
	// db.Put("target","target-val")
	// for i:=0;i< 100;i++{
	// 	x := db.Put("trash","trash-val")
	// 	fmt.Println("seq: ",x)
	// }

	ok,val:= db.Get("target",0)

	if (ok) {
		fmt.Println(val)
	}else{
		fmt.Println("None")
	}
	return
}