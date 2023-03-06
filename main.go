package main
 
import (
    "JJHDB"
	"fmt"
)
 
func main(){
	db:=JJHDB.Make()
	db.Start()
	x:=db.Put("JJH","QAQ")
	fmt.Println("seq: ",x)
	return
}