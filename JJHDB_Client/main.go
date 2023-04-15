package main

import (
	"JDBClient"
	"LogServer"
	"fmt"
	"strconv"
	"strings"

	"fyne.io/fyne"
	"fyne.io/fyne/app"
	"fyne.io/fyne/container"
	"fyne.io/fyne/layout"
	"fyne.io/fyne/widget"
)

func main() {
	// fmt.Print("Hello World")

	ch := make(chan string, 100)
	LG := LogServer.Make(ch)
	go LG.Start()

	client := JDBClient.Make()

	appClient := app.New()
	winWindow := appClient.NewWindow("Test")

	LogResults := widget.NewTextGrid()
	LogResults.ShowLineNumbers = true
	cntScrolling := container.NewScroll(LogResults)

	GetKeyEntry := widget.NewEntry()
	IndexEntry := widget.NewEntry()

	formGet := widget.NewForm(
		widget.NewFormItem("Key", GetKeyEntry),
		widget.NewFormItem("Index", IndexEntry),
	)

	GetName := widget.NewLabel("Get")
	GetShow := widget.NewTextGrid()
	GetShow.SetText("resulte will be here")

	formGet.OnSubmit = func() {
		// fmt.Println("name:", nameEntry.Text, "pass:", passEntry.Text, "login in")
		GetShow.SetText("Waiting...")
		num, err := strconv.Atoi(IndexEntry.Text)
		if err != nil {
			GetShow.SetText("index error")
			return
		}
		index := uint64(num)
		ok, val := client.Get(GetKeyEntry.Text, index)
		fmt.Println(val)
		if ok {
			GetShow.SetText("result:" + val)
		} else {
			GetShow.SetText("None")
		}
	}

	left_top := fyne.NewContainerWithLayout(
		layout.NewGridWrapLayout(fyne.NewSize(400, 75)),
		GetName, GetShow)

	leftcont := fyne.NewContainerWithLayout(
		layout.NewGridWrapLayout(fyne.NewSize(400, 150)),
		left_top, formGet)

	PutKeyEntry := widget.NewEntry()
	ValueEntry := widget.NewEntry()
	formPut := widget.NewForm(
		widget.NewFormItem("Key", PutKeyEntry),
		widget.NewFormItem("Value", ValueEntry),
	)

	PutName := widget.NewLabel("Put")
	PutShow := widget.NewTextGrid()
	PutShow.SetText("resulte will be here")

	formPut.OnSubmit = func() {
		// fmt.Println("name:", nameEntry.Text, "pass:", passEntry.Text, "login in")
		// PutShow.SetText("show")
		PutShow.SetText("Waiting...")
		seq := client.Put(PutKeyEntry.Text, ValueEntry.Text)
		if seq != 0 {
			PutShow.SetText("Success")
		} else {
			GetShow.SetText("Fail")
		}
	}
	right_top := fyne.NewContainerWithLayout(
		layout.NewGridWrapLayout(fyne.NewSize(400, 75)),
		PutName, PutShow)
	rightcont := fyne.NewContainerWithLayout(
		layout.NewGridWrapLayout(fyne.NewSize(400, 150)),
		right_top, formPut)
	top := container.NewGridWithColumns(2, leftcont, rightcont)

	content := fyne.NewContainerWithLayout(
		layout.NewGridWrapLayout(fyne.NewSize(800, 300)),
		top, cntScrolling)

	winWindow.Resize(fyne.NewSize(600, 600))
	winWindow.SetContent(content)

	go func() {
		for {
			select {
			case S := <-ch:
				LogResults.SetText(strings.TrimPrefix(LogResults.Text()+"\n"+S, "\n"))
				cntScrolling.Refresh()
				cntScrolling.ScrollToBottom()
			}
		}
	}()

	winWindow.ShowAndRun()

}
