module DBtest

go 1.20

replace JJHDB => ./JJHDB

replace stl4go => ./stl4go

require JJHDB v0.0.0-00010101000000-000000000000

require (
	stl4go v0.0.0 // indirect
)
