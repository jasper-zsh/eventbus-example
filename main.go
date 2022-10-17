package main

import (
	"eventbus-example/migration"
	"github.com/ThreeDotsLabs/watermill"
)

var logger = watermill.NewStdLogger(false, false)

func main() {
	_, err := migration.RunMigrationRouter(logger)
	if err != nil {
		panic(err)
	}
}
