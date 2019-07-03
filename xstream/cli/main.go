package main

import (
	"bufio"
	"engine/common"
	"engine/xsql/processors"
	"fmt"
	"github.com/urfave/cli"
	"os"
	"sort"
	"strings"
)

var log = common.Log


func main() {
	app := cli.NewApp()
	app.Version = "0.1"

	//nflag := []cli.Flag { cli.StringFlag{
	//		Name: "name, n",
	//		Usage: "the name of stream",
	//	}}

	dataDir, err := common.GetDataLoc()
	if err != nil {
		log.Panic(err)
	}

	app.Commands = []cli.Command{
		{
			Name:      "stream",
			Aliases:   []string{"s"},
			Usage:     "manage streams",
			Action:    func(c *cli.Context) error {
				reader := bufio.NewReader(os.Stdin)
				var inputs []string
				for {
					fmt.Print("xstream > ")

					text, _ := reader.ReadString('\n')
					inputs = append(inputs, text)
					// convert CRLF to LF
					text = strings.Replace(text, "\n", "", -1)

					if strings.ToLower(text) == "quit" || strings.ToLower(text) == "exit" {
						break
					} else {
						content, err := processors.NewStreamProcessor(text, dataDir).Exec()
						if err != nil {
							fmt.Printf("stream command error: %s\n", err)
						}else{
							for _, c := range content{
								fmt.Println(c)
							}
						}
					}
				}
				return nil
			},
		},

		{
			Name:      "query",
			Aliases:   []string{"q"},
			Usage:     "query against stream",
			Action:    func(c *cli.Context) error {
				reader := bufio.NewReader(os.Stdin)
				var inputs []string
				for {
					fmt.Print("xstream > ")

					text, _ := reader.ReadString('\n')
					inputs = append(inputs, text)
					// convert CRLF to LF
					text = strings.Replace(text, "\n", "", -1)

					if strings.ToLower(text) == "quit" || strings.ToLower(text) == "exit" {
						break
					} else {
						fmt.Println(text)

						err = processors.NewRuleProcessor(text, dataDir).Exec()
						if err != nil {
							fmt.Printf("create topology error : %s\n", err)
						}else{
							fmt.Println("topology running")
						}
					}
				}
				return nil
			},
		},
	}


	app.Name = "xstream"
	app.Usage = "The command line tool for EMQ X stream."

	app.Action = func(c *cli.Context) error {
		cli.ShowSubcommandHelp(c)
		//cli.ShowVersion(c)

		return nil
	}

	sort.Sort(cli.FlagsByName(app.Flags))
	sort.Sort(cli.CommandsByName(app.Commands))

	err = app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}