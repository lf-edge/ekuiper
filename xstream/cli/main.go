package main

import (
	"bufio"
	"engine/common"
	"fmt"
	"github.com/go-yaml/yaml"
	"github.com/urfave/cli"
	"io/ioutil"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

type clientConf struct {
	Host string `yaml:"host"`
	Port int `yaml:"port"`
}

var clientYaml = "client.yaml"

func streamProcess(client *rpc.Client, args string) error {
	var reply string
	if args == ""{
		args = strings.Join(os.Args[1:], " ")
	}
	err := client.Call("Server.Stream", args, &reply)
	if err != nil{
		fmt.Println(err)
		return err
	}else{
		fmt.Println(reply)
	}
	return nil
}

func main() {
	app := cli.NewApp()
	app.Version = "0.0.3"

	//nflag := []cli.Flag { cli.StringFlag{
	//		Name: "name, n",
	//		Usage: "the name of stream",
	//	}}

	b := common.LoadConf(clientYaml)
	var cfg map[string]clientConf
	var config *clientConf
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		fmt.Printf("Failed to load config file with error %s.\n", err)
	}else{
		c, ok := cfg["basic"]
		if !ok{
			fmt.Printf("No basic config in client.yaml, will use the default configuration.\n")
		}else{
			config = &c
		}
	}
	if config == nil {
		config = &clientConf{
			Host: "127.0.0.1",
			Port: 20498,
		}
	}

	fmt.Printf("Connecting to %s:%d \n", config.Host, config.Port)
	// Create a TCP connection to localhost on port 1234
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", config.Host, config.Port))
	if err != nil {
		fmt.Printf("Failed to connect the server, please start the server.\n")
		return
	}

	app.Commands = []cli.Command{
		{
			Name:      "query",
			Aliases:   []string{"query"},
			Usage:     "query command line",
			Action:    func(c *cli.Context) error {
				reader := bufio.NewReader(os.Stdin)
				var inputs []string
				ticker := time.NewTicker(time.Millisecond * 300)
				defer ticker.Stop()
				for {
					fmt.Print("kuiper > ")

					text, _ := reader.ReadString('\n')
					inputs = append(inputs, text)
					// convert CRLF to LF
					text = strings.Replace(text, "\n", "", -1)

					if strings.ToLower(text) == "quit" || strings.ToLower(text) == "exit" {
						break
					} else if strings.Trim(text, " ") == "" {
						continue
					} else {
						var reply string
						err := client.Call("Server.CreateQuery", text, &reply)
						if err != nil{
							fmt.Println(err)
							return err
						} else {
							fmt.Println(reply)
							go func() {
								for {
									<-ticker.C
									var result string
									_ = client.Call("Server.GetQueryResult", "", &result)
									if result != "" {
										fmt.Println(result)
									}
								}
							}()
						}
					}
				}
				return nil
			},
		},
		{
			Name:      "create",
			Aliases:   []string{"create"},
			Usage:     "create stream $stream_name | create stream $stream_name -f $stream_def_file | create rule $rule_name $rule_json | create rule $rule_name -f $rule_def_file",

			Subcommands: []cli.Command {
				{
					Name:  "stream",
					Usage: "create stream $stream_name [-f stream_def_file]",
					Flags: []cli.Flag {
						cli.StringFlag{
							Name: "file, f",
							Usage: "the location of stream definition file",
							FilePath: "/home/mystream.txt",
						},
					},
					Action: func(c *cli.Context) error {
						sfile := c.String("file")
						if sfile != "" {
							if _, err := os.Stat(c.String("file")); os.IsNotExist(err) {
								fmt.Printf("The specified stream defintion file %s does not existed.", sfile)
								return nil
							}
							fmt.Printf("Creating a new stream from file %s", sfile)
							if stream, err := ioutil.ReadFile(sfile); err != nil {
								fmt.Printf("Failed to read from stream definition file %s", sfile)
								return nil
							} else {
								args := strings.Join([]string{"CREATE STREAM ", string(stream)}, " ")
								return streamProcess(client, args)
							}
							return nil
						} else {
							return streamProcess(client, "")
						}
					},
				},
				{
					Name:  "rule",
					Usage: "create rule $rule_name [$rule_json | -f rule_def_file]",
					Flags: []cli.Flag {
						cli.StringFlag{
							Name: "file, f",
							Usage: "the location of rule definition file",
							FilePath: "/home/myrule.txt",
						},
					},
					Action: func(c *cli.Context) error {
						sfile := c.String("file")
						if sfile != "" {
							if _, err := os.Stat(c.String("file")); os.IsNotExist(err) {
								fmt.Printf("The specified rule defenition file %s does not existed.", sfile)
								return nil
							}
							fmt.Printf("Creating a new rule from file %s", sfile)
							if rule, err := ioutil.ReadFile(sfile); err != nil {
								fmt.Printf("Failed to read from rule definition file %s", sfile)
								return nil
							} else {
								if len(c.Args()) != 1 {
									fmt.Printf("Expect rule name.\n")
									return nil
								}
								rname := c.Args()[0]
								var reply string
								args := &common.Rule{rname, string(rule)}
								err = client.Call("Server.CreateRule", args, &reply)
								if err != nil {
									fmt.Println(err)
								} else {
									fmt.Println(reply)
								}
							}
							return nil
						} else {
							if len(c.Args()) != 2 {
								fmt.Printf("Expect rule name and json.\nBut found %d args:%s.\n", len(c.Args()), c.Args())
								return nil
							}
							rname := c.Args()[0]
							rjson := c.Args()[1]
							var reply string
							args := &common.Rule{rname, rjson}
							err = client.Call("Server.CreateRule", args, &reply)
							if err != nil {
								fmt.Println(err)
							} else {
								fmt.Println(reply)
							}
							return nil
						}
					},
				},
			},

		},
		{
			Name:      "describe",
			Aliases:   []string{"describe"},
			Usage:     "describe stream $stream_name | describe rule $rule_name",
			Subcommands: []cli.Command{
				{
					Name:  "stream",
					Usage: "describe stream $stream_name",
					//Flags: nflag,
					Action: func(c *cli.Context) error {
						return streamProcess(client, "")
					},
				},
				{
					Name:  "rule",
					Usage: "describe rule $rule_name",
					Action:    func(c *cli.Context) error {
						if len(c.Args()) != 1 {
							fmt.Printf("Expect rule name.\n")
							return nil
						}
						rname := c.Args()[0]
						var reply string
						err = client.Call("Server.DescRule", rname, &reply)
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println(reply)
						}
						return nil
					},
				},
			},
		},

		{
			Name:        "drop",
			Aliases:     []string{"drop"},
			Usage:       "drop stream $stream_name | drop rule $rule_name",
			Subcommands: []cli.Command{
				{
					Name:  "stream",
					Usage: "drop stream $stream_name",
					//Flags: nflag,
					Action: func(c *cli.Context) error {
						return streamProcess(client, "")
					},
				},
				{
					Name:  "rule",
					Usage: "drop rule $rule_name",
					//Flags: nflag,
					Action: func(c *cli.Context) error {
						if len(c.Args()) != 1 {
							fmt.Printf("Expect rule name.\n")
							return nil
						}
						rname := c.Args()[0]
						var reply string
						err = client.Call("Server.DropRule", rname, &reply)
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println(reply)
						}
						return nil
					},
				},
			},
		},

		{
			Name:      "show",
			Aliases:   []string{"show"},
			Usage:     "show streams | show rules",

			Subcommands: []cli.Command{
				{
					Name:  "streams",
					Usage: "show streams",
					Action: func(c *cli.Context) error {
						return streamProcess(client, "")
					},
				},
				{
					Name:  "rules",
					Usage: "show rules",
					Action:    func(c *cli.Context) error {
						var reply string
						err = client.Call("Server.ShowRules", 0, &reply)
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println(reply)
						}
						return nil
					},
				},
			},
		},

		{
			Name:        "getstatus",
			Aliases:     []string{"getstatus"},
			Usage:       "getstatus rule $rule_name",
			Subcommands: []cli.Command{
				{
					Name:  "rule",
					Usage: "getstatus rule $rule_name",
					//Flags: nflag,
					Action: func(c *cli.Context) error {
						if len(c.Args()) != 1 {
							fmt.Printf("Expect rule name.\n")
							return nil
						}
						rname := c.Args()[0]
						var reply string
						err = client.Call("Server.GetStatusRule", rname, &reply)
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println(reply)
						}
						return nil
					},
				},
			},
		},
		{
			Name:        "start",
			Aliases:     []string{"start"},
			Usage:       "start rule $rule_name",
			Subcommands: []cli.Command{
				{
					Name:  "rule",
					Usage: "start rule $rule_name",
					//Flags: nflag,
					Action: func(c *cli.Context) error {
						if len(c.Args()) != 1 {
							fmt.Printf("Expect rule name.\n")
							return nil
						}
						rname := c.Args()[0]
						var reply string
						err = client.Call("Server.StartRule", rname, &reply)
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println(reply)
						}
						return nil
					},
				},
			},
		},
		{
			Name:        "stop",
			Aliases:     []string{"stop"},
			Usage:       "stop rule $rule_name",
			Subcommands: []cli.Command{
				{
					Name:  "rule",
					Usage: "stop rule $rule_name",
					//Flags: nflag,
					Action: func(c *cli.Context) error {
						if len(c.Args()) != 1 {
							fmt.Printf("Expect rule name.\n")
							return nil
						}
						rname := c.Args()[0]
						var reply string
						err = client.Call("Server.StopRule", rname, &reply)
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println(reply)
						}
						return nil
					},
				},
			},
		},
		{
			Name:        "restart",
			Aliases:     []string{"restart"},
			Usage:       "restart rule $rule_name",
			Subcommands: []cli.Command{
				{
					Name:  "rule",
					Usage: "restart rule $rule_name",
					//Flags: nflag,
					Action: func(c *cli.Context) error {
						if len(c.Args()) != 1 {
							fmt.Printf("Expect rule name.\n")
							return nil
						}
						rname := c.Args()[0]
						var reply string
						err = client.Call("Server.RestartRule", rname, &reply)
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println(reply)
						}
						return nil
					},
				},
			},
		},
	}


	app.Name = "Kuiper"
	app.Usage = "The command line tool for EMQ X Kuiper."

	app.Action = func(c *cli.Context) error {
		cli.ShowSubcommandHelp(c)
		//cli.ShowVersion(c)

		return nil
	}

	sort.Sort(cli.FlagsByName(app.Flags))
	sort.Sort(cli.CommandsByName(app.Commands))

	err = app.Run(os.Args)
	if err != nil {
		fmt.Errorf("%s", err)
	}
}